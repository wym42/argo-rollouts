package inplace

import (
	"time"

	log "github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/apis/apps"

	"github.com/argoproj/argo-rollouts/controller/metrics"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	controllerutil "github.com/argoproj/argo-rollouts/utils/controller"
	logutil "github.com/argoproj/argo-rollouts/utils/log"
)

var rsKind = apps.SchemeGroupVersion.WithKind("ReplicaSet")

// Controller is the controller implementation for Analysis resources
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface

	replicaSetLister   appslisters.ReplicaSetLister
	replicaSetInformer appsinformers.ReplicaSetInformer
	replicaSetSynced   cache.InformerSynced

	podLister            corelisters.PodLister
	podInformer          coreinformers.PodInformer
	podInformerRunSynced cache.InformerSynced

	metricsServer *metrics.MetricsServer

	// used for unit testing
	enqueuePod      func(obj interface{})
	enqueuePodAfter func(obj interface{}, duration time.Duration)

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	podWorkQueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder     record.EventRecorder
	resyncPeriod time.Duration
}

// ControllerConfig describes the data required to instantiate a new analysis controller
type ControllerConfig struct {
	KubeClientSet kubernetes.Interface

	ReplicaSetInformer appsinformers.ReplicaSetInformer
	PodInformer        coreinformers.PodInformer

	ResyncPeriod  time.Duration
	PodWorkQueue  workqueue.RateLimitingInterface
	MetricsServer *metrics.MetricsServer
	Recorder      record.EventRecorder
}

// NewController returns a new analysis controller
func NewController(cfg ControllerConfig) *Controller {
	controller := &Controller{
		kubeclientset: cfg.KubeClientSet,

		replicaSetLister:   cfg.ReplicaSetInformer.Lister(),
		replicaSetInformer: cfg.ReplicaSetInformer,
		replicaSetSynced:   cfg.ReplicaSetInformer.Informer().HasSynced,

		podLister:            cfg.PodInformer.Lister(),
		podInformer:          cfg.PodInformer,
		podInformerRunSynced: cfg.PodInformer.Informer().HasSynced,
		metricsServer:        cfg.MetricsServer,
		podWorkQueue:         cfg.PodWorkQueue,

		recorder:     cfg.Recorder,
		resyncPeriod: cfg.ResyncPeriod,
	}

	controller.enqueuePod = func(obj interface{}) {
		controllerutil.Enqueue(obj, cfg.PodWorkQueue)
	}
	controller.enqueuePodAfter = func(obj interface{}, duration time.Duration) {
		controllerutil.EnqueueAfter(obj, duration, cfg.PodWorkQueue)
	}

	cfg.PodInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPod, ok := newObj.(*v1.Pod)
			if !ok {
				return
			}
			oldPod, ok := oldObj.(*v1.Pod)
			if !ok {
				return
			}
			// 判断试试需要处理下readiness gates
			cgate := false
			for _, gate := range newPod.Spec.ReadinessGates {
				if gate.ConditionType == v1alpha1.RolloutInplaceReadinessGates {
					cgate = true
					break
				}
			}
			if !cgate {
				return
			}
			for _, condition := range oldPod.Status.Conditions {
				if condition.Type == v1alpha1.RolloutInplaceReadinessGates {
					return
				}
			}

			newReady := false
			for i := len(newPod.Status.Conditions) - 1; i >= 0; i-- {
				if newPod.Status.Conditions[i].Type == v1.ContainersReady && newPod.Status.Conditions[i].Status == v1.ConditionTrue {
					newReady = true
					break
				}
			}
			if !newReady {
				return
			}
			controller.enqueuePod(newObj)
		},
	})

	log.Info("Setting up inplace event handlers")
	return controller
}

func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	log.Info("Starting inplace workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(func() {
			controllerutil.RunWorker(c.podWorkQueue, logutil.PodInplace, c.syncHandler, c.metricsServer)
		}, time.Second, stopCh)
	}
	log.Infof("Started %d inplace workers", threadiness)
	<-stopCh
	log.Info("Shutting down inplace workers")

	return nil
}

func (c *Controller) syncHandler(key string) error {
	startTime := time.Now()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	log.WithField(logutil.PodInplace, name).WithField(logutil.NamespaceKey, namespace).Infof("Started syncing pod-inplace at (%v)", startTime)
	pod, err := c.podLister.Pods(namespace).Get(name)
	if k8serrors.IsNotFound(err) {
		log.WithField(logutil.PodInplace, name).WithField(logutil.NamespaceKey, namespace).Info("pod has been deleted")
		return nil
	}
	if err != nil {
		return err
	}

	if pod.DeletionTimestamp != nil {
		log.WithField(logutil.PodInplace, name).Info("No reconciliation as inplace pod marked for deletion")
		return nil
	}
	cpod := pod.DeepCopy()
	// 如果没有  v1alpha1.RolloutInplaceReadinessGates 的 ReadinessGates ，直接忽略
	conditions := cpod.Status.Conditions
	has := false
	for _, gate := range cpod.Spec.ReadinessGates {
		if gate.ConditionType == v1alpha1.RolloutInplaceReadinessGates {
			ready := false
			for _, condition := range conditions {
				if condition.Type == v1alpha1.RolloutInplaceReadinessGates {
					has = true
					continue
				}
				if condition.Type == v1.ContainersReady && condition.Status == v1.ConditionTrue {
					ready = true
				}
				cpod.Status.Conditions = append(cpod.Status.Conditions, condition)
			}
			if ready && !has {
				cpod.Status.Conditions = append(cpod.Status.Conditions, v1.PodCondition{
					Type:               v1alpha1.RolloutInplaceReadinessGates,
					Status:             v1.ConditionTrue,
					Message:            "ContainersReady add RolloutInplaceReadinessGates",
					LastTransitionTime: metav1.Time{Time: time.Now()},
				})
				_, err := c.kubeclientset.CoreV1().Pods(cpod.Namespace).UpdateStatus(cpod)
				return err
			}
			break
		}
	}
	return nil
}

func (c *Controller) resolveControllerRef(namespace string, controllerRef *metav1.OwnerReference) *appsv1.ReplicaSet {
	if controllerRef.Kind != rsKind.Kind {
		return nil
	}
	rs, err := c.replicaSetLister.ReplicaSets(namespace).Get(controllerRef.Name)
	if err != nil {
		return nil
	}
	if rs.UID != controllerRef.UID {
		return nil
	}
	//TODO 可能出现资源泄漏，在转移的时候将condition弄丢，或者认为删除rs，
	if k8serrors.IsNotFound(err) {
		log.WithField(logutil.PodInplace, controllerRef.Name).WithField(logutil.NamespaceKey, namespace).Infof("rs %s has been deleted", controllerRef.Name)
		return nil
	}
	if rs.DeletionTimestamp != nil {
		log.WithField(logutil.PodInplace, controllerRef.Name).Infof("No reconciliation rs %s marked for deletion", controllerRef.Name)
		return nil
	}
	return rs
}
