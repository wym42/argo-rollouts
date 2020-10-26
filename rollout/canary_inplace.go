package rollout

import (
	"fmt"
	"math"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	replicasetutil "github.com/argoproj/argo-rollouts/utils/replicaset"
)

var inplaceRemoveCondition = map[v1.PodConditionType]bool{
	v1.PodReady:                           true,
	v1.ContainersReady:                    true,
	v1alpha1.RolloutInplaceReadinessGates: true,
	v1alpha1.RolloutInplaceOriginRSHash:   true,
	v1alpha1.RolloutInplaceNewRSHash:      true,
	v1alpha1.RolloutInplaceWaitSeconds:    true,
}

func (c *Controller) reconcileInplaceReplicaSets(roCtx *canaryContext) (bool, error) {
	logCtx := roCtx.Log()

	logCtx.Info("Finding podMap")
	podMap, err := c.findPodMapForRollout(roCtx.Rollout(), roCtx.allRSs)
	if err != nil {
		logCtx.Errorf("get pod map for rollout %s failed. %v", roCtx.Rollout().Name, err)
		return false, err
	}

	logCtx.Info("Transfer pods")
	transfer, err := c.transferUnReadyPodsToNewRS(podMap, roCtx)
	if err != nil {
		return false, err
	}
	if transfer {
		logCtx.Infof("finished transfer unready pods to newRS")
		return true, nil
	}

	logCtx.Info("Reconciling StableRS")
	scaledStableRS, err := c.reconcileStableRSForInplace(roCtx, podMap)
	if err != nil {
		return false, err
	}
	if scaledStableRS {
		logCtx.Infof("Not finished reconciling stableRS")
		return true, nil
	}

	newRS := roCtx.NewRS()
	olderRSs := roCtx.OlderRSs()
	allRSs := roCtx.AllRSs()

	logCtx.Info("Reconciling old replica sets")
	scaled, err := c.reconcileOldReplicaSetsInplace(allRSs, controller.FilterActiveReplicaSets(olderRSs), roCtx, podMap)
	if err != nil {
		return false, err
	}
	if scaled {
		logCtx.Info("Not finished reconciling old replica sets")
		return true, nil
	}

	scaledNewRS, err := c.reconcileNewReplicaSetInplace(roCtx)
	if err != nil {
		return false, err
	}
	if scaledNewRS {
		logCtx.Infof("Not finished reconciling new ReplicaSet '%s'", newRS.Name)
		return true, nil
	}
	return false, nil
}

func (c *Controller) reconcileNewReplicaSetInplace(roCtx *canaryContext) (bool, error) {
	rollout := roCtx.Rollout()
	newRS := roCtx.NewRS()
	if newRS == nil {
		return false, nil
	}

	roCtx.Log().Infof("Reconciling new ReplicaSet '%s'", newRS.Name)
	newReplicasCount, _ := calculateReplicaCountsForInplace(rollout, newRS, roCtx.StableRS(), roCtx.OlderRSs())
	scaled, _, err := c.scaleReplicaSetAndRecordEvent(newRS, newReplicasCount, rollout)
	return scaled, err
}

func (c *Controller) reconcileStableRSForInplace(roCtx *canaryContext, podMap map[types.UID]*v1.PodList) (bool, error) {
	logCtx := roCtx.Log()
	rollout := roCtx.Rollout()
	newRS := roCtx.NewRS()
	stableRS := roCtx.StableRS()
	olderRSs := roCtx.OlderRSs()
	if !replicasetutil.CheckStableRSExists(newRS, stableRS) {
		logCtx.Info("No StableRS exists to reconcile or matches newRS")
		return false, nil
	}

	newRSReplicaCount, stableRSReplicaCount := calculateReplicaCountsForInplace(rollout, newRS, stableRS, olderRSs)

	if *(rollout.Spec.Replicas) <= *(newRS.Spec.Replicas) {
		scaled, _, err := c.scaleReplicaSetAndRecordEvent(stableRS, stableRSReplicaCount, rollout)
		return scaled, err
	}

	tmp := math.Min(float64(*(stableRS.Spec.Replicas)-stableRSReplicaCount), float64(newRSReplicaCount-*(newRS.Spec.Replicas)))
	inplaceCount := int32(math.Min(float64(*(rollout.Spec.Replicas)-*(newRS.Spec.Replicas)), tmp))
	if inplaceCount > 0 {
		count, err := c.transferPodsUnReady(stableRS, newRS, podMap, inplaceCount, roCtx)
		return count > 0, err
	}
	return false, nil
}

func (c *Controller) reconcileOldReplicaSetsInplace(allRSs []*appsv1.ReplicaSet, oldRSs []*appsv1.ReplicaSet,
	roCtx *canaryContext, podMap map[types.UID]*v1.PodList) (bool, error) {
	newRS := roCtx.NewRS()
	if newRS == nil {
		return false, nil
	}
	rollout := roCtx.Rollout()
	if *(rollout.Spec.Replicas) <= *(newRS.Spec.Replicas) {
		return c.reconcileOldReplicaSetsCanary(allRSs, oldRSs, roCtx)
	}
	logCtx := roCtx.Log()
	oldPodsCount := replicasetutil.GetReplicaCountForReplicaSets(oldRSs)
	if oldPodsCount == 0 {
		// Can't scale down further
		return false, nil
	}

	// Clean up unhealthy replicas first, otherwise unhealthy replicas will block rollout
	// and cause timeout. See https://github.com/kubernetes/kubernetes/issues/16737
	oldRSs, cleanedUpRSs, err := c.cleanupUnhealthyReplicas(oldRSs, roCtx)
	if err != nil {
		return false, nil
	}

	// 当 newRS 达到标准后，直接 scala down oldRSs
	if *(newRS.Spec.Replicas) >= *(rollout.Spec.Replicas) {
		// Scale down old replica sets, need check replicasToKeep to ensure we can scale down
		scaledDownCount, err := c.scaleDownOldReplicaSetsForCanary(allRSs, oldRSs, rollout)
		logCtx.Infof("Scaled down old RSes by %d", scaledDownCount)
		if err != nil {
			return cleanedUpRSs || scaledDownCount > 0, nil
		}
	}
	//
	inplaceCount, err := c.selectOldRSPodsUnReadyForInplace(newRS, allRSs, oldRSs, roCtx, podMap)
	if err != nil {
		return false, nil
	}
	return cleanedUpRSs || inplaceCount > 0, nil
}

// 将所有unready的pods转换为新RS
func (c *Controller) transferUnReadyPodsToNewRS(podMap map[types.UID]*v1.PodList, roCtx *canaryContext) (bool, error) {
	allRSs := roCtx.AllRSs()
	newRS := roCtx.NewRS()
	rollout := roCtx.Rollout()
	totalCount := int32(0)
	podsM := c.getTransferPods(podMap, newRS)
	if len(podsM) == 0 {
		return false, nil
	}

	var errlist []error
	tmp, err := c.coolRSForUpdateInplace(true, newRS, true)
	if err != nil {
		return false, err
	}
	if tmp != nil {
		newRS = tmp
	}

	for _, oldRS := range allRSs {
		if oldRS.UID == newRS.UID {
			continue
		}
		count := int32(0)

		if pods, ok := podsM[oldRS.UID]; ok && len(pods) > 0 {
			// cool
			tmp, err := c.coolRSForUpdateInplace(true, oldRS, true)
			if err != nil {
				errlist = append(errlist, err)
			}
			if tmp != nil {
				oldRS = tmp
			}
			for _, pod := range pods {
				flag, err := c.transferPodToNewRS(&pod, newRS)
				if err != nil {
					errlist = append(errlist, err)
					continue
				}
				if flag {
					count++
				}
			}

			if count > 0 {
				_, _, err := c.scaleReplicaSetAndRecordEventTry(oldRS, *(oldRS.Spec.Replicas)-count, rollout)
				if err != nil {
					errlist = append(errlist, err)
				}
			}

			// un cool
			tmp, err = c.coolRSForUpdateInplace(true, oldRS, false)
			if err != nil {
				errlist = append(errlist, err)
			}
		}

		totalCount += count
	}

	if totalCount > 0 {
		_, result, err := c.scaleReplicaSetAndRecordEventTry(newRS, *(newRS.Spec.Replicas)+totalCount, rollout)
		if err != nil {
			errlist = append(errlist, err)
		}
		if result != nil {
			newRS = result
		}
	}

	if len(podMap) > 0 {
		c.coolRSForUpdateInplace(true, newRS, false)
	}
	return totalCount > 0, errors.NewAggregate(errlist)
}

func (c *Controller) getTransferPods(podMap map[types.UID]*v1.PodList, newRS *appsv1.ReplicaSet) map[types.UID][]v1.Pod {
	podsMap := map[types.UID][]v1.Pod{}
	for uid, podList := range podMap {
		if newRS.UID == uid {
			continue
		}
		var pods []v1.Pod
		for _, pod := range podList.Items {
			originHash := ""
			for _, condition := range pod.Status.Conditions {
				if condition.Type == v1alpha1.RolloutInplaceOriginRSHash {
					originHash = string(condition.Status)
				}
			}
			oldPodTemplateSpecHash, _ := pod.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
			if oldPodTemplateSpecHash == originHash {
				pods = append(pods, pod)
			}
		}
		if len(pods) > 0 {
			podsMap[uid] = pods
		}
	}

	return podsMap
}

func (c *Controller) coolRSForUpdateInplace(try bool, rs *appsv1.ReplicaSet, cool bool) (*appsv1.ReplicaSet, error) {
	crs := rs.DeepCopy()
	crs.Status.Conditions = nil
	if cool {
		crs.Status.Conditions = append(crs.Status.Conditions, appsv1.ReplicaSetCondition{
			Type:               v1alpha1.UpdateInplaceCoolTime,
			Status:             v1.ConditionTrue,
			Reason:             "3",
			LastTransitionTime: metav1.Now(),
		})
	}
	has := false
	for _, cond := range rs.Status.Conditions {
		if cond.Type == v1alpha1.UpdateInplaceCoolTime {
			has = true
			continue
		}
		crs.Status.Conditions = append(crs.Status.Conditions, cond)
	}
	// 原来就没有，且为uncool，则不需要发送
	if !has && !cool {
		return rs, nil
	}
	result, err := c.kubeclientset.AppsV1().ReplicaSets(crs.Namespace).UpdateStatus(crs)
	if err == nil {
		return result, err
	}
	if k8serr.IsConflict(err) && try {
		newRS, iErr := c.kubeclientset.AppsV1().ReplicaSets(rs.Namespace).Get(rs.Name, metav1.GetOptions{})
		if iErr != nil {
			return rs, err
		}
		return c.coolRSForUpdateInplace(false, newRS, cool)
	}
	return result, err
}

// 将老的rs原地重启
func (c *Controller) selectOldRSPodsUnReadyForInplace(newRS *appsv1.ReplicaSet, allRSs []*appsv1.ReplicaSet,
	oldRSs []*appsv1.ReplicaSet, roCtx *canaryContext, podMap map[types.UID]*v1.PodList) (int32, error) {
	rollout := roCtx.Rollout()
	logCtx := roCtx.Log()
	availablePodCount := replicasetutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	minAvailable := defaults.GetReplicasOrDefault(rollout.Spec.Replicas) - replicasetutil.MaxUnavailable(rollout)
	maxInplace := availablePodCount - minAvailable

	count := defaults.GetReplicasOrDefault(rollout.Spec.Replicas) - defaults.GetReplicasOrDefault(newRS.Spec.Replicas)
	if count > 0 && count < maxInplace {
		maxInplace = 0
	}
	if maxInplace <= 0 {
		return 0, nil
	}
	logCtx.Infof("Found %d available pods, inplace old RSes, maxInplace:%d.", availablePodCount, maxInplace)

	sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))

	totalInplaceCount := int32(0)

	//inplaceCount := 0
	for _, targetRS := range oldRSs {
		if maxInplace <= 0 {
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			continue
		}

		inplaceCount := int32(math.Min(float64(*(targetRS.Spec.Replicas)), float64(maxInplace)))
		count, err := c.transferPodsUnReady(targetRS, newRS, podMap, inplaceCount, roCtx)
		maxInplace -= count
		totalInplaceCount += count
		if err != nil {
			return totalInplaceCount, err
		}

	}
	return totalInplaceCount, nil
}

//将pod切换为unready，方便 change label
func (c *Controller) transferPodsUnReady(oldRS, newRS *appsv1.ReplicaSet, podMap map[types.UID]*v1.PodList,
	inplaceCount int32, roCtx *canaryContext) (int32, error) {
	totalInplaceCount := int32(0)
	if podItems, ok := podMap[oldRS.UID]; ok {
		leftNewRsCount := *(roCtx.Rollout().Spec.Replicas) - *(newRS.Spec.Replicas)
		inplaceCount = int32(math.Min(float64(inplaceCount), float64(leftNewRsCount)))

		// 不需要变更
		if inplaceCount <= 0 {
			return 0, nil
		}
		for _, item := range podItems.Items {
			if totalInplaceCount >= inplaceCount {
				break
			}
			_, err := c.transferPodUnReadyForInplace(&item, newRS)
			if err != nil {
				break
			}
			totalInplaceCount += 1
		}
	}
	return totalInplaceCount, nil
}

// 将pod转移到 unready status 状态
func (c *Controller) transferPodUnReadyForInplace(originPod *v1.Pod, newRS *appsv1.ReplicaSet) (*v1.Pod, error) {
	pod := originPod.DeepCopy()
	originPodTemplateSpecHash, _ := pod.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	newPodTemplateSpecHash, _ := newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	conditions := pod.Status.Conditions
	pod.Status.Conditions = nil
	for _, condition := range conditions {
		if _, ok := inplaceRemoveCondition[condition.Type]; ok {
			continue
		}
		pod.Status.Conditions = append(pod.Status.Conditions, condition)
	}
	pod.Status.Conditions = append(pod.Status.Conditions, v1.PodCondition{
		Type:               v1alpha1.RolloutInplaceOriginRSHash,
		Status:             v1.ConditionStatus(originPodTemplateSpecHash),
		Message:            fmt.Sprintf("update inplace to %s", newPodTemplateSpecHash),
		LastTransitionTime: metav1.Now(),
	})
	pod.Status.Conditions = append(pod.Status.Conditions, v1.PodCondition{
		Type:               v1alpha1.RolloutInplaceNewRSHash,
		Status:             v1.ConditionStatus(newPodTemplateSpecHash),
		Message:            fmt.Sprintf("update inplace to %s", newPodTemplateSpecHash),
		LastTransitionTime: metav1.Now(),
	})
	// 假设等待多长时间设置 container restart, 先借用 newRS.Spec.MinReadySeconds，
	pod.Status.Conditions = append(pod.Status.Conditions, v1.PodCondition{
		Type:               v1alpha1.RolloutInplaceWaitSeconds,
		Status:             v1.ConditionStatus(fmt.Sprintf("%d", newRS.Spec.MinReadySeconds)),
		Message:            fmt.Sprintf("update inplace to %s", newPodTemplateSpecHash),
		LastTransitionTime: metav1.Now(),
	})
	cpod, err := c.kubeclientset.CoreV1().Pods(pod.Namespace).UpdateStatus(pod)
	if err != nil {
		klog.Errorf("update inplace pod %s %s status failed, %v", pod.Namespace, pod.Name, err)
	}
	return cpod, err
}

// 切换 pod 的 rs label
func (c *Controller) transferPodToNewRS(pod *v1.Pod, newRS *appsv1.ReplicaSet) (bool, error) {
	originHash := ""
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1alpha1.RolloutInplaceOriginRSHash {
			originHash = string(condition.Status)
		}
	}

	oldPodTemplateSpecHash, _ := pod.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	if oldPodTemplateSpecHash != originHash {
		return false, nil
	}
	podTemplateSpecHash, _ := newRS.Labels[v1alpha1.DefaultRolloutUniqueLabelKey]
	cpod := pod.DeepCopy()
	// label 切换
	cpod.Labels[v1alpha1.DefaultRolloutUniqueLabelKey] = podTemplateSpecHash
	// 注解 添加下 rs的变迁历史
	cpod.Annotations = pod.Annotations
	if cpod.Annotations == nil {
		cpod.Annotations = map[string]string{}
	}
	if val, ok := cpod.Annotations[v1alpha1.RolloutInplaceRSHistoryAnnotationKey]; ok {
		arrs := strings.Split(val, ",")
		if len(arrs) > 10 {
			arrs = arrs[len(arrs)-10:]
		}
		arrs = append(arrs, oldPodTemplateSpecHash)
		cpod.Annotations[v1alpha1.RolloutInplaceRSHistoryAnnotationKey] = strings.Join(arrs, ",")
	} else {
		cpod.Annotations[v1alpha1.RolloutInplaceRSHistoryAnnotationKey] = oldPodTemplateSpecHash
	}
	// 清理下owner
	owners := cpod.OwnerReferences
	cpod.OwnerReferences = nil
	if len(owners) > 0 {
		for _, owner := range owners {
			if owner.UID == newRS.UID {
				continue
			}
			cpod.OwnerReferences = append(cpod.OwnerReferences, owner)
		}
	}

	for index, _ := range cpod.Spec.Containers {
		for _, container := range newRS.Spec.Template.Spec.Containers {
			if container.Name == cpod.Spec.Containers[index].Name {
				cpod.Spec.Containers[index].Image = container.Image
			}
		}
	}

	_, err := c.updatePodTry(cpod)
	if err != nil {
		klog.Errorf("update inplace pod %s %s to rs hash %s failed, %v", cpod.Namespace, cpod.Name, podTemplateSpecHash, err)
		return false, err
	}
	return true, nil
}

func (c *Controller) updatePodTry(pod *v1.Pod) (newPod *v1.Pod, err error) {
	newPod, err = c.kubeclientset.CoreV1().Pods(pod.Namespace).Update(pod)
	if err == nil {
		return
	}
	if k8serr.IsConflict(err) {
		pod, err = c.kubeclientset.CoreV1().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})
		if err != nil {
			return
		}
		newPod, err = c.kubeclientset.CoreV1().Pods(pod.Namespace).Update(pod)
	}
	return
}

func (c *Controller) scaleReplicaSetAndRecordEventTry(rs *appsv1.ReplicaSet, newScale int32, rollout *v1alpha1.Rollout) (bool, *appsv1.ReplicaSet, error) {
	scaled, newRS, err := c.scaleReplicaSetAndRecordEvent(rs, newScale, rollout)
	if err == nil {
		return scaled, newRS, nil
	}
	var newErr error
	newRS, newErr = c.replicaSetLister.ReplicaSets(rs.Namespace).Get(rs.Name)
	if newErr != nil {
		return false, rs, err
	}
	*rs.Spec.Replicas = newScale
	return c.scaleReplicaSetAndRecordEvent(rs, newScale, rollout)
}

func (c *Controller) findPodMapForRollout(d *v1alpha1.Rollout, rsList []*appsv1.ReplicaSet) (map[types.UID]*v1.PodList, error) {
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, err
	}
	pods, err := c.podLister.Pods(d.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	podMap := make(map[types.UID]*v1.PodList, len(rsList))
	for _, rs := range rsList {
		podMap[rs.UID] = &v1.PodList{}
	}
	for _, pod := range pods {
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef == nil {
			continue
		}
		if podList, ok := podMap[controllerRef.UID]; ok {
			podList.Items = append(podList.Items, *pod)
		}
	}
	return podMap, nil
}

func calculateReplicaCountsForInplace(rollout *v1alpha1.Rollout, newRS *appsv1.ReplicaSet, stableRS *appsv1.ReplicaSet, oldRSs []*appsv1.ReplicaSet) (int32, int32) {
	newRSReplicaCount, stableRSReplicaCount := replicasetutil.CalculateReplicaCountsForCanary(rollout, newRS, stableRS, oldRSs)
	if stableRS == nil || newRS == nil || newRS.Name == stableRS.Name  {
		return newRSReplicaCount, stableRSReplicaCount
	}
	// CalculateReplicaCountsForCanary 在配置 unAvailable > 0 的时候,此时 newRSReplicaCountx < *(rollout.Spec.Replicas) 有可能出现，这里修正一下
	if stableRSReplicaCount == 0 {
		return *(rollout.Spec.Replicas), 0
	}
	subStable := *stableRS.Spec.Replicas - stableRSReplicaCount
	subNew := newRSReplicaCount - *newRS.Spec.Replicas

	if subStable == subNew {
		return newRSReplicaCount, stableRSReplicaCount
	}
	// stable 减少太多了，这些pod可以转移到 new rs，new rs 相应增加 stable 减少的
	if subStable > subNew {
		return *(stableRS.Spec.Replicas) + *(newRS.Spec.Replicas) - stableRSReplicaCount, stableRSReplicaCount
	}
	//增加的 new rs 比减少的 stable rs 少，这时候，虽然会创建一些新的 pod，但是只会最后去销毁多余的老pod，后面根据需求再改成是不是增加的new rs pod 等于 较少的 stable rs pod
	return newRSReplicaCount, stableRSReplicaCount

}
