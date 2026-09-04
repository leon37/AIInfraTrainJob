/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	batchv1 "aiinfra.example.com/trainjob/api/v1"
)

// TrainJobReconciler reconciles a TrainJob object
type TrainJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=batch.example.com,resources=trainjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.example.com,resources=trainjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.example.com,resources=trainjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch.example.com,resources=podgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the TrainJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile

func (r *TrainJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)
	var trainJob batchv1.TrainJob
	err := r.Get(ctx, req.NamespacedName, &trainJob)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}
	var needUpdate bool
	switch trainJob.Status.Phase {
	case "":
		trainJob.Status.Phase = batchv1.TrainJobPhaseSubmitted
		needUpdate = true
	case batchv1.TrainJobPhaseSubmitted:
		trainJob.Status.Phase = batchv1.TrainJobPhaseQueued
		needUpdate = true
	case batchv1.TrainJobPhaseQueued:
		var queue batchv1.Queue
		err = r.Get(ctx, client.ObjectKey{Namespace: trainJob.Namespace, Name: trainJob.Spec.QueueName}, &queue)
		if err != nil {
			if errors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		for _, used := range queue.Status.Used {
			if used.JobName == trainJob.GetName() {
				trainJob.Status.Phase = batchv1.TrainJobPhaseStarting
				needUpdate = true
				break
			}
		}
	case batchv1.TrainJobPhaseFailed, batchv1.TrainJobPhaseSucceeded:
		return ctrl.Result{}, nil
	case batchv1.TrainJobPhaseRetrying, batchv1.TrainJobPhaseStarting, batchv1.TrainJobPhaseRunning:
		err = r.ensureMasterService(ctx, &trainJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		err, failed := r.ensureWorkerPodGroup(ctx, &trainJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		if failed {
			return ctrl.Result{}, r.Status().Update(ctx, &trainJob)
		}
		err = r.ensureWorkerPods(ctx, &trainJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		summary, err := r.aggregateWorkerStatus(ctx, &trainJob)
		if err != nil {
			return ctrl.Result{}, err
		}
		if summary.FailedPod != nil {
			err = r.handleFailedAttempt(ctx, &trainJob, summary)
			if err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.Status().Update(ctx, &trainJob)
		}
		changed := applyWorkerStatus(&trainJob, summary)
		needUpdate = changed
	case batchv1.TrainJobPhasePreempted:
		var podGroup batchv1.PodGroup
		err = r.Get(ctx, types.NamespacedName{Namespace: trainJob.Namespace, Name: workerPodGroupName(&trainJob)}, &podGroup)
		if err != nil {
			return ctrl.Result{}, err
		}
		preemptedBy := podGroup.Status.PreemptedBy
		var preemptingTrainJob batchv1.TrainJob
		requeue := false
		err = r.Get(ctx, types.NamespacedName{Namespace: trainJob.Namespace, Name: preemptedBy}, &preemptingTrainJob)
		if err != nil {
			if errors.IsNotFound(err) {
				requeue = true
			} else {
				return ctrl.Result{}, err
			}
		}
		if preemptingTrainJob.Status.Phase == batchv1.TrainJobPhaseRunning || preemptingTrainJob.Status.Phase == batchv1.TrainJobPhaseFailed {
			requeue = true
		}
		if requeue {
			err = r.deletePodGroup(ctx, &trainJob)
			if err != nil {
				return ctrl.Result{}, err
			}
			trainJob.Status.Phase = batchv1.TrainJobPhaseQueued
			needUpdate = true
		}
	}

	if needUpdate {
		err = r.Status().Update(ctx, &trainJob)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TrainJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	const queueNameKey = "spec.queueName"
	const preemptedByKey = "status.preemptedBy"
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.TrainJob{}, queueNameKey, func(rawObj client.Object) []string {
		job := rawObj.(*batchv1.TrainJob)
		if job.Spec.QueueName == "" {
			return nil
		}
		return []string{job.Spec.QueueName}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.PodGroup{}, preemptedByKey, func(rawObj client.Object) []string {
		podGroup := rawObj.(*batchv1.PodGroup)
		if podGroup.Status.PreemptedBy == "" {
			return nil
		}
		return []string{podGroup.Status.PreemptedBy}
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.TrainJob{}).
		Owns(&v1.Pod{}).
		Owns(&batchv1.PodGroup{}).
		Watches(&batchv1.Queue{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			var jobs batchv1.TrainJobList

			if err := r.List(ctx, &jobs, client.MatchingFields{queueNameKey: object.GetName()}); err != nil {
				return []reconcile.Request{}
			}
			reqs := make([]ctrl.Request, 0, len(jobs.Items))
			for _, job := range jobs.Items {
				if job.Status.Phase != batchv1.TrainJobPhaseQueued {
					continue
				}

				reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
					Name:      job.Name,
					Namespace: job.Namespace,
				}})
			}
			return reqs
		})).Watches(&batchv1.TrainJob{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
		req := make([]ctrl.Request, 0)

		var podGroups batchv1.PodGroupList
		if err := r.List(ctx, &podGroups, client.MatchingFields{preemptedByKey: object.GetName()}); err != nil {
			return []reconcile.Request{}
		}
		for _, podGroup := range podGroups.Items {
			controller := metav1.GetControllerOf(&podGroup)
			if controller == nil {
				continue
			}
			req = append(req, ctrl.Request{NamespacedName: types.NamespacedName{
				Name:      controller.Name,
				Namespace: podGroup.Namespace,
			}})
		}

		return req
	})).
		Complete(r)
}

func masterServiceName(trainJob *batchv1.TrainJob) string {
	return fmt.Sprintf("%s-master", trainJob.Name)
}

func masterServiceSelector(trainJob *batchv1.TrainJob) map[string]string {
	return map[string]string{
		"rank":          "0",
		"attempt":       strconv.Itoa(int(trainJob.Status.Attempt)),
		"trainjob-name": trainJob.Name,
	}
}

func workerPodLabels(trainJob *batchv1.TrainJob, rank int32) map[string]string {
	return map[string]string{
		"rank":          strconv.Itoa(int(rank)),
		"trainjob-name": trainJob.Name,
		"attempt":       strconv.Itoa(int(trainJob.Status.Attempt)),
		"pod-group":     workerPodGroupName(trainJob),
	}
}

func masterAddr(trainJob *batchv1.TrainJob) string {
	return masterServiceName(trainJob) + "." + trainJob.Namespace + ".svc"
}

func masterPort(trainJob *batchv1.TrainJob) int32 {
	if trainJob.Spec.MasterPort > 0 {
		return trainJob.Spec.MasterPort
	}
	return 29500
}

func workerEnv(trainJob *batchv1.TrainJob, rank int32) []v1.EnvVar {
	return []v1.EnvVar{
		{Name: "MASTER_ADDR", Value: masterAddr(trainJob)},
		{Name: "MASTER_PORT", Value: strconv.Itoa(int(masterPort(trainJob)))},
		{Name: "WORLD_SIZE", Value: strconv.Itoa(int(trainJob.Spec.WorldSize))},
		{Name: "RANK", Value: strconv.Itoa(int(rank))},
		{Name: "LOCAL_RANK", Value: "0"},
	}
}

func workerPodName(trainJob *batchv1.TrainJob, rank int32) string {
	var podName strings.Builder
	podName.WriteString(trainJob.Name)
	podName.WriteString("-attempt-")
	podName.WriteString(strconv.Itoa(int(trainJob.Status.Attempt)))
	podName.WriteString("-rank-")
	podName.WriteString(strconv.Itoa(int(rank)))
	return podName.String()
}

func buildWorkerPod(trainJob *batchv1.TrainJob, rank int32) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerPodName(trainJob, rank),
			Namespace: trainJob.Namespace,
			Labels:    workerPodLabels(trainJob, rank),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      "worker",
					Image:     trainJob.Spec.Image,
					Command:   trainJob.Spec.Command,
					Args:      trainJob.Spec.Args,
					Env:       workerEnv(trainJob, rank),
					Resources: trainJob.Spec.Resources,
				},
			},
			RestartPolicy:     v1.RestartPolicyNever,
			SchedulerName:     trainJob.Spec.SchedulerName,
			PriorityClassName: trainJob.Spec.PriorityClassName,
		},
	}
	if trainJob.Spec.CheckpointSpec != nil && trainJob.Spec.CheckpointSpec.Enabled && rank == 0 {
		pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
			{
				Name:      "checkpoint",
				MountPath: trainJob.Spec.CheckpointSpec.MountPath,
			},
		}
		pod.Spec.Volumes = []v1.Volume{
			{
				Name: "checkpoint",
				VolumeSource: v1.VolumeSource{
					PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
						ClaimName: trainJob.Spec.CheckpointSpec.PVCName,
					},
				},
			},
		}
	}
	return pod
}

func buildMasterService(trainJob *batchv1.TrainJob) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      masterServiceName(trainJob),
			Namespace: trainJob.Namespace,
		},
		Spec: v1.ServiceSpec{
			Selector: masterServiceSelector(trainJob),
			Type:     v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{
					Port:       masterPort(trainJob),
					TargetPort: intstr.FromInt(int(masterPort(trainJob))),
				},
			},
		},
	}
}

func (r *TrainJobReconciler) ensureMasterService(ctx context.Context, trainJob *batchv1.TrainJob) error {
	var svc v1.Service
	err := r.Get(ctx, types.NamespacedName{Name: masterServiceName(trainJob), Namespace: trainJob.Namespace}, &svc)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		newSvc := buildMasterService(trainJob)
		err = ctrl.SetControllerReference(trainJob, newSvc, r.Scheme)
		if err != nil {
			return err
		}
		err = r.Create(ctx, newSvc)
		if err != nil {
			return err
		}
		return nil
	}

	desiredSelector := masterServiceSelector(trainJob)
	desiredPort := masterPort(trainJob)

	update := false
	if !reflect.DeepEqual(desiredSelector, svc.Spec.Selector) {
		svc.Spec.Selector = desiredSelector
		update = true
	}
	if len(svc.Spec.Ports) != 1 {
		svc.Spec.Ports = []v1.ServicePort{
			{
				Port:       masterPort(trainJob),
				TargetPort: intstr.FromInt(int(masterPort(trainJob))),
			},
		}
		update = true
	} else {
		if svc.Spec.Ports[0].Port != desiredPort {
			svc.Spec.Ports[0].Port = desiredPort
			update = true
		}
		if svc.Spec.Ports[0].TargetPort.IntVal != desiredPort {
			svc.Spec.Ports[0].TargetPort.IntVal = desiredPort
			update = true
		}
	}

	if update {
		err = r.Update(ctx, &svc)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *TrainJobReconciler) ensureWorkerPods(ctx context.Context, trainJob *batchv1.TrainJob) error {
	for rank := 0; rank < int(trainJob.Spec.WorldSize); rank++ {
		var pod v1.Pod
		err := r.Get(ctx, types.NamespacedName{Namespace: trainJob.Namespace, Name: workerPodName(trainJob, int32(rank))}, &pod)
		if err == nil {
			continue
		}

		if !errors.IsNotFound(err) {
			return err
		}
		newPod := buildWorkerPod(trainJob, int32(rank))
		err = ctrl.SetControllerReference(trainJob, newPod, r.Scheme)
		if err != nil {
			return err
		}
		err = r.Create(ctx, newPod)
		if err != nil {
			return err
		}
	}
	return nil
}

func isPodReady(pod v1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == v1.PodReady && cond.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

type workerStatusSummary struct {
	Total            int32
	RunningWorkers   int32
	ReadyWorkers     int32
	SucceededWorkers int32
	FailedPod        *v1.Pod
}

func (r *TrainJobReconciler) aggregateWorkerStatus(ctx context.Context, trainJob *batchv1.TrainJob) (*workerStatusSummary, error) {
	summary := &workerStatusSummary{}
	var pods v1.PodList
	err := r.List(ctx, &pods, client.MatchingLabels{"trainjob-name": trainJob.Name, "attempt": strconv.Itoa(int(trainJob.Status.Attempt))})
	if err != nil {
		return nil, err
	}
	summary.Total = int32(len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if isPodReady(*pod) {
			summary.ReadyWorkers++
		}
		if pod.Status.Phase == v1.PodRunning {
			summary.RunningWorkers++
		}
		if pod.Status.Phase == v1.PodSucceeded {
			summary.SucceededWorkers++
		}
		if pod.Status.Phase == v1.PodFailed && summary.FailedPod == nil {
			summary.FailedPod = pod
		}
	}
	return summary, nil
}

func failureSummaryFromPod(pod *v1.Pod) *batchv1.FailureSummary {
	label := pod.GetLabels()
	summary := &batchv1.FailureSummary{
		Reason:     pod.Status.Reason,
		Message:    pod.Status.Message,
		ObservedAt: metav1.Now(),
	}
	attempt, err := strconv.Atoi(label["attempt"])
	if err == nil {
		summary.Attempt = int32(attempt)
	}
	rank, err := strconv.Atoi(label["rank"])
	if err == nil {
		summary.Rank = int32(rank)
	}

	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Terminated != nil {
			summary.Reason = containerStatus.State.Terminated.Reason
			summary.Message = containerStatus.State.Terminated.Message
			exitCode := containerStatus.State.Terminated.ExitCode
			summary.ExitCode = &exitCode
			break
		}
	}
	return summary
}

func failureSummaryFromPodGroup(podGroup *batchv1.PodGroup) *batchv1.FailureSummary {
	summary := &batchv1.FailureSummary{
		Reason:      podGroup.Status.Reason,
		ObservedAt:  metav1.Now(),
		PreemptedBy: podGroup.Status.PreemptedBy,
	}
	return summary
}

func nextPhase(trainJob *batchv1.TrainJob, summary *workerStatusSummary) batchv1.TrainJobPhase {
	if summary.SucceededWorkers == trainJob.Spec.WorldSize {
		return batchv1.TrainJobPhaseSucceeded
	}

	if summary.Total < trainJob.Spec.WorldSize {
		return batchv1.TrainJobPhaseStarting
	}

	if summary.RunningWorkers+summary.SucceededWorkers < trainJob.Spec.WorldSize {
		return batchv1.TrainJobPhaseStarting
	}

	return batchv1.TrainJobPhaseRunning
}

func shouldRetry(trainJob *batchv1.TrainJob, summary *workerStatusSummary) bool {
	if summary.FailedPod != nil && trainJob.Status.Attempt < trainJob.Spec.RetryLimit {
		return true
	}
	return false
}

func (r *TrainJobReconciler) deletePodGroup(ctx context.Context, trainJob *batchv1.TrainJob) error {
	var podGroup batchv1.PodGroup
	err := r.Get(ctx, client.ObjectKey{Name: workerPodGroupName(trainJob), Namespace: trainJob.Namespace}, &podGroup)
	if err != nil {
		return err
	}
	err = r.Delete(ctx, &podGroup)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *TrainJobReconciler) deleteWorkerPodsForAttempt(ctx context.Context, trainJob *batchv1.TrainJob, attempt int32) error {
	var pods v1.PodList
	err := r.List(ctx, &pods, client.MatchingLabels{"trainjob-name": trainJob.Name, "attempt": strconv.Itoa(int(attempt))})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		err = r.Delete(ctx, &pod)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *TrainJobReconciler) deleteServiceForAttempt(ctx context.Context, trainJob *batchv1.TrainJob) error {
	var svc v1.Service
	err := r.Get(ctx, types.NamespacedName{Namespace: trainJob.Namespace, Name: masterServiceName(trainJob)}, &svc)
	if err != nil {
		return err
	}

	err = r.Delete(ctx, &svc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *TrainJobReconciler) handleFailedAttempt(ctx context.Context, trainJob *batchv1.TrainJob, summary *workerStatusSummary) error {
	trainJob.Status.LastFailure = failureSummaryFromPod(summary.FailedPod)
	failedAttempt := trainJob.Status.Attempt
	err := r.deleteWorkerPodsForAttempt(ctx, trainJob, failedAttempt)
	if err != nil {
		return err
	}
	if shouldRetry(trainJob, summary) {
		trainJob.Status.Phase = batchv1.TrainJobPhaseRetrying
		trainJob.Status.Attempt = failedAttempt + 1
		return nil
	}
	trainJob.Status.Phase = batchv1.TrainJobPhaseFailed
	return nil
}

func applyWorkerStatus(trainJob *batchv1.TrainJob, summary *workerStatusSummary) bool {
	if trainJob.Status.Phase == batchv1.TrainJobPhaseSucceeded || trainJob.Status.Phase == batchv1.TrainJobPhaseFailed {
		return false
	}
	trainJob.Status.RunningWorkers = summary.RunningWorkers
	trainJob.Status.ReadyWorkers = summary.ReadyWorkers
	phase := nextPhase(trainJob, summary)
	changed := trainJob.Status.Phase != phase
	trainJob.Status.Phase = phase
	return changed
}

func (r *TrainJobReconciler) ensureWorkerPodGroup(ctx context.Context, trainJob *batchv1.TrainJob) (error, bool) {
	var podGroup batchv1.PodGroup
	err := r.Get(ctx, types.NamespacedName{Namespace: trainJob.Namespace, Name: workerPodGroupName(trainJob)}, &podGroup)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err, false
		}
		newPodGroup := buildWorkerPodGroup(trainJob)
		err = ctrl.SetControllerReference(trainJob, newPodGroup, r.Scheme)
		if err != nil {
			return err, false
		}

		err = r.Create(ctx, newPodGroup)
		if err != nil {
			return err, false
		}
		return nil, false
	}

	if trainJob.Status.Phase == batchv1.TrainJobPhaseStarting {
		if podGroup.Status.Failed {
			summary := failureSummaryFromPodGroup(&podGroup)
			err = r.deleteWorkerPodsForAttempt(ctx, trainJob, trainJob.Status.Attempt)
			if err != nil {
				return err, true
			}
			trainJob.Status.Phase = batchv1.TrainJobPhaseFailed
			trainJob.Status.LastFailure = summary
			return nil, true
		}
	}

	if trainJob.Status.Phase == batchv1.TrainJobPhaseRunning {
		if podGroup.Status.PreemptedBy != "" {
			summary := failureSummaryFromPodGroup(&podGroup)
			err = r.deleteWorkerPodsForAttempt(ctx, trainJob, trainJob.Status.Attempt)
			if err != nil {
				return err, true
			}
			trainJob.Status.Phase = batchv1.TrainJobPhasePreempted
			trainJob.Status.LastFailure = summary
			return nil, true
		}
		if len(podGroup.Status.PreemptionDetail) > 0 {
			podGroup.Status.PreemptionDetail = nil
			err = r.Status().Update(ctx, &podGroup)
			if err != nil {
				return err, true
			}
		}
	}

	return nil, false
}

func workerPodGroupName(trainJob *batchv1.TrainJob) string {
	var podName strings.Builder
	podName.WriteString(trainJob.Name)
	podName.WriteString("-attempt-")
	podName.WriteString(strconv.Itoa(int(trainJob.Status.Attempt)))
	return podName.String()
}

func buildWorkerPodGroup(trainJob *batchv1.TrainJob) *batchv1.PodGroup {
	podGroup := &batchv1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerPodGroupName(trainJob),
			Namespace: trainJob.Namespace,
		},
		Spec: batchv1.PodGroupSpec{
			MinMember:              trainJob.Spec.WorldSize,
			ScheduleTimeoutSeconds: trainJob.Spec.ScheduleTimeoutSeconds,
		},
	}

	if podGroup.Spec.ScheduleTimeoutSeconds <= 0 {
		podGroup.Spec.ScheduleTimeoutSeconds = 300
	}

	return podGroup
}
