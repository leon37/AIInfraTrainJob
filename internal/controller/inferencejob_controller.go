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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	batchv1 "aiinfra.example.com/trainjob/api/v1"
)

// InferenceJobReconciler reconciles a InferenceJob object
type InferenceJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=batch.example.com,resources=inferencejobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.example.com,resources=inferencejobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.example.com,resources=inferencejobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the InferenceJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *InferenceJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)
	var inferenceJob batchv1.InferenceJob
	err := r.Get(ctx, req.NamespacedName, &inferenceJob)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	podObjectKey := client.ObjectKey{Namespace: inferenceJob.Namespace, Name: inferenceJob.Name}
	var needUpdate bool

	switch inferenceJob.Status.Phase {
	case "":
		inferenceJob.Status.Phase = batchv1.InferenceJobPhaseQueued
		needUpdate = true

	case batchv1.InferenceJobPhaseQueued:
		var queue batchv1.Queue
		err = r.Get(ctx, client.ObjectKey{Namespace: inferenceJob.Namespace, Name: inferenceJob.Spec.QueueName}, &queue)
		if err != nil {
			if errors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		for _, used := range queue.Status.Used {
			if used.JobName == inferenceJob.GetName() && used.JobType == batchv1.QueuedJobTypeInference {
				inferenceJob.Status.Phase = batchv1.InferenceJobPhaseStarting
				needUpdate = true
				break
			}
		}
	case batchv1.InferenceJobPhaseStarting, batchv1.InferenceJobPhaseRunning, batchv1.InferenceJobPhaseRestarting:
		var pod v1.Pod
		var podNotFound bool
		err = r.Get(ctx, podObjectKey, &pod)
		if err != nil {
			if errors.IsNotFound(err) {
				podNotFound = true
			} else {
				return ctrl.Result{}, err
			}
		}
		podStatus := getInferenceJobPodStatusByPod(&pod)
		if podNotFound {
			podStatus = getInferenceJobPodStatusByPod(nil)
		}
		curPhase := inferenceJob.Status.Phase
		next := getInferenceJobNextPhase(curPhase, podStatus, pod, inferenceJob.Spec.MaxRestarts)
		if next != inferenceJob.Status.Phase {
			inferenceJob.Status.Phase = next
			needUpdate = true
		}
		if podNotFound {
			err = r.ensureWorkerPod(ctx, &inferenceJob)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		if next == batchv1.InferenceJobPhaseFailing {
			for _, container := range pod.Status.ContainerStatuses {
				if container.State.Terminated != nil {
					inferenceJob.Status.LastFailure = &batchv1.InferencePodLastFailedSummary{
						Reason:     container.State.Terminated.Reason,
						Message:    container.State.Terminated.Message,
						ExitCode:   &container.State.Terminated.ExitCode,
						ObservedAt: metav1.Now(),
					}
				} else if container.LastTerminationState.Terminated != nil {
					inferenceJob.Status.LastFailure = &batchv1.InferencePodLastFailedSummary{
						Reason:     container.LastTerminationState.Terminated.Reason,
						Message:    container.LastTerminationState.Terminated.Message,
						ExitCode:   &container.LastTerminationState.Terminated.ExitCode,
						ObservedAt: metav1.Now(),
					}
				}
			}
		}
	case batchv1.InferenceJobPhaseFailing:
		var pod v1.Pod
		err = r.Get(ctx, podObjectKey, &pod)
		if err != nil {
			if errors.IsNotFound(err) {
				inferenceJob.Status.Phase = batchv1.InferenceJobPhaseFailed
				needUpdate = true
			} else {
				return ctrl.Result{}, err
			}
		} else {
			err = r.Delete(ctx, &pod)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
	case batchv1.InferenceJobPhaseFailed:
		return ctrl.Result{}, nil
	}

	if needUpdate {
		err = r.Status().Update(ctx, &inferenceJob)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func getInferenceJobPodStatusByPod(p *v1.Pod) InferenceJobPodStatus {
	if p == nil || p.DeletionTimestamp != nil {
		return InferenceJobPodStatusNotExist
	}
	if isPodReady(*p) {
		return InferenceJobPodStatusReady
	}

	for _, container := range p.Status.ContainerStatuses {
		if (container.State.Waiting != nil && container.LastTerminationState.Terminated != nil) || container.State.Terminated != nil {
			return InferenceJobPodStatusCrash
		}
	}
	return InferenceJobPodStatusNotReady
}

func getInferenceJobNextPhase(curPhase batchv1.InferenceJobPhase, podStatus InferenceJobPodStatus, pod v1.Pod, maxRestarts int32) batchv1.InferenceJobPhase {
	podRestartCount := getPodRestartCount(pod)
	if podRestartCount > maxRestarts {
		return batchv1.InferenceJobPhaseFailing
	}
	switch podStatus {
	case InferenceJobPodStatusNotReady:
		return curPhase
	case InferenceJobPodStatusNotExist:
		if curPhase == batchv1.InferenceJobPhaseStarting {
			return batchv1.InferenceJobPhaseStarting
		}
		return batchv1.InferenceJobPhaseRestarting
	case InferenceJobPodStatusReady:
		return batchv1.InferenceJobPhaseRunning
	case InferenceJobPodStatusCrash:
		if podRestartCount >= maxRestarts {
			return batchv1.InferenceJobPhaseFailing
		}
		return batchv1.InferenceJobPhaseRestarting
	}
	return curPhase
}

func getPodRestartCount(pod v1.Pod) int32 {
	var ret int32
	for _, container := range pod.Status.ContainerStatuses {
		ret += container.RestartCount
	}
	return ret
}

func (r *InferenceJobReconciler) ensureWorkerPod(ctx context.Context, inferenceJob *batchv1.InferenceJob) error {
	var pod v1.Pod
	err := r.Get(ctx, types.NamespacedName{Namespace: inferenceJob.Namespace, Name: inferenceJob.Name}, &pod)
	if err == nil || !errors.IsNotFound(err) {
		return err
	}

	newPod := buildInferencePod(inferenceJob)
	err = ctrl.SetControllerReference(inferenceJob, newPod, r.Scheme)
	if err != nil {
		return err
	}
	err = r.Create(ctx, newPod)
	if err != nil {
		return err
	}

	return nil
}

func buildInferencePod(inferenceJob *batchv1.InferenceJob) *v1.Pod {
	volumeMounts := inferenceJob.Spec.VolumeMounts
	volumes := inferenceJob.Spec.Volumes
	var findVolumeMountSM bool
	var volumeSmName = "dshm"
	for _, vm := range volumeMounts {
		if vm.MountPath == "/dev/shm" {
			findVolumeMountSM = true
			volumeSmName = vm.Name
			break
		}
	}

	if !findVolumeMountSM {
		volumeMounts = append(volumeMounts, v1.VolumeMount{
			Name:      volumeSmName,
			MountPath: "/dev/shm",
		})
		sizeLimit := resource.MustParse("1Gi")
		volumes = append(volumes, v1.Volume{
			Name: volumeSmName,
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{
					Medium:    v1.StorageMediumMemory,
					SizeLimit: &sizeLimit,
				},
			},
		})
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: inferenceJob.Namespace,
			Name:      inferenceJob.Name,
			Labels:    map[string]string{"jobName": inferenceJob.Name, "jobType": string(batchv1.QueuedJobTypeInference)},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:           "worker",
					Image:          inferenceJob.Spec.Image,
					Command:        inferenceJob.Spec.Command,
					Args:           inferenceJob.Spec.Args,
					Env:            inferenceJob.Spec.Env,
					Resources:      inferenceJob.Spec.Resources,
					ReadinessProbe: inferenceJob.Spec.ReadinessProbe,
					LivenessProbe:  inferenceJob.Spec.LivenessProbe,
					StartupProbe:   inferenceJob.Spec.StartupProbe,
					VolumeMounts:   volumeMounts,
				},
			},
			RestartPolicy:     v1.RestartPolicyAlways,
			SchedulerName:     inferenceJob.Spec.SchedulerName,
			PriorityClassName: inferenceJob.Spec.PriorityClassName,
			Volumes:           volumes,
		},
	}

	return pod
}

// SetupWithManager sets up the controller with the Manager.
func (r *InferenceJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.InferenceJob{}, queueNameKey, func(rawObj client.Object) []string {
		job := rawObj.(*batchv1.InferenceJob)
		if job.Spec.QueueName == "" {
			return nil
		}
		return []string{job.Spec.QueueName}
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.InferenceJob{}).
		Owns(&v1.Pod{}).
		Watches(&batchv1.Queue{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			var jobs batchv1.InferenceJobList

			if err := r.List(ctx, &jobs, client.MatchingFields{queueNameKey: object.GetName()}); err != nil {
				return []reconcile.Request{}
			}
			reqs := make([]ctrl.Request, 0, len(jobs.Items))
			for _, job := range jobs.Items {
				if job.Status.Phase != batchv1.InferenceJobPhaseQueued {
					continue
				}

				reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
					Name:      job.Name,
					Namespace: job.Namespace,
				}})
			}
			return reqs
		})).
		Named("inferencejob").
		Complete(r)
}

type InferenceJobPodStatus string

const (
	InferenceJobPodStatusReady    InferenceJobPodStatus = "Ready"
	InferenceJobPodStatusNotExist InferenceJobPodStatus = "NotExist"
	InferenceJobPodStatusCrash    InferenceJobPodStatus = "Crash"
	InferenceJobPodStatusNotReady InferenceJobPodStatus = "NotReady"
)
