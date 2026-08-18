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
	"sort"

	v1 "k8s.io/api/core/v1"
	v2 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	batchv1 "aiinfra.example.com/trainjob/api/v1"
)

// QueueReconciler reconciles a Queue object
type QueueReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//1. 取到当前这个 Queue 对象(reconcile 的 key 就是它);读出 Spec.quota 和 Status.used。
//2. 先清理:把 Status.used 里那些"作业已不存在 / 已是 Succeeded/Failed"的项剔掉(这一步同时解决了我之前挂起的那个漏洞——kubectl delete 直接删掉的作业,你靠"逐项检查它还在不在"来兜底,而不是只靠捕捉终态事件)。
//3. 算 remaining = quota - sum(used 里的快照)(逐维度)。
//4. 用 spec.queueName 索引列出该队列所有 Queued 作业,按优先级降序排(同级按创建时间升序)。
//5. 看排第一的那个放不放得下:放得下 → 往 Status.used 追加它的 {名字, 请求快照} → Status().Update() 写回;放不下 → 直接结束(block)。
//6. 写回若遇到版本冲突(409),正常返回 error 让框架重试即可(controller-runtime 会重新入队)。

// +kubebuilder:rbac:groups=batch.example.com,resources=queues,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.example.com,resources=queues/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.example.com,resources=queues/finalizers,verbs=update
// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=priorityclasses,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Queue object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *QueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)
	var queue batchv1.Queue
	err := r.Client.Get(ctx, req.NamespacedName, &queue)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	const queueNameKey = "spec.queueName"
	var curJobs batchv1.TrainJobList
	err = r.Client.List(ctx, &curJobs, client.MatchingFields{queueNameKey: queue.GetName()}, client.InNamespace(queue.GetNamespace()))
	if err != nil {
		return ctrl.Result{}, err
	}
	jobMap := make(map[string]batchv1.TrainJob)
	for _, job := range curJobs.Items {
		jobMap[job.Name] = job
	}

	var update bool
	var newUsed []batchv1.QueueUsed
	for _, usedIns := range queue.Status.Used {
		job, ok := jobMap[usedIns.JobName]
		if !ok {
			update = true
			continue
		}
		if job.Status.Phase == batchv1.TrainJobPhaseSucceeded || job.Status.Phase == batchv1.TrainJobPhaseFailed {
			update = true
			continue
		}
		newUsed = append(newUsed, usedIns)
	}
	queue.Status.Used = newUsed

	remaining := queue.Spec.ResourceQuota.DeepCopy()
	for _, used := range queue.Status.Used {
		for resourceName, quantity := range used.ResourceUsed {
			curQuantity := remaining[resourceName].DeepCopy()
			curQuantity.Sub(quantity)
			remaining[resourceName] = curQuantity
		}
	}

	var queuedJobs []batchv1.TrainJob
	for _, job := range jobMap {
		if job.Status.Phase == batchv1.TrainJobPhaseQueued {
			queuedJobs = append(queuedJobs, job)
		}
	}

	if len(queuedJobs) > 0 {
		priorityClassMap := make(map[string]int32)
		var pcl v2.PriorityClassList
		err = r.Client.List(ctx, &pcl)
		if err != nil {
			return ctrl.Result{}, err
		}
		for _, pclItem := range pcl.Items {
			priorityClassMap[pclItem.Name] = pclItem.Value
		}

		sort.Slice(queuedJobs, func(i, j int) bool {
			return priorityClassMap[queuedJobs[i].Spec.PriorityClassName] > priorityClassMap[queuedJobs[j].Spec.PriorityClassName]
		})

		curJob := queuedJobs[0]
		resourcesNeeded := make(v1.ResourceList)
		var curValid = true
		for resourceName, quantityNeed := range curJob.Spec.Resources.Requests {
			tmp := remaining[resourceName].DeepCopy()
			if !quantityNeed.Mul(int64(curJob.Spec.WorldSize)) {
				curValid = false
				break
			}
			if tmp.Cmp(quantityNeed) < 0 {
				curValid = false
				break
			}
			resourcesNeeded[resourceName] = quantityNeed
		}

		if curValid {
			needAppend := true
			for _, used := range queue.Status.Used {
				if used.JobName == curJob.Name {
					needAppend = false
					break
				}
			}
			if needAppend {
				queue.Status.Used = append(queue.Status.Used, batchv1.QueueUsed{
					JobName:      curJob.Name,
					ResourceUsed: resourcesNeeded,
				})
				update = true
			}
		}
	}

	if update {
		return ctrl.Result{}, r.Client.Status().Update(ctx, &queue)
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *QueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Queue{}).
		Named("queue").
		Watches(&batchv1.TrainJob{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			job, ok := object.(*batchv1.TrainJob)
			if !ok {
				return []reconcile.Request{}
			}
			//if !slices.Contains([]batchv1.TrainJobPhase{batchv1.TrainJobPhaseQueued, batchv1.TrainJobPhaseSucceeded, batchv1.TrainJobPhaseFailed}, job.Status.Phase) {
			//	return []reconcile.Request{}
			//}
			if job.Spec.QueueName == "" {
				return []reconcile.Request{}
			}

			return []reconcile.Request{{
				NamespacedName: types.NamespacedName{
					Namespace: object.GetNamespace(),
					Name:      job.Spec.QueueName,
				},
			}}
		})).
		Complete(r)
}
