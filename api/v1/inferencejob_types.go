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

package v1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// InferenceJobSpec defines the desired state of InferenceJob
type InferenceJobSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	QueueName         string `json:"queueName"`
	SchedulerName     string `json:"schedulerName"`
	PriorityClassName string `json:"priorityClassName"`
	// MaxRestarts 是容器【原地重启】次数的上限。
	// 重启由 kubelet 按 Pod 的 restartPolicy 执行，Pod 对象和它占的卡都不动；
	// 控制器只读 Pod 的重启计数，超过上限进入 Failing，确认 Pod 删除后才置为 Failed。
	// 注意和 TrainJob.Spec.RetryLimit 区分：那个数的是"删掉整批 Pod、换新 attempt 重建"的次数。
	MaxRestarts int32    `json:"maxRestarts"`
	Command     []string `json:"command,omitempty"`
	Args        []string `json:"args,omitempty"`
	Image       string   `json:"image"`
	// Resources 是这【一个副本】的资源需求，原样透传到 Pod。
	// InferenceJob 只有一个副本，配额就按这一份扣减；
	// 别照搬 TrainJob.Spec.Resources 的语义（那个是单 worker 的量，配额扣减时要乘 WorldSize）。
	Resources v1.ResourceRequirements `json:"resources,omitempty"`
	// Volumes / VolumeMounts 原样透传到 Pod 和容器，控制器不改用户写的内容。
	// 例外：VolumeMounts 里没有任何挂载点是 /dev/shm 时，控制器会补一个
	// emptyDir{medium: Memory} 挂到 /dev/shm —— 容器默认的 /dev/shm 只有 64MiB，vLLM 起不来。
	// 需要自定义大小（例如 TP=2 要加大 sizeLimit）时，在这里显式挂 /dev/shm，控制器就不再补。
	Volumes []v1.Volume `json:"volumes,omitempty"`
	// VolumeMounts 与 Volumes 配对使用，/dev/shm 的补齐规则见 Volumes。
	VolumeMounts   []v1.VolumeMount `json:"volumeMounts,omitempty"`
	Env            []v1.EnvVar      `json:"env,omitempty"`
	ReadinessProbe *v1.Probe        `json:"readinessProbe,omitempty"`
}

// InferenceJobStatus defines the observed state of InferenceJob.
type InferenceJobStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the InferenceJob resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions  []metav1.Condition             `json:"conditions,omitempty"`
	Phase       InferenceJobPhase              `json:"phase"`
	LastFailure *InferencePodLastFailedSummary `json:"lastFailure,omitempty"`
}

type InferencePodLastFailedSummary struct {
	Reason     string      `json:"reason,omitempty"`
	Message    string      `json:"message,omitempty"`
	ExitCode   *int32      `json:"exitCode,omitempty"`
	ObservedAt metav1.Time `json:"observedAt,omitempty"`
}

type InferenceJobPhase string

const (
	InferenceJobPhaseQueued     InferenceJobPhase = "Queued"
	InferenceJobPhaseStarting   InferenceJobPhase = "Starting"
	InferenceJobPhaseRunning    InferenceJobPhase = "Running"
	InferenceJobPhaseRestarting InferenceJobPhase = "Restarting"
	InferenceJobPhaseFailing    InferenceJobPhase = "Failing"
	InferenceJobPhaseFailed     InferenceJobPhase = "Failed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// InferenceJob is the Schema for the inferencejobs API
type InferenceJob struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of InferenceJob
	// +required
	Spec InferenceJobSpec `json:"spec"`

	// status defines the observed state of InferenceJob
	// +optional
	Status InferenceJobStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// InferenceJobList contains a list of InferenceJob
type InferenceJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []InferenceJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&InferenceJob{}, &InferenceJobList{})
}
