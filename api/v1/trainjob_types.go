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

// TrainJobSpec defines the desired state of TrainJob
type TrainJobSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// foo is an example field of TrainJob. Edit trainjob_types.go to remove/update
	// +optional
	WorldSize         int32                   `json:"worldSize"`
	MasterPort        int32                   `json:"masterPort,omitempty"`
	Image             string                  `json:"image"`
	Command           []string                `json:"command,omitempty"`
	Args              []string                `json:"args,omitempty"`
	Resources         v1.ResourceRequirements `json:"resources,omitempty"`
	RetryLimit        int32                   `json:"retryLimit"`
	SchedulerName     string                  `json:"schedulerName,omitempty"`
	CheckpointSpec    *TrainJobCheckpointSpec `json:"checkpointSpec,omitempty"`
	QueueName         string                  `json:"queueName"`
	PriorityClassName string                  `json:"priorityClassName,omitempty"`
	ScheduleMaxCount  int32                   `json:"scheduleMaxCount,omitempty"`
}

type TrainJobCheckpointSpec struct {
	Enabled   bool   `json:"enabled,omitempty"`
	MountPath string `json:"mountPath,omitempty"`
	PVCName   string `json:"pvcName,omitempty"`
}
type FailureSummary struct {
	Attempt     int32       `json:"attempt"`
	Rank        int32       `json:"rank"`
	Reason      string      `json:"reason,omitempty"`
	Message     string      `json:"message,omitempty"`
	ExitCode    *int32      `json:"exitCode,omitempty"`
	ObservedAt  metav1.Time `json:"observedAt,omitempty"`
	PreemptedBy string      `json:"preemptedBy,omitempty"`
}

// TrainJobStatus defines the observed state of TrainJob.
type TrainJobStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the TrainJob resource.
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
	Conditions     []metav1.Condition `json:"conditions,omitempty"`
	Phase          TrainJobPhase      `json:"phase"`
	Attempt        int32              `json:"attempt"`
	RunningWorkers int32              `json:"runningWorkers,omitempty"`
	ReadyWorkers   int32              `json:"readyWorkers,omitempty"`
	LastFailure    *FailureSummary    `json:"lastFailure,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// TrainJob is the Schema for the trainjobs API
type TrainJob struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TrainJob
	// +required
	Spec TrainJobSpec `json:"spec"`

	// status defines the observed state of TrainJob
	// +optional
	Status TrainJobStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TrainJobList contains a list of TrainJob
type TrainJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TrainJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TrainJob{}, &TrainJobList{})
}

type TrainJobPhase string

const (
	TrainJobPhaseSubmitted TrainJobPhase = "Submitted"
	TrainJobPhaseQueued    TrainJobPhase = "Queued"
	TrainJobPhaseStarting  TrainJobPhase = "Starting"
	TrainJobPhaseRunning   TrainJobPhase = "Running"
	TrainJobPhaseRetrying  TrainJobPhase = "Retrying"
	TrainJobPhaseSucceeded TrainJobPhase = "Succeeded"
	TrainJobPhaseFailed    TrainJobPhase = "Failed"
	TrainJobPhasePreempted TrainJobPhase = "Preempted"
)
