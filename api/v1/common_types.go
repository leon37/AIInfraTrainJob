package v1

type JobType string

const (
	JobTypeTrain     JobType = "Train"
	JobTypeInference JobType = "Inference"
)
