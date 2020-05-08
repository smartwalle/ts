package internal

type JobStatus int

const (
	JobStatusValid   JobStatus = 1
	JobStatusInvalid JobStatus = 2
)

type Job struct {
	Key      string    `json:"key"`
	Path     string    `json:"path"`
	FullPath string    `json:"full_path"`
	Value    string    `json:"value"`
	Spec     string    `json:"spec"`
	NextTime string    `json:"next_time"`
	Type     int       `json:"type"`
	Status   JobStatus `json:"status"`
}
