package internal

type JobStatus int

const (
	JobStatusValid   JobStatus = 1
	JobStatusInvalid JobStatus = 2
)

type JobType int

func (t JobType) String() string {
	if t == JobTypeLoop {
		return "循环任务"
	}
	return "单次任务"
}

const (
	JobTypeOnce JobType = 1 // 添加一次，执行一次
	JobTypeLoop JobType = 2 // 添加一次，循环执行
)

type Job struct {
	Key        string    `json:"key"`
	Path       string    `json:"path"`
	FullPath   string    `json:"full_path"`
	Value      string    `json:"value"`
	Spec       string    `json:"spec"`
	NextTime   string    `json:"next_time"`
	UpdateTime string    `json:"update_time"`
	Type       JobType   `json:"type"`
	Status     JobStatus `json:"status"`
}
