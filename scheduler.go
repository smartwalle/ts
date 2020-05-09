package ts

import "time"

const (
	kPrefix = "/ts/"
)

type Handler func(key, value string)

type Scheduler interface {
	Handle(key string, handler Handler) error

	Add(key, spec, value string) error

	Remove(key, value string) error

	UpdateNextTime(key, value string, nextTime time.Time) error
}
