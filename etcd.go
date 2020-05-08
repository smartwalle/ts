package ts

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/smartwalle/ts/internal"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	kSchedulerJob  = "/ts/job"
	kSchedulerLock = "/ts/lock"
)

type etcdScheduler struct {
	prefix     string
	lockPrefix string
	client     *clientv3.Client
	parser     internal.Parser
	location   *time.Location

	mu       *sync.Mutex
	handlers map[string]Handler
}

func NewETCDScheduler(prefix string, client *clientv3.Client) Scheduler {
	var s = &etcdScheduler{}
	s.prefix = path.Join(kSchedulerJob, "/", prefix)
	s.lockPrefix = path.Join(kSchedulerLock, "/", prefix)
	s.client = client
	s.parser = internal.NewParser(internal.Minute | internal.Hour | internal.Dom | internal.Month | internal.Dow | internal.Descriptor)
	s.location = time.UTC
	s.mu = &sync.Mutex{}
	s.handlers = make(map[string]Handler)
	s.watch()
	return s
}

func (this *etcdScheduler) buildPath(key, value string) string {
	return path.Join(this.prefix, key, value)
}

func (this *etcdScheduler) Handle(key string, handler Handler) error {
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return fmt.Errorf("empty key string")
	}

	this.mu.Lock()
	defer this.mu.Unlock()
	var nPath = this.buildPath(key, "")

	if _, exists := this.handlers[nPath]; exists {
		return fmt.Errorf("handler %s exists", key)
	}
	this.handlers[nPath] = handler
	return nil
}

func (this *etcdScheduler) Add(key, spec, value string) error {
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return fmt.Errorf("empty key string")
	}
	spec = strings.TrimSpace(spec)
	if len(spec) == 0 {
		return fmt.Errorf("empty spec string")
	}
	value = strings.TrimSpace(value)
	if len(value) == 0 {
		return fmt.Errorf("empty value string")
	}

	var job = &internal.Job{}
	job.Key = key
	job.Path = this.buildPath(key, "")
	job.FullPath = this.buildPath(key, value)
	job.Value = value
	job.Spec = spec
	job.Type = 1
	job.Status = internal.JobStatusValid

	return this.add(job)
}

func (this *etcdScheduler) add(job *internal.Job) error {
	schedule, err := this.parser.Parse(job.Spec)
	if err != nil {
		return err
	}
	var nextTime = schedule.Next(time.Now())

	// 更新任务下次执行时间
	job.NextTime = nextTime.In(this.location).Format(time.RFC3339)

	jobBytes, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return this.tryAddJob(job.FullPath, string(jobBytes), nextTime)
}

func (this *etcdScheduler) tryAddJob(key, value string, nextTime time.Time) error {
	grantRsp, err := this.client.Grant(context.Background(), nextTime.Unix()-time.Now().Unix())
	if err != nil {
		return err
	}

	var kv = clientv3.NewKV(this.client)
	_, err = kv.Put(context.Background(), key, value, clientv3.WithLease(grantRsp.ID))
	return err
}

func (this *etcdScheduler) Remove(key, value string) error {
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return fmt.Errorf("empty key string")
	}
	value = strings.TrimSpace(value)
	if len(value) == 0 {
		return fmt.Errorf("empty value string")
	}

	var nPath = this.buildPath(key, value)
	var kv = clientv3.NewKV(this.client)

	// 获取 job
	getRsp, err := kv.Get(context.Background(), nPath)
	if err != nil {
		return err
	}

	if len(getRsp.Kvs) > 0 && getRsp.Kvs[0].Version > 0 {
		var job *internal.Job
		if err := json.Unmarshal(getRsp.Kvs[0].Value, &job); err != nil {
			return err
		}
		// 先将其状态调整为 无效
		job.Status = internal.JobStatusInvalid

		// 更新数据
		jobBytes, _ := json.Marshal(job)
		if _, err := kv.Put(context.Background(), nPath, string(jobBytes)); err != nil {
			return err
		}

		// 然后将其删除
		if _, err := kv.Delete(context.Background(), nPath); err != nil {
			return err
		}
	}
	return nil
}

func (this *etcdScheduler) UpdateNextTime(key, value string, nextTime time.Time) error {
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return fmt.Errorf("empty key string")
	}
	value = strings.TrimSpace(value)
	if len(value) == 0 {
		return fmt.Errorf("empty value string")
	}

	var nPath = this.buildPath(key, value)
	var kv = clientv3.NewKV(this.client)

	// 获取 job
	getRsp, err := kv.Get(context.Background(), nPath)
	if err != nil {
		return err
	}

	if len(getRsp.Kvs) > 0 && getRsp.Kvs[0].Version > 0 {
		var job *internal.Job
		if err := json.Unmarshal(getRsp.Kvs[0].Value, &job); err != nil {
			return err
		}
		// 更新时间
		job.NextTime = nextTime.In(this.location).Format(time.RFC3339)
		jobBytes, err := json.Marshal(job)
		if err != nil {
			return err
		}

		// 更新数据
		grantRsp, err := this.client.Grant(context.Background(), nextTime.Unix()-time.Now().Unix())
		if err != nil {
			return err
		}
		if _, err = kv.Put(context.Background(), nPath, string(jobBytes), clientv3.WithLease(grantRsp.ID)); err != nil {
			return err
		}
	}
	return nil
}

func (this *etcdScheduler) watch() {
	var watcher = clientv3.NewWatcher(this.client)
	var watchChan = watcher.Watch(context.Background(), this.prefix, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithFilterPut())

	go func() {
		for {
			select {
			case v, ok := <-watchChan:
				if ok == false {
					return
				}
				go this.handleEvents(v.Events)
			}
		}
	}()
}

func (this *etcdScheduler) handleEvents(events []*clientv3.Event) {
	for _, event := range events {
		switch event.Type {
		case clientv3.EventTypeDelete:
			var job *internal.Job
			if err := json.Unmarshal(event.PrevKv.Value, &job); err != nil {
				continue
			}
			if job == nil {
				continue
			}
			if job.Status != internal.JobStatusValid {
				continue
			}

			var currentTime = job.NextTime
			// 重新添加任务
			this.add(job)

			go this.handleJob(job, currentTime)
		}
	}
}

func (this *etcdScheduler) tryAcquire(key string) bool {
	var lease = clientv3.NewLease(this.client)
	grantRsp, err := lease.Grant(context.Background(), 30)
	if err != nil {
		return false
	}

	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	put := clientv3.OpPut(key, "", clientv3.WithLease(grantRsp.ID))
	rsp, err := this.client.Txn(context.Background()).If(cmp).Then(put).Commit()
	if err != nil {
		return false
	}
	return rsp.Succeeded
}

func (this *etcdScheduler) handleJob(job *internal.Job, currentTime string) {
	this.mu.Lock()
	var handler = this.handlers[job.Path]
	this.mu.Unlock()

	if handler == nil {
		return
	}

	// 防止重复执行
	var lockPath = path.Join(this.lockPrefix, job.Key, job.Value, currentTime)
	if this.tryAcquire(lockPath) == false {
		return
	}
	handler(job.Key, job.Value)
}
