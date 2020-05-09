package ts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/smartwalle/ts/internal"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	kETCDJob  = kPrefix + "job"
	kETCDLock = kPrefix + "lock"
)

var (
	ErrEmptyKey   = errors.New("empty key string")
	ErrEmptySpec  = errors.New("empty spec string")
	ErrEmptyValue = errors.New("empty value string")
	ErrNilHandler = errors.New("nil handler")
)

type etcdScheduler struct {
	jobPrefix  string
	lockPrefix string
	client     *clientv3.Client
	parser     internal.Parser
	location   *time.Location

	mu       *sync.Mutex
	handlers map[string]Handler
}

func NewETCDScheduler(prefix string, client *clientv3.Client) Scheduler {
	var s = &etcdScheduler{}
	s.jobPrefix = path.Join(kETCDJob, "/", prefix)
	s.lockPrefix = path.Join(kETCDLock, "/", prefix)
	s.client = client
	s.parser = internal.NewParser(internal.Minute | internal.Hour | internal.Dom | internal.Month | internal.Dow | internal.Descriptor)
	s.location = time.UTC
	s.mu = &sync.Mutex{}
	s.handlers = make(map[string]Handler)
	s.watch()
	logger.Printf("初始化 ETCDScheduler 成功，Job Prefix: [%s]，Lock Prefix: [%s] \n", s.jobPrefix, s.lockPrefix)
	return s
}

func (this *etcdScheduler) buildPath(key, value string) string {
	return path.Join(this.jobPrefix, key, value)
}

func (this *etcdScheduler) Handle(key string, handler Handler) error {
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if handler == nil {
		return ErrNilHandler
	}

	this.mu.Lock()
	defer this.mu.Unlock()
	var nPath = this.buildPath(key, "")

	if _, exists := this.handlers[nPath]; exists {
		return fmt.Errorf("handler %s exists", key)
	}
	this.handlers[nPath] = handler

	logger.Printf("添加任务 [%s] 的回调函数成功\n", key)

	return nil
}

func (this *etcdScheduler) Add(key, spec, value string) error {
	return this.addJob(key, spec, value, internal.JobTypeLoop)
}

func (this *etcdScheduler) AddOnce(key, spec, value string) error {
	return this.addJob(key, spec, value, internal.JobTypeOnce)
}

func (this *etcdScheduler) addJob(key, spec, value string, jobType internal.JobType) error {
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return ErrEmptyKey
	}
	spec = strings.TrimSpace(spec)
	if len(spec) == 0 {
		return ErrEmptySpec
	}
	value = strings.TrimSpace(value)
	if len(value) == 0 {
		return ErrEmptyValue
	}

	var job = &internal.Job{}
	job.Key = key
	job.Path = this.buildPath(key, "")
	job.FullPath = this.buildPath(key, value)
	job.Value = value
	job.Spec = spec
	job.Type = jobType
	job.Status = internal.JobStatusValid

	return this.run(job)
}

func (this *etcdScheduler) run(job *internal.Job) error {
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

	if err = this.tryRunJob(job.FullPath, string(jobBytes), nextTime); err != nil {
		return err
	}
	logger.Printf("激活任务 [%s]-[%s] 成功，类型 [%s]，完整路径 [%s]，下次执行时间为 [%s]\n", job.Key, job.Value, job.Type, job.FullPath, job.NextTime)
	return nil
}

func (this *etcdScheduler) tryRunJob(key, value string, nextTime time.Time) error {
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
		return ErrEmptyKey
	}
	value = strings.TrimSpace(value)
	if len(value) == 0 {
		return ErrEmptyValue
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
		logger.Printf("删除任务 [%s]-[%s] 成功 \n", key, value)
	}
	return nil
}

func (this *etcdScheduler) UpdateNextTime(key, value string, nextTime time.Time) error {
	key = strings.TrimSpace(key)
	if len(key) == 0 {
		return ErrEmptyKey
	}
	value = strings.TrimSpace(value)
	if len(value) == 0 {
		return ErrEmptyValue
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

		logger.Printf("更新任务 [%s] 的下次执行时间为 [%s] 成功 \n", key, job.NextTime)
	}
	return nil
}

func (this *etcdScheduler) watch() {
	var watcher = clientv3.NewWatcher(this.client)
	var watchChan = watcher.Watch(context.Background(), this.jobPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithFilterPut())

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

			var jobTime = job.NextTime

			// 重新添加任务
			if job.Type == internal.JobTypeLoop {
				this.run(job)
			}

			go this.handleJob(job, jobTime)
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

func (this *etcdScheduler) handleJob(job *internal.Job, jobTime string) {
	this.mu.Lock()
	var handler = this.handlers[job.Path]
	this.mu.Unlock()

	if handler == nil {
		logger.Printf("任务 [%s]-[%s] 回调函数为空，执行失败 \n", job.Key, job.Value)
		return
	}

	// 防止重复执行
	var lockPath = path.Join(this.lockPrefix, job.Key, job.Value, jobTime)
	if this.tryAcquire(lockPath) == false {
		logger.Printf("任务 [%s]-[%s] 抢锁 [%s] 失败，执行失败 \n", job.Key, job.Value, lockPath)
		return
	}

	var err = handler(job.Key, job.Value)
	if err != nil {
		logger.Printf("执行任务 [%s]-[%s] 完成，有返回错误信息: [%v] \n", job.Key, job.Value, err)
	} else {
		logger.Printf("执行任务 [%s]-[%s] 完成\n", job.Key, job.Value)
	}
}
