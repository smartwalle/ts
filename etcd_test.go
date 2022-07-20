package ts_test

import (
	"fmt"
	"github.com/smartwalle/ts"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestNewScheduler(t *testing.T) {
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{"192.168.1.77:2379"}})
	if err != nil {
		fmt.Println(err)
		return
	}

	var s = ts.NewScheduler("xx", etcdCli)

	s.Handle("user", func(key, value string) error {
		fmt.Println("user", value, time.Now())
		return nil
	})
	s.Handle("user/order", func(key, value string) error {
		fmt.Println("order", value, time.Now())
		return nil
	})

	fmt.Println(s.Add("user", "*/1 * * * *", "1"))
	fmt.Println(s.Add("user", "*/1 * * * *", "2"))
	fmt.Println(s.Add("user/order", "*/1 * * * *", "1"))
	fmt.Println(s.AddOnce("user/order", "*/1 * * * *", "2"))

	select {}
}
