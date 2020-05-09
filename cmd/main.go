package main

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/smartwalle/ts"
	"time"
)

func main() {
	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
	if err != nil {
		fmt.Println(err)
		return
	}

	var s = ts.NewETCDScheduler("xx", etcdCli)

	s.Handle("user", func(key, value string) {
		fmt.Println("user", value, time.Now())
	})
	s.Handle("user/order", func(key, value string) {
		fmt.Println("order", value, time.Now())
	})

	fmt.Println(s.Add("user", "*/1 * * * *", "1"))
	fmt.Println(s.Add("user", "*/1 *  * * *", "2"))
	fmt.Println(s.Add("user/order", "*/1 * * * *", "1"))

	select {}
}
