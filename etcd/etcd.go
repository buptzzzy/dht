package etcd

import (
	"context"
	"dht/config"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"time"
)

var client *clientv3.Client

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 180,
	})
	if err != nil {
		logrus.Errorf("etcd: init etcd failed, err: %v", err)
		return
	}
	return
}


//拉取日志收集配置项
func GetConf(key string) (confList []config.NodeEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	defer cancel()
	//fmt.Println("key is !!!!!!!!!!!!!!!!!", key)
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("etcd: get conf from etcd by key:%s, err:%v.", key, err)
		return
	}
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 conf from etcd by key:%s.", key)
		return
	}
	ret := resp.Kvs[0]
	//ret.value是json格式的字符串
	err = json.Unmarshal(ret.Value, &confList)
	if err != nil {
		logrus.Errorf("etcd: unmarshal Value:%v failed, err:%v.", ret.Value, err)
		return
	}
	return
}
