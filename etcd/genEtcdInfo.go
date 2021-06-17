package etcd

import (
	"context"
	"dht/config"
	"dht/models"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"time"
)

var endpoints string

func RegisterToEtcd(endpoint string) {
	if endpoint == "" {
		endpoint = "127.0.0.1:2379"
	}
	endpoints = endpoint
	split := strings.Split(endpoints, ":")
	//fmt.Println(split[0])
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect to etcd failed, err:", err)
		return
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	var defaultMysql = `"{"user":"root","password":"111111","address":"39.105.189.17:3306","database":"dht"}`
	sqlCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	key := "mysql_dsn_%s_conf"
	key = fmt.Sprintf(key, split[0])
	_, err = client.Put(sqlCtx, key, defaultMysql)
	if err != nil {
		fmt.Println("put to etcd failed, err:", err)
		return
	}
	cancelFunc()
	var (
		dht_node_key    = fmt.Sprintf("dht_node_%s_conf", split[0])
		mysql_dsn_key   = fmt.Sprintf("mysql_dsn_%s_conf", split[0])
		node_exist_key  = fmt.Sprintf("node_exist_%s_conf", split[0])
		dht_network_key = fmt.Sprintf("dht_network_%s_conf", split[0])
	)
	resp, err := client.Get(ctx, dht_node_key)
	if err != nil {
		logrus.Errorf("etcd: get conf from etcd by key:%s, err:%v.", dht_node_key, err)
		return
	}
	logrus.Infof("The etcd has %s config infomation as: %v", dht_node_key, resp)

	resp, err = client.Get(ctx, mysql_dsn_key)
	if err != nil {
		logrus.Errorf("etcd: get conf from etcd by key:%s, err:%v.", mysql_dsn_key, err)
		return
	}
	logrus.Infof("The etcd has %s config infomation as: %v", mysql_dsn_key, resp)

	resp, err = client.Get(ctx, node_exist_key)
	if err != nil {
		logrus.Errorf("etcd: get conf from etcd by key:%s, err:%v.", node_exist_key, err)
		return
	}
	logrus.Infof("The etcd has %s config infomation as: %v", node_exist_key, resp)

	resp, err = client.Get(ctx, dht_network_key)
	if err != nil {
		logrus.Errorf("etcd: get conf from etcd by key:%s, err:%v.", dht_network_key, err)
		return
	}
	logrus.Infof("The etcd has %s config infomation as: %v", dht_network_key, resp)

	ip, err := config.GetOutbountIp()
	if err != nil {
		logrus.Errorf("get ip failed, err:%v.", err)
		return
	}

	fmt.Printf("Your host ip is %s, we suggest you use it as you node ip.", ip)
	////取值
	//ctx2, cancle2 := context.WithTimeout(context.Background(), time.Second)
	//gR, err2 := client.Get(ctx2, key)
	//if err2 != nil {
	//	fmt.Println("get from etcd failed, err:", err)
	//	return
	//}
	//for _, ev := range gR.Kvs {
	//	fmt.Printf("key:%s , value:%s", ev.Key, ev.Value)
	//}
	//fmt.Println("The etcd has config befor:")
	//fmt.Printf("%v \n", gR)
	fmt.Println("Do you want to use these configs or not (y/n):")
	var flag string
	fmt.Scanln(&flag)
LOOP:
	for {
		if flag == "y" {
			break LOOP
		} else if flag == "n" {
			fmt.Println("please input your config:([isBrigdeNode] [NodeId] [ip:port] ")
			//put命令，传上下文进去,指定一个超时时间
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*60)
			//str := `[{"path":"/Users/wangzhiyuan/logs/s4.log","topic":"web_log"},{"path":"/Users/wangzhiyuan/logs/web.log","topic":"web_log"}]`
			//str := `[{"first":"yes","id":"1","address":"127.0.0.1:8001"}]`
			var isBrigeNode string
			fmt.Scanln(&isBrigeNode)
			var NodeId string
			fmt.Scanln(&NodeId)
			var ipPort string
			fmt.Scanln(&ipPort)
			var str = `[{"first":"` + isBrigeNode + `","id":"` + NodeId + `","address":"` + ipPort + `"}]`
			key := "dht_node_%s_conf"
			outbountIp, err := config.GetOutbountIp()
			if err != nil {
				logrus.Errorf("etcd: get ip failed, err:%v.", err)
				return
			}
			key = fmt.Sprintf(key, outbountIp)
			_, err = client.Put(ctx, key, str)
			if err != nil {
				fmt.Println("put to etcd failed, err:", err)
				return
			}
			logrus.Infof("etcd: create node config success, key:%s, value:%s", key, str)
			cancelFunc()
			break LOOP
		}
	}
}

func RegisterNodeToEtcd(n *models.Node) (err error) {
	if endpoints == "" {
		endpoints = "127.0.0.1:2379"
	}
	//fmt.Println(split[0])
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect to etcd failed, err:", err)
		return
	}
	defer client.Close()
	marshal, err := json.Marshal(n)
	if err != nil {
		logrus.Errorf("etcd: marshal %v err:%v.", n, err)
	}
	nodeCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	key := "node_exist_conf"
	_, err = client.Put(nodeCtx, key, n.Addr)
	_, err = client.Put(nodeCtx, n.Addr, string(marshal))
	if err != nil {
		fmt.Println("put to etcd failed, err:", err)
		return
	}
	logrus.Infof("Register bridge node:%v to etcd success!", n)
	return
}

func DeleteNodeFromEtcd(n *models.Node) (err error) {
	if endpoints == "" {
		endpoints = "127.0.0.1:2379"
	}
	//fmt.Println(split[0])
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoints},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("connect to etcd failed, err:", err)
		return
	}
	defer client.Close()
	nodeCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	//key := "node_exist_conf"
	//_, err = client.Delete(nodeCtx, key)
	_, err = client.Delete(nodeCtx, n.Addr)
	if err != nil {
		fmt.Println("delete from etcd failed, err:", err)
		return
	}
	logrus.Infof("Delete bridge node:%v from etcd success!", n)
	return
}

func main2() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Println("link to etcd error :", err)
		return
	}
	defer cli.Close()

	//watch
	watchCh := cli.Watch(context.Background(), "s4")
	//检测到key的值变化，或者key删除了,通过通道传递消息
	for wresp := range watchCh {
		//通道没有值的话就阻塞了
		for _, evt := range wresp.Events {
			fmt.Printf("type:%s key:%s value:%s.\n", evt.Type, evt.Kv.Key, evt.Kv.Value)
		}
	}
}
