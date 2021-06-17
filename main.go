package main

import (
	"database/sql"
	"dht/chord"
	"dht/config"
	"dht/etcd"
	"dht/models"
	"dht/mysql"
	"dht/server"
	"fmt"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

type Config struct {
	EtcdConfig  `ini:"etcd"`
	MysqlConfig `ini:"mysql"`
}

type EtcdConfig struct {
	Address     string `ini:"address"`
	DhtNodeKey  string `ini:"dht_node_key"`
	MysqlDsnKey string `ini:"mysql_dsn_key"`
}

type MysqlConfig struct {
	User     string `ini:"user"`
	Password string `ini:"password"`
	Address  string `ini:"address"`
	Database string `ini:"database"`
}

func getDsn(user, password, address, database string) string {
	//"root:111@tcp(127.0.0.1:3306)/go_test"
	return user + ":" + password + "@tcp(" + address + ")/" + database
}

var (
	//所有的config.ini里面的信息
	configObj *Config
	//边界节点/领导节点
	brigeNode *models.Node
	//连接池
	db *sql.DB
)

func main() {
	//-2
	fmt.Printf("Please input the etcd config address, example(127.0.0.1:2379):")
	var endpoints string
	fmt.Scanln(&endpoints)
	etcd.RegisterToEtcd(endpoints)
	//-1:获取本机IP，为后续去ETCD取配置文件作准本
	ip, err := config.GetOutbountIp()
	if err != nil {
		logrus.Errorf("get ip failed, err:%v.", err)
		return
	}

	//拿到ip替换配置文件里面的%s
	//0.初始化
	//因为下面要用反射改变值，所以要指针
	configObj = new(Config)
	cfg, err := ini.Load("./config/config.ini")
	if err != nil {
		logrus.Errorf("open init config error, %v", err)
		return
	}
	logrus.Infof("open init config success!")
	logrus.Infof("%v", cfg)
	err = ini.MapTo(configObj, "./config/config.ini")
	//初始化Etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("Init etcd failed, err:%v.", err)
		return
	}
	logrus.Infof("Init etcd success.")
	//获取所有的配置项， key 应该是： "collect_log_conf", 也就是ini中的value
	dhtNodeKey := fmt.Sprintf(configObj.EtcdConfig.DhtNodeKey, ip)
	//fmt.Printf("%v:%s:%s", collectKey,configObj.EtcdConfig.DhtNodeKey,ip)

	allConf, err := etcd.GetConf(dhtNodeKey)
	if err != nil {
		logrus.Errorf("get conf from etcd err:%v.", err)
		return
	}
	logrus.Infof("get conf from etcd success, allConf:%v.", allConf)
	//id := CreateID("1")
	//fmt.Println(reflect.TypeOf(allConf))
	//连接数据库
	dsn := getDsn(configObj.User, configObj.Password, configObj.MysqlConfig.Address, configObj.Database)
	fmt.Println(dsn)
	db, err = mysql.InitDB(dsn)
	var node *chord.Node
	if allConf[0].First == "yes" || allConf[0].First == "y" || allConf[0].First == "1" {
		brigeNode = chord.NewInode(allConf[0].Id, allConf[0].Address)
		//把节点注册到etcd
		etcd.RegisterNodeToEtcd(brigeNode)
		//退出的时候删除
		defer etcd.DeleteNodeFromEtcd(brigeNode)
		node, err = chord.Init(allConf[0].Id, allConf[0].Address, db)
		if err != nil {
			logrus.Errorf("build chord failed, err:%v.", err)
			return
		}

	} else {
		node, err = chord.Add(allConf[0].Id, allConf[0].Address, brigeNode, db)
		//把
		if err != nil {
			logrus.Errorf("join chord failed, err:%v.", err)
			return
		}
	}
	logrus.Infof("%v", node)

	defer func() {
		node.DropTable = fmt.Sprintf(node.DropTable, node.GetNodeTable())
		_, err = db.Exec(node.DropTable)
		if err != nil {
			logrus.Errorf("chord: exit but drop table failed, err:%v!", err)
			return
		}
		logrus.Infof("chord: exit and drop table %s!", node.GetNodeTable())
	}()
	var port string
	fmt.Println("please input which port you want to watch:")
	fmt.Scanln(&port)
	server.Run(node, db, port)
}
