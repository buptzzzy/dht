package chord

import (
	"database/sql"
	"dht/config"
	"dht/models"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/big"
	"time"
)

func Add(id string, addr string, sister *models.Node, db *sql.DB)  (node *Node,err error){

	cfg := config.DefaultNodeConfig()
	cfg.Id = id
	cfg.Addr = addr
	cfg.Timeout = 100 * time.Millisecond
	cfg.MaxIdle = 1 * time.Second

	node, err = NewNode(cfg, sister)
	if err != nil {
		logrus.Errorf("node %d join failed, err:%v.", cfg.Id, err)
		return
	}


	bytes := (&big.Int{}).SetBytes(node.Id)
	tableId := bytes.String()[:5]
	node.nodeTable = "node" + tableId
	node.createTable = "create table %s ( `id` BIGINT(32) NOT NULL AUTO_INCREMENT, `identity` VARCHAR(30) DEFAULT '', `mappingData` varChar(100) DEFAULT '0', PRIMARY KEY(`id`))ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4"
	node.isCreate = "select count(*) from %s"
	node.insertMeta = "insert into %s (id,identity,mappingData) values(0, 'metaIdentity', 'Do not delete this record')"
	node.insert = "insert into %s (identity,mappingData) values(?,?)"
	node.resolve = "select id,identity,mappingData from %s where identity=?"
	node.dropId = "delete from %s where identity=?"
	node.updateId = "update %s SET mappingData=? where identity=?"
	node.DropTable = "drop table %s"

	_, err = db.Query(node.isCreate)
	if err != nil {
		node.createTable = fmt.Sprintf(node.createTable, node.nodeTable)
		node.insertMeta = fmt.Sprintf(node.insertMeta, node.nodeTable)
		//table := "node" + node.Id[0:8]
		_, err = db.Exec(node.createTable)
		if err != nil {
			logrus.Errorf("chord: create table %s failed, err:%v.", node.nodeTable, err)
			return
		}
		logrus.Infof("chord: table %s is created success!", node.nodeTable)
		_, err = db.Exec(node.insertMeta)
		if err != nil {
			logrus.Errorf("chord: table %s init failed, err: %v.", node.nodeTable, err)
		}
		logrus.Infof("chord: table %s init success!", node.nodeTable)
	}else {
		logrus.Warnf("table %s has existed!", node.nodeTable)
	}
	node.insert = fmt.Sprintf(node.insert, node.nodeTable)
	node.resolve = fmt.Sprintf(node.resolve, node.nodeTable)
	node.dropId = fmt.Sprintf(node.dropId, node.nodeTable)
	node.updateId = fmt.Sprintf(node.updateId, node.nodeTable)
	logrus.Infof("chord: table sql %s init success!", node.updateId)
	logrus.Infof("chord: table sql %s init success!", node.insert)
	return node, err
}