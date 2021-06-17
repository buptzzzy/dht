package mysql

import (
	"database/sql"
	"github.com/sirupsen/logrus"

	//只要导入了包，必定执行init方法，加 _ 表示只用Init()方法
	_ "github.com/go-sql-driver/mysql"
)

// Init 驱动的init()方法
/*
	func init() {
		sql.Register("mysql", &MySQLDriver{})
	}
	往database/sql里面注册了一个名为 mysql的数据库
*/
func Init(dsn string) {
	//dsn := "user:password@tcp(ip:port)/databasename"
	//dsn := "root:111@tcp(127.0.0.1:3306)/go_test"
	db, err := sql.Open("mysql", dsn)
	//前提是注册对应数据库的驱动
	if err != nil {
		logrus.Errorf("mysql: open mysql failed, err:%v.", err)
		return
	}
	//尝试连接数据库, 校验用户名密码是否正确
	err = db.Ping()
	if err != nil {
		logrus.Errorf("mysql: connect mysql failed, err:%v.", err)
		return
	}
	logrus.Infof("mysql: connect Mysql success!")
	rows, err := db.Query("select count(*) from tableno")
	if err != nil {
		logrus.Errorf("%v",err)
		return
	}
	logrus.Infof("%v", rows)
	//循环读取数据
	var count int
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			logrus.Errorf("identity: scan from rows failed, err:%v!", err)
			return
		}
		logrus.Infof("identity:%#v.", count)
	}
}
//使用连接池比较方便节省时间
//定义一个全局对象db, 使用连接池连接Mysql
func InitDB(dsn string) (db *sql.DB, err error) {
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		logrus.Errorf("mysql: open mysql failed, err:%v.", err)
		return
	}
	err = db.Ping()
	if err != nil {
		logrus.Errorf("mysql: connect mysql failed, err:%v.", err)
		return
	}
	//连接上数据库了
	logrus.Infof("mysql: connect mysql success!")
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(20)
	return
}
