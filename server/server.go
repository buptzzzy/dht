package server

import (
	"database/sql"
	"dht/chord"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
)

var nodeGlobal *chord.Node
var dbGlobal *sql.DB

func Run(node *chord.Node, db *sql.DB, port string) {
	idResp = new(idResponse)
	nodeGlobal = node
	dbGlobal = db
	http.HandleFunc("dht/register", IdentityRegisterHandler)
	http.HandleFunc("dht/resolve", IdentityResolveHandler)
	http.HandleFunc("dht/update", IdentityUpdateHandler)
	http.HandleFunc("dht/delete", IdentityDeleteHandler)
	listenAddress := "localhost:" + port
	for {
		http.ListenAndServe( listenAddress, nil)
	}
}

type message struct {
	Identity string `json:"Identity"`
	Type int `json:"type"`
	MappingData string `json:"mappingData"`
}

type idResponse struct {
	message
	Status int `json:"status"`
	Info string `json:"info"`
}

var  idResp	*idResponse

func IdentityRegisterHandler(w http.ResponseWriter, r *http.Request){
	reqBody := new(message)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("get request body failed, err:%v.", err)
		return
	}
	err = json.Unmarshal(body, reqBody)
	if err != nil {
		logrus.Errorf("Unmarshal data failed, err:%v.", err)
		return
	}
	logrus.Infof("request get %v.", reqBody)
	info, err := nodeGlobal.Set(reqBody.Identity, reqBody.MappingData, dbGlobal)
	idResp.Info = info
	if err != nil {
		logrus.Errorf("server: insertNode failed, err:%v.", err)
		idResp.Status = 0
		return
	}
	idResp.Status = 1
	mes := message{
		Identity: reqBody.Identity,
		MappingData: reqBody.MappingData,
		Type: reqBody.Type,
	}
	idResp.message = mes
	logrus.Infof("server: insert identity:%s, mappingData %s success!", reqBody.Identity, reqBody.MappingData)
	bytes, err := json.Marshal(idResp)
	if err != nil {
		logrus.Errorf("marshal failed, err:%v.", err)
		return
	}
	io.WriteString(w, string(bytes))
}


func IdentityResolveHandler(w http.ResponseWriter, r *http.Request){
	reqBody := new(message)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("get request body failed, err:%v.", err)
		return
	}
	err = json.Unmarshal(body, reqBody)
	if err != nil {
		logrus.Errorf("Unmarshal data failed, err:%v.", err)
		return
	}
	logrus.Infof("request get %v.", reqBody)
	mappings, err := nodeGlobal.Get(reqBody.Identity, dbGlobal)
	var info string
	if err != nil {
		logrus.Errorf("server: resolve identity:%s failed, err:%v.", reqBody.Identity, err)
		info = fmt.Sprintf("server: resolve identity:%s failed, err:%v.", reqBody.Identity, err)
		idResp.Info = info
		idResp.Status = 0
		return
	}
	idResp.Status = 1
	mes := message{
		Identity: reqBody.Identity,
		MappingData: string(mappings),
		Type: reqBody.Type,
	}
	idResp.message = mes
	info = fmt.Sprintf("server: resolve identity:%s, mappingData %s success!", reqBody.Identity, string(mappings))
	idResp.Info = info
	logrus.Infof("server: resolve identity:%s, mappingData %s success!", reqBody.Identity, string(mappings))
	bytes, err := json.Marshal(idResp)
	if err != nil {
		logrus.Errorf("marshal failed, err:%v.", err)
		return
	}
	io.WriteString(w, string(bytes))
}


func IdentityUpdateHandler(w http.ResponseWriter, r *http.Request){
	reqBody := new(message)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("get request body failed, err:%v.", err)
		return
	}
	err = json.Unmarshal(body, reqBody)
	if err != nil {
		logrus.Errorf("Unmarshal data failed, err:%v.", err)
		return
	}
	logrus.Infof("request get %v.", reqBody)
	info, err := nodeGlobal.Update(reqBody.Identity, reqBody.MappingData, dbGlobal)
	idResp.Info = info
	if err != nil {
		logrus.Errorf("server: update idengtity %s failed, err:%v.", reqBody.Identity, err)
		idResp.Status = 0
		return
	}
	idResp.Status = 1
	mes := message{
		Identity: reqBody.Identity,
		MappingData: reqBody.MappingData,
		Type: reqBody.Type,
	}
	idResp.message = mes
	logrus.Infof("server: update identity:%s, mappingData %s success!", reqBody.Identity, reqBody.MappingData)
	bytes, err := json.Marshal(idResp)
	if err != nil {
		logrus.Errorf("marshal failed, err:%v.", err)
		return
	}
	io.WriteString(w, string(bytes))
}

func IdentityDeleteHandler(w http.ResponseWriter, r *http.Request){
	reqBody := new(message)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("get request body failed, err:%v.", err)
		return
	}
	err = json.Unmarshal(body, reqBody)
	if err != nil {
		logrus.Errorf("Unmarshal data failed, err:%v.", err)
		return
	}
	logrus.Infof("request get %v.", reqBody)
	info, err := nodeGlobal.Delete(reqBody.Identity, dbGlobal)
	idResp.Info = info
	if err != nil {
		logrus.Errorf("server: delete failed, err:%v.", err)
		idResp.Status = 0
		return
	}
	idResp.Status = 1
	mes := message{
		Identity: reqBody.Identity,
		Type: reqBody.Type,
	}
	idResp.message = mes
	logrus.Infof("server: delete identity:%s success!", reqBody.Identity)
	bytes, err := json.Marshal(idResp)
	if err != nil {
		logrus.Errorf("marshal failed, err:%v.", err)
		return
	}
	io.WriteString(w, string(bytes))
}