package chord

import (
	"crypto/sha1"
	"database/sql"
	"dht/config"
	"dht/identity"
	"dht/models"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"math/big"
	"sync"
	"time"
)

type Node struct {
	*models.Node

	nodeTable   string
	createTable string
	isCreate    string
	insertMeta  string
	DropTable   string
	insert      string
	resolve     string
	dropId      string
	updateId    string

	cnf *config.NodeConfig

	predecessor *models.Node
	predMtx     sync.RWMutex

	successor *models.Node
	succMtx   sync.RWMutex

	shutdownCh chan struct{}

	fingerTable fingerTable
	ftMtx       sync.RWMutex

	storage Storage
	stMtx   sync.RWMutex

	transport Transport
	tsMtx     sync.RWMutex

	lastStablized time.Time
}

func (n *Node) GetNodeTable() string {
	return n.nodeTable
}
func NewInode(id string, addr string) *models.Node {
	h := sha1.New()
	if _, err := h.Write([]byte(id)); err != nil {
		return nil
	}
	val := h.Sum(nil)

	return &models.Node{
		Id:   val,
		Addr: addr,
	}
}

/*
	NewNode creates a new Chord node. Returns error if node already
	exists in the chord ring
*/
func NewNode(cnf *config.NodeConfig, joinNode *models.Node) (*Node, error) {
	if err := cnf.Validate(); err != nil {
		return nil, err
	}
	node := &Node{
		Node:       new(models.Node),
		shutdownCh: make(chan struct{}),
		cnf:        cnf,
		storage:    NewMapStore(cnf.Hash),
	}

	var nID string
	if cnf.Id != "" {
		nID = cnf.Id
	} else {
		nID = cnf.Addr
	}
	id, err := node.hashKey(nID)
	if err != nil {
		return nil, err
	}
	aInt := (&big.Int{}).SetBytes(id)

	fmt.Printf("new node id %d, \n", aInt)

	node.Node.Id = id
	node.Node.Addr = cnf.Addr

	// Populate finger table
	node.fingerTable = newFingerTable(node.Node, cnf.HashSize)

	// Start RPC server
	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	models.RegisterChordServer(transport.server, node)

	node.transport.Start()

	if err := node.join(joinNode); err != nil {
		return nil, err
	}

	// Peridoically stabilize the node.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.stabilize()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Peridoically fix finger tables.
	go func() {
		next := 0
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				next = node.fixFinger(next)
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// Peridoically checkes whether predecessor has failed.

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.checkPredecessor()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	return node, nil
}

func (n *Node) InsertIdentity(sqlStr string, db *sql.DB, id, mappingdata string) {
	res, err := db.Exec(sqlStr, id, mappingdata)
	if err != nil {
		logrus.Errorf("insert identity failed, err:%v.", err)
		return
	}
	//拿到刚刚插入的数据的Id值，不同的数据库有不同的实现
	theID, err := res.LastInsertId()
	if err != nil {
		logrus.Errorf("get lastInsertId failed, err:%v.", err)
		return
	}
	logrus.Infof("the id is:%v.", theID)
}

func (n *Node) ResolveIdentity(sqlStr string, db *sql.DB, id string) (bytes []byte, err error) {
	var idInstance identity.Identity
	rows, err := db.Query(sqlStr, id)
	if err != nil {
		logrus.Errorf("identity: query failed, err:%v.", err)
		return
	}
	defer rows.Close()
	//循环读取数据
	for rows.Next() {
		err = rows.Scan(&idInstance.Id, &idInstance.Identity, &idInstance.MappingData)
		if err != nil {
			logrus.Errorf("identity: scan from rows failed, err:%v!", err)
			return
		}
		logrus.Infof("identity:%#v.", idInstance)
	}
	bytes, err = json.Marshal(idInstance)
	if err != nil {
		logrus.Errorf("chord: marshal %v failed, err:%v!", idInstance, err)
		return nil, err
	}
	return bytes, nil
}

func (n *Node) UpdateIdentity(sqlStr string, db *sql.DB, id, mappingData string) (affected int64) {
	res, err := db.Exec(sqlStr, mappingData, id)
	if err != nil {
		logrus.Errorf("update failed, err:%v.", err)
		return
	}
	//拿到受影响的行数
	affected, err = res.RowsAffected()
	if err != nil {
		logrus.Errorf("get affected rows failed, err:%v.", err)
		return
	}
	logrus.Infof("Update success, affected rows nums is:%v.", affected)
	return
}

func (n *Node) DeleteIdentity(sqlStr string, db *sql.DB, id string) (affected int64) {
	res, err := db.Exec(sqlStr, id)
	if err != nil {
		logrus.Errorf("delete failed, err:%v.", err)
		return
	}
	//拿到受影响的行数
	affected, err = res.RowsAffected()
	if err != nil {
		logrus.Errorf("get affected rows failed, err:%v.", err)
		return
	}
	logrus.Infof("Delete success, affected rows nums is:%v.", affected)
	return
}

func (n *Node) hashKey(key string) ([]byte, error) {
	h := n.cnf.Hash()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	val := h.Sum(nil)
	return val, nil
}

func (n *Node) join(joinNode *models.Node) error {
	// First check if node already present in the circle
	// Join this node to the same chord ring as parent
	var foo *models.Node
	// // Ask if our id already exists on the ring.
	if joinNode != nil {
		remoteNode, err := n.findSuccessorRPC(joinNode, n.Id)
		if err != nil {
			return err
		}

		if isEqual(remoteNode.Id, n.Id) {
			return ERR_NODE_EXISTS
		}
		foo = joinNode
	} else {
		foo = n.Node
	}

	succ, err := n.findSuccessorRPC(foo, n.Id)
	if err != nil {
		return err
	}
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()

	return nil
}

/*
	Public storage implementation
*/

func (n *Node) Find(key string) (*models.Node, error) {
	return n.locate(key)
}

func (n *Node) Get(key string, db *sql.DB) ([]byte, error) {
	return n.get(key, db)
}
func (n *Node) Set(key, value string, db *sql.DB) (string, error) {
	return n.set(key, value, db)
}

func (n *Node) Update(key, value string, db *sql.DB) (string, error) {
	return n.update(key, value, db)
}

func (n *Node) Delete(key string, db *sql.DB) (string, error) {
	return n.delete(key, db)
}

/*
	Finds the node for the key
*/
func (n *Node) locate(key string) (*models.Node, error) {
	id, err := n.hashKey(key)
	if err != nil {
		return nil, err
	}
	succ, err := n.findSuccessor(id)
	return succ, err
}

func (n *Node) get(key string, db *sql.DB) (val []byte, err error) {
	node, err := n.locate(key)
	bytes := (&big.Int{}).SetBytes(node.Id)
	tableId := bytes.String()[:5]
	Table := "node" + tableId
	var sqlStr string
	if n.nodeTable != Table {
		sqlStr = fmt.Sprintf("select id,identity,mappingData from %s where identity=?", Table)
	}
	sqlStr = n.resolve
	if err != nil {
		return nil, err
	}
	value, err := n.getKeyRPC(node, key)
	if value == nil {
		val, err = func() ([]byte, error) {
			val, err = n.ResolveIdentity(sqlStr, db, key)
			if val != nil {
				logrus.Infof("chord: find identity %s in mysql!", key)
				return val, nil
			}
			return nil, err
		}()
	}
	if val != nil {
		return val, nil
	}
	if err != nil {
		logrus.Warnf("Look up identity:%s failed!", key)
		return nil, err
	}
	logrus.Infof("chord: find identity %s in memory!", key)
	val = value.Value
	return val, nil
}

func (n *Node) set(key, value string, db *sql.DB) (info string, err error) {
	node, err := n.locate(key)
	if err != nil {
		return
	}
	bytes := (&big.Int{}).SetBytes(node.Id)
	tableId := bytes.String()[:5]
	Table := "node" + tableId
	fmt.Println(Table)
	var (
		sqlResolve string
		sqlInsert  string
	)

	if n.nodeTable != Table {
		sqlInsert = fmt.Sprintf("insert into %s (identity,mappingData) values(?,?)", Table)
		sqlResolve = fmt.Sprintf("select id,identity,mappingData from %s where identity=?", Table)
	}
	sqlInsert = n.insert
	sqlResolve = n.resolve
	//fmt.Println("111111111111:",sqlResolve, "222222:", sqlInsert)
	err = n.setKeyRPC(node, key, value)
	get, _ := n.ResolveIdentity(sqlResolve, db, key)
	if get == nil {
		n.InsertIdentity(sqlInsert, db, key, value)
		info = fmt.Sprintf("Identity %s : mappingData: %s insert success", key, value)
	} else {
		logrus.Warnf("Identity %s has already existed!", key)
		info = fmt.Sprintf("Identity %s has already existed!", key)
	}
	return
}

func (n *Node) update(key, value string, db *sql.DB) (info string, err error) {
	node, err := n.locate(key)
	if err != nil {
		info = fmt.Sprintf("update Identity %s : mappingData: %s err", key, value)
		return
	}
	bytes := (&big.Int{}).SetBytes(node.Id)
	tableId := bytes.String()[:5]
	Table := "node" + tableId
	fmt.Println(Table)
	var (
		sqlUpdate  string
	)

	if n.nodeTable != Table {
		sqlUpdate = fmt.Sprintf("update %s SET mappingData=? where identity=?", Table)
	}
	sqlUpdate  = n.updateId
	logrus.Infof("chord: table sql %s init success!", sqlUpdate)
	//fmt.Println("111111111111:",sqlResolve, "222222:", sqlInsert)
	err = n.setKeyRPC(node, key, value)
	if err != nil {
		info = fmt.Sprintf("update Identity %s : mappingData: %s err", key, value)
		return
	}
	n.UpdateIdentity(sqlUpdate, db, key, value)
	info = fmt.Sprintf("Identity %s : mappingData: %s update success", key, value)
	return
}

func (n *Node) delete(key string, db *sql.DB) (info string, err error) {
	node, err := n.locate(key)
	if err != nil {
		return
	}
	bytes := (&big.Int{}).SetBytes(node.Id)
	tableId := bytes.String()[:5]
	Table := "node" + tableId
	fmt.Println(Table)
	var (
		sqlResolve string
		sqlDelete  string
	)
	if n.nodeTable != Table {
		sqlDelete = fmt.Sprintf("delete from %s where identity=?", Table)
		sqlResolve = fmt.Sprintf("select id,identity,mappingData from %s where identity=?", Table)
	}
	sqlDelete = n.dropId
	sqlResolve = n.resolve
	//fmt.Println("111111111111:",sqlResolve, "222222:", sqlInsert)
	//err = n.setKeyRPC(node, key, value)
	get, _ := n.ResolveIdentity(sqlResolve, db, key)
	if get != nil {
		n.DeleteIdentity(sqlDelete, db, key)
		info = fmt.Sprintf("Identity %s delete success", key)
	} else {
		logrus.Warnf("Identity %s doesn't exist!", key)
		info = fmt.Sprintf("Identity %s doesn't exist!", key)
	}
	err = n.deleteKeyRPC(node, key)
	if err != nil {
		info = fmt.Sprintf("delete identity %s err %v", key, err)
		return
	}
	return
}

func (n *Node) transferKeys(pred, succ *models.Node) {

	keys, err := n.requestKeys(pred, succ)
	if len(keys) > 0 {
		fmt.Println("transfering: ", keys, err)
	}
	delKeyList := make([]string, 0, 10)
	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		n.storage.Set(item.Key, item.Value)
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from the successor node, as current node
	// is responsible for the keys
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}

}

func (n *Node) moveKeysFromLocal(pred, succ *models.Node) {

	keys, err := n.storage.Between(pred.Id, succ.Id)
	if len(keys) > 0 {
		fmt.Println("transfering: ", keys, succ, err)
	}
	delKeyList := make([]string, 0, 10)
	// store the keys in current node
	for _, item := range keys {
		if item == nil {
			continue
		}
		err := n.setKeyRPC(succ, item.Key, item.Value)
		if err != nil {
			fmt.Println("error transfering key: ", item.Key, succ.Addr)
		}
		delKeyList = append(delKeyList, item.Key)
	}
	// delete the keys from the successor node, as current node
	// is responsible for the keys
	if len(delKeyList) > 0 {
		n.deleteKeys(succ, delKeyList)
	}

}

func (n *Node) deleteKeys(node *models.Node, keys []string) error {
	return n.deleteKeysRPC(node, keys)
}

// When a new node joins, it requests keys from it's successor
func (n *Node) requestKeys(pred, succ *models.Node) ([]*models.KV, error) {

	if isEqual(n.Id, succ.Id) {
		return nil, nil
	}
	return n.requestKeysRPC(
		succ, pred.Id, n.Id,
	)
}

/*
	Fig 5 implementation for find_succesor
	First check if key present in local table, if not
	then look for how to travel in the ring
*/
func (n *Node) findSuccessor(id []byte) (*models.Node, error) {
	// Check if lock is needed throughout the process
	n.succMtx.RLock()
	defer n.succMtx.RUnlock()
	curr := n.Node
	succ := n.successor

	if succ == nil {
		return curr, nil
	}

	var err error

	if betweenRightIncl(id, curr.Id, succ.Id) {
		return succ, nil
	} else {
		pred := n.closestPrecedingNode(id)
		/*
			NOT SURE ABOUT THIS, RECHECK from paper!!!
			if preceeding node and current node are the same,
			store the key on this node
		*/

		if isEqual(pred.Id, n.Id) {
			succ, err = n.getSuccessorRPC(pred)
			if err != nil {
				return nil, err
			}
			if succ == nil {
				// not able to wrap around, current node is the successor
				return pred, nil
			}
			return succ, nil
		}

		succ, err := n.findSuccessorRPC(pred, id)
		// fmt.Println("successor to closest node ", succ, err)
		if err != nil {
			return nil, err
		}
		if succ == nil {
			// not able to wrap around, current node is the successor
			return curr, nil
		}
		return succ, nil

	}
	return nil, nil
}

// Fig 5 implementation for closest_preceding_node
func (n *Node) closestPrecedingNode(id []byte) *models.Node {
	n.predMtx.RLock()
	defer n.predMtx.RUnlock()

	curr := n.Node

	m := len(n.fingerTable) - 1
	for i := m; i >= 0; i-- {
		f := n.fingerTable[i]
		if f == nil || f.Node == nil {
			continue
		}
		if between(f.Id, curr.Id, id) {
			return f.Node
		}
	}
	return curr
}

/*
	Periodic functions implementation
*/

func (n *Node) stabilize() {

	n.succMtx.RLock()
	succ := n.successor
	if succ == nil {
		n.succMtx.RUnlock()
		return
	}
	n.succMtx.RUnlock()

	x, err := n.getPredecessorRPC(succ)
	if err != nil || x == nil {
		fmt.Println("error getting predecessor, ", err, x)
		return
	}
	if x.Id != nil && between(x.Id, n.Id, succ.Id) {
		n.succMtx.Lock()
		n.successor = x
		n.succMtx.Unlock()
	}
	n.notifyRPC(succ, n.Node)
}

func (n *Node) checkPredecessor() {
	// implement using rpc func
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if pred != nil {
		err := n.transport.CheckPredecessor(pred)
		if err != nil {
			fmt.Println("predecessor failed!", err)
			n.predMtx.Lock()
			n.predecessor = nil
			n.predMtx.Unlock()
		}
	}
}

/*
	RPC callers implementation
*/

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getSuccessorRPC(node *models.Node) (*models.Node, error) {
	return n.transport.GetSuccessor(node)
}

// setSuccessorRPC sets the successor of a given node.
func (n *Node) setSuccessorRPC(node *models.Node, succ *models.Node) error {
	return n.transport.SetSuccessor(node, succ)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *models.Node, id []byte) (*models.Node, error) {
	return n.transport.FindSuccessor(node, id)
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getPredecessorRPC(node *models.Node) (*models.Node, error) {
	return n.transport.GetPredecessor(node)
}

// setPredecessorRPC sets the predecessor of a given node.
func (n *Node) setPredecessorRPC(node *models.Node, pred *models.Node) error {
	return n.transport.SetPredecessor(node, pred)
}

// notifyRPC notifies a remote node that pred is its predecessor.
func (n *Node) notifyRPC(node, pred *models.Node) error {
	return n.transport.Notify(node, pred)
}

func (n *Node) getKeyRPC(node *models.Node, key string) (*models.GetResponse, error) {
	return n.transport.GetKey(node, key)
}
func (n *Node) setKeyRPC(node *models.Node, key, value string) error {
	return n.transport.SetKey(node, key, value)
}
func (n *Node) deleteKeyRPC(node *models.Node, key string) error {
	return n.transport.DeleteKey(node, key)
}

func (n *Node) requestKeysRPC(
	node *models.Node, from []byte, to []byte,
) ([]*models.KV, error) {
	return n.transport.RequestKeys(node, from, to)
}

func (n *Node) deleteKeysRPC(
	node *models.Node, keys []string,
) error {
	return n.transport.DeleteKeys(node, keys)
}

/*
	RPC interface implementation
*/

// GetSuccessor gets the successor on the node..
func (n *Node) GetSuccessor(ctx context.Context, r *models.ER) (*models.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()
	if succ == nil {
		return emptyNode, nil
	}

	return succ, nil
}

// SetSuccessor sets the successor on the node..
func (n *Node) SetSuccessor(ctx context.Context, succ *models.Node) (*models.ER, error) {
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()
	return emptyRequest, nil
}

// SetPredecessor sets the predecessor on the node..
func (n *Node) SetPredecessor(ctx context.Context, pred *models.Node) (*models.ER, error) {
	n.predMtx.Lock()
	n.predecessor = pred
	n.predMtx.Unlock()
	return emptyRequest, nil
}

func (n *Node) FindSuccessor(ctx context.Context, id *models.ID) (*models.Node, error) {
	succ, err := n.findSuccessor(id.Id)
	if err != nil {
		return nil, err
	}

	if succ == nil {
		return nil, ERR_NO_SUCCESSOR
	}

	return succ, nil

}

func (n *Node) CheckPredecessor(ctx context.Context, id *models.ID) (*models.ER, error) {
	return emptyRequest, nil
}

func (n *Node) GetPredecessor(ctx context.Context, r *models.ER) (*models.Node, error) {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()
	if pred == nil {
		return emptyNode, nil
	}
	return pred, nil
}

func (n *Node) Notify(ctx context.Context, node *models.Node) (*models.ER, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()
	var prevPredNode *models.Node

	pred := n.predecessor
	if pred == nil || between(node.Id, pred.Id, n.Id) {
		// fmt.Println("setting predecessor", n.Id, node.Id)
		if n.predecessor != nil {
			prevPredNode = n.predecessor
		}
		n.predecessor = node

		// transfer keys from parent node
		if prevPredNode != nil {
			if between(n.predecessor.Id, prevPredNode.Id, n.Id) {
				n.transferKeys(prevPredNode, n.predecessor)
			}
		}

	}

	return emptyRequest, nil
}

func (n *Node) XGet(ctx context.Context, req *models.GetRequest) (*models.GetResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Get(req.Key)
	if err != nil {
		return emptyGetResponse, err
	}
	return &models.GetResponse{Value: val}, nil
}

func (n *Node) XSet(ctx context.Context, req *models.SetRequest) (*models.SetResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	fmt.Println("setting key on ", n.Node.Addr, req.Key, req.Value)
	err := n.storage.Set(req.Key, req.Value)
	return emptySetResponse, err
}

func (n *Node) XDelete(ctx context.Context, req *models.DeleteRequest) (*models.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.Delete(req.Key)
	return emptyDeleteResponse, err
}

func (n *Node) XRequestKeys(ctx context.Context, req *models.RequestKeysRequest) (*models.RequestKeysResponse, error) {
	n.stMtx.RLock()
	defer n.stMtx.RUnlock()
	val, err := n.storage.Between(req.From, req.To)
	if err != nil {
		return emptyRequestKeysResponse, err
	}
	return &models.RequestKeysResponse{Values: val}, nil
}

func (n *Node) XMultiDelete(ctx context.Context, req *models.MultiDeleteRequest) (*models.DeleteResponse, error) {
	n.stMtx.Lock()
	defer n.stMtx.Unlock()
	err := n.storage.MDelete(req.Keys...)
	return emptyDeleteResponse, err
}

func (n *Node) Stop() {
	close(n.shutdownCh)

	// Notify successor to change its predecessor pointer to our predecessor.
	// Do nothing if we are our own successor (i.e. we are the only node in the
	// ring).
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()

	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()

	if n.Node.Addr != succ.Addr && pred != nil {
		n.moveKeysFromLocal(pred, succ)
		predErr := n.setPredecessorRPC(succ, pred)
		succErr := n.setSuccessorRPC(pred, succ)
		fmt.Println("stop errors: ", predErr, succErr)
	}

	n.transport.Stop()
}
