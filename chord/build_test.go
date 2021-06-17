package chord

import (
	"dht/config"
	"fmt"
	"math/big"
	"testing"
	"time"
)

func Test_Init(t *testing.T) {
	cfg := config.DefaultNodeConfig()
	cfg.Id = "1"
	cfg.Addr = "127.0.0.1:8081"
	cfg.Timeout = 100 * time.Millisecond
	cfg.MaxIdle = 1 * time.Second

	node, _ := NewNode(cfg, nil)
	bytes := (&big.Int{}).SetBytes(node.Id)
	tableId := bytes.String()[:5]
	fmt.Println(node.Id, tableId)

	fmt.Println(node)
}