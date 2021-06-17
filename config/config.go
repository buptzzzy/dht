package config

import (
	"crypto/sha1"
	"fmt"
	"google.golang.org/grpc"
	"hash"
	"net"
	"strings"
	"time"
)

type NodeConfig struct {
	Id string
	Addr string

	ServerOpts []grpc.ServerOption
	DialOpts []grpc.DialOption

	Hash func() hash.Hash //Hash function to use
	HashSize int

	StabilizeMin time.Duration // Minimum stabilization time
	StabilizeMax time.Duration // Maximum stabilization time

	Timeout time.Duration
	MaxIdle time.Duration
}

type NodeEntry struct {
	First string `json:"first"`
	Id string `json:"id"`
	Address string `json:"address"`
}

func (c *NodeConfig) Validate() error {
	// hashsize shouldnt be less than hash func size
	//todo
	return nil
}

func DefaultNodeConfig() *NodeConfig {
	n := &NodeConfig{
		Hash:     sha1.New,
		DialOpts: make([]grpc.DialOption, 0, 5),
	}
	// n.HashSize = n.Hash().Size()
	n.HashSize = n.Hash().Size() * 8

	n.DialOpts = append(n.DialOpts,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
		grpc.FailOnNonTempDialError(true),
		grpc.WithInsecure(),
	)
	return n
}

func GenAddress(ip, port string) string {
	return ip + ":" + port
}

func GetIP(Address string) string {
	split := strings.Split(Address, ":")
	return split[0]
}

func GetPort(Address string) string {
	split := strings.Split(Address, ":")
	return split[1]
}

func GetOutbountIp() (ip string, err error) {
	//假装要发起连接了，拿到与外部通信的地址
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		fmt.Errorf("err:%v", err)
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	//fmt.Println(localAddr.String())
	ip = localAddr.IP.String()
	return
}