package config

import (
	"fmt"
	"testing"
)

func TestName(t *testing.T) {
	ip, err := GetOutbountIp()
	if err != nil {
		fmt.Printf("get ip failed:err:%v.\n", err)
	}
	if ip != "" {
		fmt.Println("get IP success!")
	}
	address := "127.9.0.1:8080"
	ip = "127.0.1.3"
	port := "9011"
	fmt.Println(address, ip, port)
	address = GenAddress(ip, port)
	ip = GetIP(address)
	port = GetPort(address)
	fmt.Println(address, ip, port)
}
