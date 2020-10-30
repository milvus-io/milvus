package main

import (
	"fmt"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{IPAddress: ip, Port: port}
	err := client.Connect(connectParam)
	assert.Nil(t, err)
}

func TestConnectRepeat(t *testing.T) {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{IPAddress: ip, Port: port}
	err := client.Connect(connectParam)
	assert.Nil(t, err)
	err1 := client.Connect(connectParam)
	assert.Nil(t, err1)
}

func TestDisconnect(t *testing.T) {
	client := GetClient()
	error := client.Disconnect()
	assert.Nil(t, error)
	assert.False(t, client.IsConnected())
}

func GenInvalidConnectArgs() map[string]int64 {
	var port int64 = 19530
	args := map[string]int64{
		"1.1.1.1":       port,
		"www.baidu.com": 100000,
	}
	return args
}

func TestConnectInvalidConnectArgs(t *testing.T) {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	args := GenInvalidConnectArgs()
	for host := range args {
		connectParam := milvus.ConnectParam{IPAddress: host, Port: args[host]}
		err := client.Connect(connectParam)
		assert.NotNil(t, err)
		fmt.Println(err)
	}
}

func TestDisconnectRepeat(t *testing.T) {
	client := GetClient()
	client.Disconnect()
	assert.False(t, client.IsConnected())
	error := client.Disconnect()
	assert.Nil(t, error)
}
