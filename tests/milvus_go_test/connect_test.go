package main

import (
	"fmt"
	"github.com/milvus-io/milvus-sdk-go/milvus"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestConnect(t *testing.T)  {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{IPAddress: ip, Port: strconv.Itoa(port)}
	err := client.Connect(connectParam)
	assert.Nil(t, err)
}

func TestConnectRepeat(t *testing.T)  {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{IPAddress: ip, Port: strconv.Itoa(port)}
	err := client.Connect(connectParam)
	assert.Nil(t, err)
	err1 := client.Connect(connectParam)
	assert.Nil(t, err1)
	
}

func TestDisconnect(t *testing.T)  {
	client := GetClient()
	error := client.Disconnect()
	assert.Nil(t, error)
	assert.False(t, client.IsConnected())
}

func GenInvalidConnectArgs() [][]string{
	var port string = "19530"
	args := [][]string{
		{"1.1.1.1", port},
		{"255.255.0.0", port},
		{"1.2.2", port},
		{"中文", port},
		{"www.baidu.com", "100000"},
		{"127.0.0.1", "100000"},
		{"www.baidu.com", "80"},
	}
	return args
}
func TestConnectInvalidConnectArgs(t *testing.T)  {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	args := GenInvalidConnectArgs()
	for _,arg := range args {
		connectParam := milvus.ConnectParam{IPAddress: arg[0], Port: arg[1]}
		err := client.Connect(connectParam)
		assert.NotNil(t, err)
		fmt.Println(err)
	}
}