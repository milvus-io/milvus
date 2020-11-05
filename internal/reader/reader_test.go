package reader

//import (
//	"context"
//	"strconv"
//	"sync"
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//	"github.com/zilliztech/milvus-distributed/internal/conf"
//	"github.com/zilliztech/milvus-distributed/internal/msgclient"
//)
//
//const ctxTimeInMillisecond = 10
//
//// NOTE: start pulsar and etcd before test
//func TestReader_startQueryNode(t *testing.T) {
//	conf.LoadConfig("config.yaml")
//
//	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
//	ctx, cancel := context.WithDeadline(context.Background(), d)
//	defer cancel()
//
//	pulsarAddr := "pulsar://"
//	pulsarAddr += conf.Config.Pulsar.Address
//	pulsarAddr += ":"
//	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
//
//	StartQueryNode(ctx, pulsarAddr)
//
//	// To make sure to get here
//	assert.Equal(t, 0, 0)
//}
//
//// NOTE: start pulsar before test
//func TestReader_RunInsertDelete(t *testing.T) {
//	conf.LoadConfig("config.yaml")
//
//	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
//	ctx, cancel := context.WithDeadline(context.Background(), d)
//	defer cancel()
//
//	mc := msgclient.ReaderMessageClient{}
//	pulsarAddr := "pulsar://"
//	pulsarAddr += conf.Config.Pulsar.Address
//	pulsarAddr += ":"
//	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
//
//	mc.InitClient(ctx, pulsarAddr)
//	mc.ReceiveMessage()
//
//	node := CreateQueryNode(ctx, 0, 0, &mc)
//
//	wg := sync.WaitGroup{}
//
//	wg.Add(1)
//	go node.RunInsertDelete(&wg)
//	wg.Wait()
//
//	node.Close()
//
//	// To make sure to get here
//	assert.Equal(t, 0, 0)
//}
//
//// NOTE: start pulsar before test
//func TestReader_RunSearch(t *testing.T) {
//	conf.LoadConfig("config.yaml")
//
//	d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
//	ctx, cancel := context.WithDeadline(context.Background(), d)
//	defer cancel()
//
//	mc := msgclient.ReaderMessageClient{}
//	pulsarAddr := "pulsar://"
//	pulsarAddr += conf.Config.Pulsar.Address
//	pulsarAddr += ":"
//	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
//
//	mc.InitClient(ctx, pulsarAddr)
//	mc.ReceiveMessage()
//
//	node := CreateQueryNode(ctx, 0, 0, &mc)
//
//	wg := sync.WaitGroup{}
//
//	wg.Add(1)
//	go node.RunSearch(&wg)
//	wg.Wait()
//
//	node.Close()
//
//	// To make sure to get here
//	assert.Equal(t, 0, 0)
//}
