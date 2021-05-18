package sessionutil

import (
	"fmt"
	"sync"
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
)

var Params paramtable.BaseTable

func TestGetServerID(t *testing.T) {
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList sync.Mutex = sync.Mutex{}

	res := make([]int64, 0)

	getIDFunc := func() {
		id, err := GetServerID(etcdKV)
		assert.Nil(t, err)
		muList.Lock()
		res = append(res, id)
		muList.Unlock()
		wg.Done()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go getIDFunc()
	}
	wg.Wait()
	for i := 0; i < 10; i++ {
		assert.Contains(t, res, int64(i))
	}

}

func TestRegister(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	addChannel, deletechannel := WatchServices(ctx, etcdKV, "test")
	for i := 0; i < 10; i++ {
		id, err := GetServerID(etcdKV)
		assert.Nil(t, err)
		session := NewSession(id, "test", "localhost")
		_, err = RegisterService(etcdKV, false, session, 10)
		assert.Nil(t, err)
		sessionReturn := <-addChannel
		assert.Equal(t, sessionReturn, session)
	}

	sessions, err := GetSessions(etcdKV, "test")
	assert.Nil(t, err)
	assert.Equal(t, len(sessions), 10)
	for i := 10; i < 10; i++ {
		assert.Equal(t, sessions[i].ServerID, int64(i))
		err = etcdKV.Remove(fmt.Sprintf("test-%d", i))
		assert.Nil(t, err)
		sessionReturn := <-deletechannel
		assert.Equal(t, sessionReturn, sessions[i])
	}
}

func TestRegisterExclusive(t *testing.T) {
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	id, err := GetServerID(etcdKV)
	assert.Nil(t, err)
	session := NewSession(id, "test", "localhost")
	_, err = RegisterService(etcdKV, true, session, 10)
	assert.Nil(t, err)

	id, err = GetServerID(etcdKV)
	assert.Nil(t, err)
	session = NewSession(id, "test", "helloworld")
	_, err = RegisterService(etcdKV, true, session, 10)
	assert.NotNil(t, err)
}

func TestKeepAlive(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	rootPath := "/etcd/test/root"
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	id, err := GetServerID(etcdKV)
	assert.Nil(t, err)
	session := NewSession(id, "test", "localhost")
	ch, err := RegisterService(etcdKV, false, session, 10)
	assert.Nil(t, err)
	aliveCh := ProcessKeepAliveResponse(ctx, ch)

	signal := <-aliveCh
	assert.Equal(t, signal, true)

	sessions, err := GetSessions(etcdKV, "test")
	assert.Nil(t, err)
	assert.Equal(t, len(sessions), 1)
	assert.Equal(t, sessions[0].ServerID, int64(0))
}
