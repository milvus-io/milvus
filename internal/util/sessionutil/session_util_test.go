package sessionutil

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
)

var Params paramtable.BaseTable

func TestGetServerIDConcurrently(t *testing.T) {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	rootPath := fmt.Sprintf("/%d/test/meta", randVal)
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList sync.Mutex = sync.Mutex{}

	self := NewSession("test", "testAddr", false)
	sm := NewSessionManager(ctx, etcdAddr, rootPath, self)
	res := make([]int64, 0)

	getIDFunc := func() {
		sm.checkIDExist()
		id, err := sm.getServerID()
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
	for i := 1; i <= 10; i++ {
		assert.Contains(t, res, int64(i))
	}

}

func TestInit(t *testing.T) {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	rootPath := fmt.Sprintf("/%d/test/meta", randVal)
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	self := NewSession("test", "testAddr", false)
	sm := NewSessionManager(ctx, etcdAddr, rootPath, self)
	sm.Init()
	assert.NotEqual(t, 0, sm.Self.LeaseID)
	assert.NotEqual(t, 0, sm.Self.ServerID)
}

func TestUpdateSessions(t *testing.T) {
	ctx := context.Background()
	rand.Seed(time.Now().UnixNano())
	randVal := rand.Int()
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	rootPath := fmt.Sprintf("/%d/test/meta", randVal)
	etcdKV := etcdkv.NewEtcdKV(cli, rootPath)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList sync.Mutex = sync.Mutex{}

	self := NewSession("test", "testAddr", false)
	sm := NewSessionManager(ctx, etcdAddr, rootPath, self)

	err = sm.UpdateSessions("test")
	assert.Nil(t, err)
	addCh, delCh := sm.WatchServices(ctx, "test")

	sessionManagers := make([]*SessionManager, 0)

	getIDFunc := func() {
		service := NewSession("test", "testAddr", false)
		singleManager := NewSessionManager(ctx, etcdAddr, rootPath, service)
		singleManager.Init()
		muList.Lock()
		sessionManagers = append(sessionManagers, singleManager)
		muList.Unlock()
		wg.Done()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go getIDFunc()
	}
	wg.Wait()

	assert.Eventually(t, func() bool {
		return len(sm.GetSessions("test")) == 10
	}, 10*time.Second, 100*time.Millisecond)
	assert.Equal(t, len(sm.GetSessions("testt")), 0)

	etcdKV.RemoveWithPrefix("")
	assert.Eventually(t, func() bool {
		return len(sm.GetSessions("test")) == 0
	}, 10*time.Second, 100*time.Millisecond)

	addSessions := []*Session{}
	for i := 0; i < 10; i++ {
		session := <-addCh
		addSessions = append(addSessions, session)
	}
	assert.Equal(t, len(addSessions), 10)

	delSessions := []*Session{}
	for i := 0; i < 10; i++ {
		session := <-delCh
		delSessions = append(delSessions, session)
	}
	assert.Equal(t, len(addSessions), 10)
}
