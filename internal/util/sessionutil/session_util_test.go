package sessionutil

import (
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
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "")
	_, err = cli.Delete(ctx, DefaultServiceRoot, clientv3.WithPrefix())
	assert.Nil(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList sync.Mutex = sync.Mutex{}

	s := NewSession(ctx, []string{etcdAddr})
	res := make([]int64, 0)

	getIDFunc := func() {
		s.checkIDExist()
		id, err := s.getServerID()
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
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "")
	_, err = cli.Delete(ctx, DefaultServiceRoot, clientv3.WithPrefix())
	assert.Nil(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	s := NewSession(ctx, []string{etcdAddr})
	s.Init("test", "testAddr", false)
	assert.NotEqual(t, int64(0), s.leaseID)
	assert.NotEqual(t, int64(0), s.ServerID)
}

func TestUpdateSessions(t *testing.T) {
	ctx := context.Background()
	Params.Init()

	etcdAddr, err := Params.Load("_EtcdAddress")
	if err != nil {
		panic(err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "")
	_, err = cli.Delete(ctx, DefaultServiceRoot, clientv3.WithPrefix())
	assert.Nil(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList sync.Mutex = sync.Mutex{}

	s := NewSession(ctx, []string{etcdAddr})

	sessions, err := s.GetSessions("test")
	assert.Nil(t, err)
	assert.Equal(t, len(sessions), 0)
	addCh, delCh := s.WatchServices("test")

	sList := []*Session{}

	getIDFunc := func() {
		singleS := NewSession(ctx, []string{etcdAddr})
		singleS.Init("test", "testAddr", false)
		muList.Lock()
		sList = append(sList, singleS)
		muList.Unlock()
		wg.Done()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go getIDFunc()
	}
	wg.Wait()

	assert.Eventually(t, func() bool {
		sessions, _ := s.GetSessions("test")
		return len(sessions) == 10
	}, 10*time.Second, 100*time.Millisecond)
	notExistSessions, _ := s.GetSessions("testt")
	assert.Equal(t, len(notExistSessions), 0)

	etcdKV.RemoveWithPrefix("")
	assert.Eventually(t, func() bool {
		sessions, _ := s.GetSessions("test")
		return len(sessions) == 0
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
