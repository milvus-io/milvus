package sessionutil

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
)

var Params paramtable.BaseTable

func TestGetServerIDConcurrently(t *testing.T) {
	ctx := context.Background()
	Params.Init()

	endpoints, err := Params.Load("_EtcdEndpoints")
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	if err != nil {
		panic(err)
	}

	etcdEndpoints := strings.Split(endpoints, ",")
	etcdKV, err := etcdkv.NewEtcdKV(etcdEndpoints, metaRoot)
	assert.NoError(t, err)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList sync.Mutex = sync.Mutex{}

	s := NewSession(ctx, metaRoot, etcdEndpoints)
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

	endpoints, err := Params.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdEndpoints := strings.Split(endpoints, ",")
	etcdKV, err := etcdkv.NewEtcdKV(etcdEndpoints, metaRoot)
	assert.NoError(t, err)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	s := NewSession(ctx, metaRoot, etcdEndpoints)
	s.Init("inittest", "testAddr", false)
	assert.NotEqual(t, int64(0), s.leaseID)
	assert.NotEqual(t, int64(0), s.ServerID)
	sessions, _, err := s.GetSessions("inittest")
	assert.Nil(t, err)
	assert.Contains(t, sessions, "inittest-"+strconv.FormatInt(s.ServerID, 10))
}

func TestUpdateSessions(t *testing.T) {
	ctx := context.Background()
	Params.Init()

	endpoints, err := Params.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}

	etcdEndpoints := strings.Split(endpoints, ",")
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdKV, err := etcdkv.NewEtcdKV(etcdEndpoints, "")
	assert.NoError(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList sync.Mutex = sync.Mutex{}

	s := NewSession(ctx, metaRoot, etcdEndpoints)

	sessions, rev, err := s.GetSessions("test")
	assert.Nil(t, err)
	assert.Equal(t, len(sessions), 0)
	eventCh := s.WatchServices("test", rev)

	sList := []*Session{}

	getIDFunc := func() {
		singleS := NewSession(ctx, metaRoot, etcdEndpoints)
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
		sessions, _, _ := s.GetSessions("test")
		return len(sessions) == 10
	}, 10*time.Second, 100*time.Millisecond)
	notExistSessions, _, _ := s.GetSessions("testt")
	assert.Equal(t, len(notExistSessions), 0)

	etcdKV.RemoveWithPrefix(metaRoot)
	assert.Eventually(t, func() bool {
		sessions, _, _ := s.GetSessions("test")
		return len(sessions) == 0
	}, 10*time.Second, 100*time.Millisecond)

	sessionEvents := []*SessionEvent{}
	addEventLen := 0
	delEventLen := 0
	eventLength := len(eventCh)
	for i := 0; i < eventLength; i++ {
		sessionEvent := <-eventCh
		if sessionEvent.EventType == SessionAddEvent {
			addEventLen++
		}
		if sessionEvent.EventType == SessionDelEvent {
			delEventLen++
		}
		sessionEvents = append(sessionEvents, sessionEvent)
	}
	assert.Equal(t, len(sessionEvents), 20)
	assert.Equal(t, addEventLen, 10)
	assert.Equal(t, delEventLen, 10)
}

func TestSessionLivenessCheck(t *testing.T) {
	s := &Session{}
	ctx := context.Background()
	ch := make(chan bool)
	s.liveCh = ch
	signal := make(chan struct{}, 1)

	flag := false

	go s.LivenessCheck(ctx, func() {
		flag = true
		signal <- struct{}{}
	})

	assert.False(t, flag)
	ch <- true

	assert.False(t, flag)
	close(ch)

	<-signal
	assert.True(t, flag)

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	ch = make(chan bool)
	s.liveCh = ch
	flag = false

	go s.LivenessCheck(ctx, func() {
		flag = true
		signal <- struct{}{}
	})

	assert.False(t, flag)
}
