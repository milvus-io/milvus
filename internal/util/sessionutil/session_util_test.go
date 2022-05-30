package sessionutil

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
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
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList = sync.Mutex{}

	s := NewSession(ctx, metaRoot, etcdCli)
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
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	s := NewSession(ctx, metaRoot, etcdCli)
	s.Init("inittest", "testAddr", false, false)
	assert.NotEqual(t, int64(0), s.leaseID)
	assert.NotEqual(t, int64(0), s.ServerID)
	s.Register()
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
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	defer etcdCli.Close()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "")

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	var muList = sync.Mutex{}

	s := NewSession(ctx, metaRoot, etcdCli)

	sessions, rev, err := s.GetSessions("test")
	assert.Nil(t, err)
	assert.Equal(t, len(sessions), 0)
	eventCh := s.WatchServices("test", rev, nil)

	sList := []*Session{}

	getIDFunc := func() {
		etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
		require.NoError(t, err)
		singleS := NewSession(ctx, metaRoot, etcdCli)
		singleS.Init("test", "testAddr", false, false)
		singleS.Register()
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

func TestWatcherHandleWatchResp(t *testing.T) {
	ctx := context.Background()
	Params.Init()

	endpoints, err := Params.Load("_EtcdEndpoints")
	require.NoError(t, err)

	etcdEndpoints := strings.Split(endpoints, ",")
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	defer etcdCli.Close()

	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/by-dev/session-ut")
	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("/by-dev/session-ut")
	s := NewSession(ctx, metaRoot, etcdCli)
	defer s.Revoke(time.Second)

	getWatcher := func(s *Session, rewatch Rewatch) *sessionWatcher {
		return &sessionWatcher{
			s:       s,
			prefix:  "test",
			rewatch: rewatch,
			eventCh: make(chan *SessionEvent, 10),
		}
	}

	t.Run("handle normal events", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			Events: []*clientv3.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Value: []byte(`{"ServerID": 1, "ServerName": "test1"}`),
					},
				},
				{
					Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{
						Value: []byte(`{"ServerID": 2, "ServerName": "test2"}`),
					},
				},
			},
		}
		err := w.handleWatchResponse(wresp)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(w.eventCh))
	})

	t.Run("handle abnormal events", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			Events: []*clientv3.Event{
				{
					Type: mvccpb.PUT,
					Kv: &mvccpb.KeyValue{
						Value: []byte(``),
					},
				},
				{
					Type: mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{
						Value: []byte(``),
					},
				},
			},
		}
		var err error
		assert.NotPanics(t, func() {
			err = w.handleWatchResponse(wresp)
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(w.eventCh))
	})

	t.Run("err compacted resp, nil Rewatch", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			CompactRevision: 1,
		}
		err := w.handleWatchResponse(wresp)
		assert.NoError(t, err)
	})

	t.Run("err compacted resp, valid Rewatch", func(t *testing.T) {
		w := getWatcher(s, func(sessions map[string]*Session) error {
			return nil
		})
		wresp := clientv3.WatchResponse{
			CompactRevision: 1,
		}
		err := w.handleWatchResponse(wresp)
		assert.NoError(t, err)
	})

	t.Run("err canceled", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			Canceled: true,
		}
		err := w.handleWatchResponse(wresp)
		assert.Error(t, err)
	})

	t.Run("err handled but rewatch failed", func(t *testing.T) {
		w := getWatcher(s, func(sessions map[string]*Session) error {
			return errors.New("mocked")
		})
		wresp := clientv3.WatchResponse{
			CompactRevision: 1,
		}
		err := w.handleWatchResponse(wresp)
		t.Log(err.Error())

		assert.Error(t, err)
	})

	t.Run("err handled but list failed", func(t *testing.T) {
		s := NewSession(ctx, "/by-dev/session-ut", etcdCli)
		s.etcdCli.Close()
		w := getWatcher(s, func(sessions map[string]*Session) error {
			return nil
		})
		wresp := clientv3.WatchResponse{
			CompactRevision: 1,
		}

		err = w.handleWatchResponse(wresp)
		assert.Error(t, err)
	})

}

func TestSessionRevoke(t *testing.T) {
	s := &Session{}
	assert.NotPanics(t, func() {
		s.Revoke(time.Second)
	})

	s = (*Session)(nil)
	assert.NotPanics(t, func() {
		s.Revoke(time.Second)
	})

	ctx := context.Background()
	Params.Init()

	endpoints, err := Params.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	defer etcdCli.Close()
	require.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	s = NewSession(ctx, metaRoot, etcdCli)
	s.Init("revoketest", "testAddr", false, false)
	assert.NotPanics(t, func() {
		s.Revoke(time.Second)
	})
}

func TestSession_Registered(t *testing.T) {
	session := &Session{}
	session.UpdateRegistered(false)
	assert.False(t, session.Registered())
	session.UpdateRegistered(true)
	assert.True(t, session.Registered())
}

func TestSession_String(t *testing.T) {
	s := &Session{}
	log.Debug("log session", zap.Any("session", s))
}

func TestSessionProcessActiveStandBy(t *testing.T) {
	// initial etcd
	Params.Init()
	endpoints, err := Params.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	metaRoot := fmt.Sprintf("%d/%s1", rand.Int(), DefaultServiceRoot)

	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	defer etcdKV.Close()
	defer etcdKV.RemoveWithPrefix("")

	var wg sync.WaitGroup
	ch := make(chan bool)
	signal := make(chan struct{}, 1)
	flag := false

	// register session 1, will be active
	ctx1 := context.Background()
	s1 := NewSession(ctx1, metaRoot, etcdCli)
	s1.Init("inittest", "testAddr", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()
	wg.Add(1)
	s1.liveCh = ch
	s1.ProcessActiveStandBy(func() {
		log.Debug("Session 1 become active")
		wg.Done()
	})
	go s1.LivenessCheck(ctx1, func() {
		flag = true
		signal <- struct{}{}
		s1.keepAliveCancel()
		// directly delete the primary key to make this UT fast,
		// or the session2 has to wait for session1 release after ttl(60s)
		etcdCli.Delete(ctx1, s1.primaryKey)
	})
	assert.False(t, s1.isStandby.Load().(bool))

	// register session 2, will be standby
	ctx2 := context.Background()
	s2 := NewSession(ctx2, metaRoot, etcdCli)
	s2.Init("inittest", "testAddr", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	wg.Add(1)
	go s2.ProcessActiveStandBy(func() {
		log.Debug("Session 2 become active")
		wg.Done()
	})
	assert.True(t, s2.isStandby.Load().(bool))

	// stop session 1, session 2 will take over primary service
	log.Debug("Stop session 1, session 2 will take over primary service")
	assert.False(t, flag)
	ch <- true
	assert.False(t, flag)
	close(ch)
	<-signal
	assert.True(t, flag)

	wg.Wait()
	assert.False(t, s2.isStandby.Load().(bool))
}
