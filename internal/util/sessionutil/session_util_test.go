package sessionutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

var Params paramtable.BaseTable

func TestGetServerIDConcurrently(t *testing.T) {
	ctx := context.Background()
	Params.Init()

	endpoints := Params.LoadWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

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

	endpoints := Params.LoadWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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

	endpoints := Params.LoadWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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

	endpoints := Params.LoadWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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
			s:        s,
			prefix:   "test",
			rewatch:  rewatch,
			eventCh:  make(chan *SessionEvent, 10),
			validate: func(*Session) bool { return true },
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
		assert.NotPanics(t, func() {
			w.handleWatchResponse(wresp)
		})

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
		assert.NotPanics(t, func() {
			w.handleWatchResponse(wresp)
		})
		assert.Equal(t, 0, len(w.eventCh))
	})

	t.Run("err compacted resp, nil Rewatch", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			CompactRevision: 1,
		}
		assert.NotPanics(t, func() {
			w.handleWatchResponse(wresp)
		})
	})

	t.Run("err compacted resp, valid Rewatch", func(t *testing.T) {
		w := getWatcher(s, func(sessions map[string]*Session) error {
			return nil
		})
		wresp := clientv3.WatchResponse{
			CompactRevision: 1,
		}
		assert.NotPanics(t, func() {
			w.handleWatchResponse(wresp)
		})
	})

	t.Run("err canceled", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			Canceled: true,
		}

		assert.Panics(t, func() {
			w.handleWatchResponse(wresp)
		})
	})

	t.Run("err handled but rewatch failed", func(t *testing.T) {
		w := getWatcher(s, func(sessions map[string]*Session) error {
			return errors.New("mocked")
		})
		wresp := clientv3.WatchResponse{
			CompactRevision: 1,
		}
		assert.Panics(t, func() {
			w.handleWatchResponse(wresp)
		})
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

		assert.Panics(t, func() {
			w.handleWatchResponse(wresp)
		})

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

	endpoints := Params.LoadWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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

func TestSesssionMarshal(t *testing.T) {
	s := &Session{
		ServerID:   1,
		ServerName: "test",
		Address:    "localhost",
		Version:    common.Version,
	}

	bs, err := json.Marshal(s)
	require.NoError(t, err)

	s2 := &Session{}
	err = json.Unmarshal(bs, s2)
	assert.NoError(t, err)
	assert.Equal(t, s.ServerID, s2.ServerID)
	assert.Equal(t, s.ServerName, s2.ServerName)
	assert.Equal(t, s.Address, s2.Address)
	assert.Equal(t, s.Version.String(), s2.Version.String())
}

func TestSessionUnmarshal(t *testing.T) {
	t.Run("json failure", func(t *testing.T) {
		s := &Session{}
		err := json.Unmarshal([]byte("garbage"), s)
		assert.Error(t, err)
	})

	t.Run("version error", func(t *testing.T) {
		s := &Session{}
		err := json.Unmarshal([]byte(`{"Version": "a.b.c"}`), s)
		assert.Error(t, err)
	})
}

type SessionWithVersionSuite struct {
	suite.Suite
	tmpDir     string
	etcdServer *embed.Etcd

	metaRoot   string
	serverName string
	sessions   []*Session
	client     *clientv3.Client
}

// SetupSuite setup suite env
func (suite *SessionWithVersionSuite) SetupSuite() {
	dir, err := ioutil.TempDir(os.TempDir(), "milvus_ut")
	suite.Require().NoError(err)
	suite.tmpDir = dir
	suite.T().Log("using tmp dir:", dir)

	config := embed.NewConfig()

	config.Dir = os.TempDir()
	config.LogLevel = "warn"
	config.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:0")
	suite.Require().NoError(err)

	config.LCUrls = []url.URL{*u}
	u, err = url.Parse("http://localhost:0")
	suite.Require().NoError(err)
	config.LPUrls = []url.URL{*u}

	etcdServer, err := embed.StartEtcd(config)
	suite.Require().NoError(err)
	suite.etcdServer = etcdServer
}

func (suite *SessionWithVersionSuite) TearDownSuite() {
	if suite.etcdServer != nil {
		suite.etcdServer.Close()
	}
	if suite.tmpDir != "" {
		os.RemoveAll(suite.tmpDir)
	}
}

func (suite *SessionWithVersionSuite) SetupTest() {
	client := v3client.New(suite.etcdServer.Server)
	suite.client = client

	ctx := context.Background()
	suite.metaRoot = "sessionWithVersion"
	suite.serverName = "sessionComp"

	s1 := NewSession(ctx, suite.metaRoot, client)
	s1.Version.Major, s1.Version.Minor, s1.Version.Patch = 0, 0, 0
	s1.Init(suite.serverName, "s1", false, false)
	s1.Register()

	suite.sessions = append(suite.sessions, s1)

	s2 := NewSession(ctx, suite.metaRoot, client)
	s2.Version.Major, s2.Version.Minor, s2.Version.Patch = 2, 1, 0
	s2.Init(suite.serverName, "s2", false, false)
	s2.Register()

	suite.sessions = append(suite.sessions, s2)

	s3 := NewSession(ctx, suite.metaRoot, client)
	s3.Version.Major, s3.Version.Minor, s3.Version.Patch = 2, 2, 0
	s3.Version.Build = []string{"dev"}
	s3.Init(suite.serverName, "s3", false, false)
	s3.Register()

	suite.sessions = append(suite.sessions, s3)

}

func (suite *SessionWithVersionSuite) TearDownTest() {
	for _, s := range suite.sessions {
		s.Revoke(time.Second)
	}

	suite.sessions = nil
	_, err := suite.client.Delete(context.Background(), suite.metaRoot, clientv3.WithPrefix())
	suite.Require().NoError(err)

	if suite.client != nil {
		suite.client.Close()
		suite.client = nil
	}
}

func (suite *SessionWithVersionSuite) TestGetSessionsWithRangeVersion() {
	s := NewSession(context.Background(), suite.metaRoot, suite.client)

	suite.Run(">1.0.0", func() {
		r, err := semver.ParseRange(">1.0.0")
		suite.Require().NoError(err)

		result, _, err := s.GetSessionsWithVersionRange(suite.serverName, r)
		suite.Require().NoError(err)
		suite.Equal(2, len(result))
	})

	suite.Run(">2.1.0", func() {
		r, err := semver.ParseRange(">2.1.0")
		suite.Require().NoError(err)

		result, _, err := s.GetSessionsWithVersionRange(suite.serverName, r)
		suite.Require().NoError(err)
		suite.Equal(1, len(result))
	})

	suite.Run(">=2.2.0", func() {
		r, err := semver.ParseRange(">=2.2.0")
		suite.Require().NoError(err)

		result, _, err := s.GetSessionsWithVersionRange(suite.serverName, r)
		suite.Require().NoError(err)
		suite.Equal(0, len(result))
	})

	suite.Run(">=0.0.0 with garbage", func() {
		ctx := context.Background()
		r, err := semver.ParseRange(">=0.0.0")
		suite.Require().NoError(err)

		suite.client.Put(ctx, path.Join(suite.metaRoot, DefaultServiceRoot, suite.serverName, "garbage"), "garbage")
		suite.client.Put(ctx, path.Join(suite.metaRoot, DefaultServiceRoot, suite.serverName, "garbage_1"), `{"Version": "a.b.c"}`)

		_, _, err = s.GetSessionsWithVersionRange(suite.serverName, r)
		suite.Error(err)
	})
}

func (suite *SessionWithVersionSuite) TestWatchServicesWithVersionRange() {
	s := NewSession(context.Background(), suite.metaRoot, suite.client)

	suite.Run(">1.0.0 <=2.1.0", func() {
		r, err := semver.ParseRange(">1.0.0 <=2.1.0")
		suite.Require().NoError(err)

		_, rev, err := s.GetSessionsWithVersionRange(suite.serverName, r)
		suite.Require().NoError(err)

		ch := s.WatchServicesWithVersionRange(suite.serverName, r, rev, nil)

		// remove all sessions
		go func() {
			for _, s := range suite.sessions {
				s.Revoke(time.Second)
			}
		}()

		t := time.NewTimer(time.Second)
		defer t.Stop()
		select {
		case evt := <-ch:
			suite.Equal(suite.sessions[1].ServerID, evt.Session.ServerID)
		case <-t.C:
			suite.Fail("no event received, failing")
		}
	})
}

func TestSessionWithVersionRange(t *testing.T) {
	suite.Run(t, new(SessionWithVersionSuite))
}
