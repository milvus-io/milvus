package sessionutil

import (
	"context"
	"encoding/json"
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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestGetServerIDConcurrently(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	params := paramtable.Get()

	endpoints := params.GetWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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
		assert.NoError(t, err)
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
	assert.ElementsMatch(t, []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, res)
}

func TestInit(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	params := paramtable.Get()

	endpoints := params.GetWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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
	assert.NoError(t, err)
	assert.Contains(t, sessions, "inittest-"+strconv.FormatInt(s.ServerID, 10))
}

func TestUpdateSessions(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	params := paramtable.Get()

	endpoints := params.GetWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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

	s := NewSession(ctx, metaRoot, etcdCli, WithResueNodeID(false))

	sessions, rev, err := s.GetSessions("test")
	assert.NoError(t, err)
	assert.Equal(t, len(sessions), 0)
	eventCh := s.WatchServices("test", rev, nil)

	sList := []*Session{}

	getIDFunc := func() {
		etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
		require.NoError(t, err)
		singleS := NewSession(ctx, metaRoot, etcdCli, WithResueNodeID(false))
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
	paramtable.Init()
	params := paramtable.Get()

	endpoints := params.GetWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	s := NewSession(context.Background(), metaRoot, etcdCli)
	ctx := context.Background()
	ch := make(chan bool)
	s.liveCh = ch
	signal := make(chan struct{}, 1)

	flag := false

	s.LivenessCheck(ctx, func() {
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

	s.LivenessCheck(ctx, func() {
		flag = true
		signal <- struct{}{}
	})

	assert.False(t, flag)
}

func TestWatcherHandleWatchResp(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	params := paramtable.Get()

	endpoints := params.GetWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
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

	s1 := NewSession(ctx, suite.metaRoot, client, WithResueNodeID(false))
	s1.Version.Major, s1.Version.Minor, s1.Version.Patch = 0, 0, 0
	s1.Init(suite.serverName, "s1", false, false)
	s1.Register()

	suite.sessions = append(suite.sessions, s1)

	s2 := NewSession(ctx, suite.metaRoot, client, WithResueNodeID(false))
	s2.Version.Major, s2.Version.Minor, s2.Version.Patch = 2, 1, 0
	s2.Init(suite.serverName, "s2", false, false)
	s2.Register()

	suite.sessions = append(suite.sessions, s2)

	s3 := NewSession(ctx, suite.metaRoot, client, WithResueNodeID(false))
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
	s := NewSession(context.Background(), suite.metaRoot, suite.client, WithResueNodeID(false))

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
	s := NewSession(context.Background(), suite.metaRoot, suite.client, WithResueNodeID(false))

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

		select {
		case evt := <-ch:
			suite.Equal(suite.sessions[1].ServerID, evt.Session.ServerID)
		case <-time.After(time.Second):
			suite.Fail("no event received, failing")
		}
	})
}

func TestSessionWithVersionRange(t *testing.T) {
	suite.Run(t, new(SessionWithVersionSuite))
}

func TestSessionProcessActiveStandBy(t *testing.T) {
	// initial etcd
	paramtable.Init()
	params := paramtable.Get()
	endpoints, err := params.Load("_EtcdEndpoints")
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
	s1 := NewSession(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("inittest", "testAddr", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.SetEnableRetryKeepAlive(false)
	s1.Register()
	wg.Add(1)
	s1.liveCh = ch
	s1.ProcessActiveStandBy(func() error {
		log.Debug("Session 1 become active")
		wg.Done()
		return nil
	})
	s1.LivenessCheck(ctx1, func() {
		flag = true
		signal <- struct{}{}
		s1.keepAliveCancel()
	})
	assert.False(t, s1.isStandby.Load().(bool))

	// register session 2, will be standby
	ctx2 := context.Background()
	s2 := NewSession(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("inittest", "testAddr", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.SetEnableRetryKeepAlive(false)
	s2.Register()
	wg.Add(1)
	go s2.ProcessActiveStandBy(func() error {
		log.Debug("Session 2 become active")
		wg.Done()
		return nil
	})
	assert.True(t, s2.isStandby.Load().(bool))

	//assert.True(t, s2.watchingPrimaryKeyLock)
	// stop session 1, session 2 will take over primary service
	log.Debug("Stop session 1, session 2 will take over primary service")
	assert.False(t, flag)
	ch <- true
	assert.False(t, flag)
	s1.safeCloseLiveCh()
	<-signal
	assert.True(t, flag)

	wg.Wait()
	assert.False(t, s2.isStandby.Load().(bool))
}

func TestSessionEventType_String(t *testing.T) {
	tests := []struct {
		name string
		t    SessionEventType
		want string
	}{
		{t: SessionNoneEvent, want: ""},
		{t: SessionAddEvent, want: "SessionAddEvent"},
		{t: SessionDelEvent, want: "SessionDelEvent"},
		{t: SessionUpdateEvent, want: "SessionUpdateEvent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.t.String(), "String()")
		})
	}
}

func TestSession_apply(t *testing.T) {
	session := &Session{}
	opts := []SessionOption{WithTTL(100), WithRetryTimes(200)}
	session.apply(opts...)
	assert.Equal(t, int64(100), session.sessionTTL)
	assert.Equal(t, int64(200), session.sessionRetryTimes)
}

func TestIntegrationMode(t *testing.T) {
	ctx := context.Background()
	params := paramtable.Get()
	params.Init()
	params.Save(params.IntegrationTestCfg.IntegrationMode.Key, "true")

	endpoints := params.GetWithDefault("etcd.endpoints", paramtable.DefaultEtcdEndpoints)
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err = etcdKV.RemoveWithPrefix("")
	assert.NoError(t, err)

	s1 := NewSession(ctx, metaRoot, etcdCli)
	assert.Equal(t, false, s1.reuseNodeID)
	s2 := NewSession(ctx, metaRoot, etcdCli)
	assert.Equal(t, false, s2.reuseNodeID)
	s1.Init("inittest1", "testAddr1", false, false)
	s1.Init("inittest2", "testAddr2", false, false)
	assert.NotEqual(t, s1.ServerID, s2.ServerID)
}

type SessionSuite struct {
	suite.Suite
	tmpDir     string
	etcdServer *embed.Etcd

	metaRoot   string
	serverName string
	client     *clientv3.Client
}

func (s *SessionSuite) SetupSuite() {
	paramtable.Init()
	dir, err := ioutil.TempDir(os.TempDir(), "milvus_ut")
	s.Require().NoError(err)
	s.tmpDir = dir
	s.T().Log("using tmp dir:", dir)

	config := embed.NewConfig()

	config.Dir = os.TempDir()
	config.LogLevel = "warn"
	config.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:0")
	s.Require().NoError(err)

	config.LCUrls = []url.URL{*u}
	u, err = url.Parse("http://localhost:0")
	s.Require().NoError(err)
	config.LPUrls = []url.URL{*u}

	etcdServer, err := embed.StartEtcd(config)
	s.Require().NoError(err)
	s.etcdServer = etcdServer
}

func (s *SessionSuite) TearDownSuite() {
	if s.etcdServer != nil {
		s.etcdServer.Close()
	}
	if s.tmpDir != "" {
		os.RemoveAll(s.tmpDir)
	}
}

func (s *SessionSuite) SetupTest() {
	client := v3client.New(s.etcdServer.Server)
	s.client = client

	s.metaRoot = fmt.Sprintf("milvus-ut/session-%s/", funcutil.GenRandomStr())
}

func (s *SessionSuite) TearDownTest() {
	_, err := s.client.Delete(context.Background(), s.metaRoot, clientv3.WithPrefix())
	s.Require().NoError(err)

	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *SessionSuite) TestDisconnected() {
	st := &Session{}
	st.SetDisconnected(true)
	sf := &Session{}
	sf.SetDisconnected(false)

	cases := []struct {
		tag    string
		input  *Session
		expect bool
	}{
		{"not_set", &Session{}, false},
		{"set_true", st, true},
		{"set_false", sf, false},
	}

	for _, c := range cases {
		s.Run(c.tag, func() {
			s.Equal(c.expect, c.input.Disconnected())
		})
	}
}

func (s *SessionSuite) TestRetryKeepAlive() {
	st := &Session{}
	st.retryKeepAlive.Store(true)
	sf := &Session{}
	sf.retryKeepAlive.Store(false)

	cases := []struct {
		tag    string
		input  *Session
		expect bool
	}{
		{"not_set", &Session{}, false},
		{"set_true", st, true},
		{"set_false", sf, false},
	}

	for _, c := range cases {
		s.Run(c.tag, func() {
			s.Equal(c.expect, c.input.isRetryingKeepAlive())
		})
	}
}

func (s *SessionSuite) TestGoingStop() {
	ctx := context.Background()
	sdisconnect := NewSession(ctx, s.metaRoot, s.client)
	sdisconnect.SetDisconnected(true)

	sess := NewSession(ctx, s.metaRoot, s.client)
	sess.Init("test", "normal", false, false)
	sess.Register()

	cases := []struct {
		tag         string
		input       *Session
		expectError bool
	}{
		{"nil", nil, true},
		{"not_inited", &Session{}, true},
		{"disconnected", sdisconnect, true},
		{"normal", sess, false},
	}

	for _, c := range cases {
		s.Run(c.tag, func() {
			err := c.input.GoingStop()
			if c.expectError {
				s.Error(err)
			} else {
				s.NoError(err)
			}
		})
	}
}

func (s *SessionSuite) TestRevoke() {
	ctx := context.Background()
	disconnected := NewSession(ctx, s.metaRoot, s.client, WithResueNodeID(false))
	disconnected.Init("test", "disconnected", false, false)
	disconnected.Register()
	disconnected.SetDisconnected(true)

	sess := NewSession(ctx, s.metaRoot, s.client, WithResueNodeID(false))
	sess.Init("test", "normal", false, false)
	sess.Register()

	cases := []struct {
		tag      string
		input    *Session
		preExist bool
		success  bool
	}{
		{"not_inited", &Session{}, false, true},
		{"disconnected", disconnected, true, false},
		{"normal", sess, false, true},
	}

	for _, c := range cases {
		s.Run(c.tag, func() {
			c.input.Revoke(time.Second)
			resp, err := s.client.Get(ctx, c.input.getCompleteKey())
			s.Require().NoError(err)
			if !c.preExist || c.success {
				s.Equal(0, len(resp.Kvs))
			}
			if c.preExist && !c.success {
				s.Equal(1, len(resp.Kvs))
			}
		})
	}
}

func (s *SessionSuite) TestKeepAliveRetryEnable() {
	ctx := context.Background()
	session := NewSession(ctx, s.metaRoot, s.client)
	session.Init("test", "normal", false, false)

	// Register
	ch, err := session.registerService()
	if err != nil {
		panic(err)
	}
	session.SetEnableRetryKeepAlive(true)
	session.liveCh = make(chan bool)
	session.processKeepAliveResponse(ch)
	session.LivenessCheck(ctx, nil)
	session.keepAliveCancel()

	// sleep a while wait goroutine process
	time.Sleep(time.Millisecond * 100)
	// expected Disconnected = false, means session is not closed
	assert.Equal(s.T(), false, session.Disconnected())
}

func (s *SessionSuite) TestKeepAliveRetryDisable() {
	ctx := context.Background()
	session := NewSession(ctx, s.metaRoot, s.client)
	session.Init("test", "normal", false, false)

	// Register
	ch, err := session.registerService()
	if err != nil {
		panic(err)
	}
	session.SetEnableRetryKeepAlive(false)
	session.liveCh = make(chan bool)
	session.processKeepAliveResponse(ch)
	session.LivenessCheck(ctx, nil)
	session.keepAliveCancel()

	// sleep a while wait goroutine process
	time.Sleep(time.Millisecond * 100)
	// expected Disconnected = true, means session is closed
	assert.Equal(s.T(), true, session.Disconnected())
}

func (s *SessionSuite) TestSafeCloseLiveCh() {
	ctx := context.Background()
	session := NewSession(ctx, s.metaRoot, s.client)
	session.Init("test", "normal", false, false)
	session.liveCh = make(chan bool)
	session.safeCloseLiveCh()
	assert.NotPanics(s.T(), func() {
		session.safeCloseLiveCh()
	})
}

func TestSessionSuite(t *testing.T) {
	suite.Run(t, new(SessionSuite))
}
