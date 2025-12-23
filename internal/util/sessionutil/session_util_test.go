package sessionutil

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/json"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestGetServerIDConcurrently(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err := etcdKV.RemoveWithPrefix(ctx, "")
	assert.NoError(t, err)

	defer etcdKV.RemoveWithPrefix(ctx, "")

	var wg sync.WaitGroup
	muList := sync.Mutex{}

	s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
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

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err := etcdKV.RemoveWithPrefix(ctx, "")
	assert.NoError(t, err)

	defer etcdKV.RemoveWithPrefix(ctx, "")

	s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
	s.Init("inittest", "testAddr", false, false)
	assert.NotEqual(t, int64(0), s.LeaseID)
	assert.NotEqual(t, int64(0), s.ServerID)
	s.Register()
	sessions, _, err := s.GetSessions(ctx, "inittest")
	assert.NoError(t, err)
	assert.Contains(t, sessions, "inittest-"+strconv.FormatInt(s.ServerID, 10))
}

func TestInitNoArgs(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err := etcdKV.RemoveWithPrefix(ctx, "")
	assert.NoError(t, err)

	defer etcdKV.RemoveWithPrefix(ctx, "")

	s := NewSession(ctx)
	s.Init("inittest", "testAddr", false, false)
	assert.NotEqual(t, int64(0), s.LeaseID)
	assert.NotEqual(t, int64(0), s.ServerID)
	s.Register()
	sessions, _, err := s.GetSessions(ctx, "inittest")
	assert.NoError(t, err)
	assert.Contains(t, sessions, "inittest-"+strconv.FormatInt(s.ServerID, 10))
}

func TestUpdateSessions(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "")

	defer etcdKV.RemoveWithPrefix(ctx, "")

	var wg sync.WaitGroup
	muList := sync.Mutex{}

	s := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithResueNodeID(false))

	sessions, rev, err := s.GetSessions(ctx, "test")
	assert.NoError(t, err)
	assert.Equal(t, len(sessions), 0)
	watcher := s.WatchServices("test", rev, nil)

	sList := []*Session{}

	getIDFunc := func() {
		singleS := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithResueNodeID(false))
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
		sessions, _, _ := s.GetSessions(ctx, "test")
		return len(sessions) == 10
	}, 10*time.Second, 100*time.Millisecond)
	notExistSessions, _, _ := s.GetSessions(ctx, "testt")
	assert.Equal(t, len(notExistSessions), 0)

	etcdKV.RemoveWithPrefix(ctx, metaRoot)
	assert.Eventually(t, func() bool {
		sessions, _, _ := s.GetSessions(ctx, "test")
		return len(sessions) == 0
	}, 10*time.Second, 100*time.Millisecond)

	sessionEvents := []*SessionEvent{}
	addEventLen := 0
	delEventLen := 0

	ch := time.After(time.Second * 5)
LOOP:
	for {
		select {
		case <-ch:
			t.FailNow()
		case sessionEvent := <-watcher.EventChannel():

			if sessionEvent.EventType == SessionAddEvent {
				addEventLen++
			}
			if sessionEvent.EventType == SessionDelEvent {
				delEventLen++
			}
			sessionEvents = append(sessionEvents, sessionEvent)
			if len(sessionEvents) == 20 {
				break LOOP
			}
		}
	}
	assert.Equal(t, addEventLen, 10)
	assert.Equal(t, delEventLen, 10)
}

func TestWatcherHandleWatchResp(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()

	etcdKV := etcdkv.NewEtcdKV(etcdCli, "/by-dev/session-ut")
	defer etcdKV.RemoveWithPrefix(ctx, "/by-dev/session-ut")
	s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
	defer s.Stop()

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
		SessionRaw: SessionRaw{
			ServerID:   1,
			ServerName: "test",
			Address:    "localhost",
		},
		Version: common.Version,
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
	tmpDir string

	metaRoot   string
	serverName string
	sessions   []*Session
	client     *clientv3.Client
}

func (suite *SessionWithVersionSuite) SetupSuite() {
	client, _ := kvfactory.GetEtcdAndPath()
	suite.client = client
}

func (suite *SessionWithVersionSuite) SetupTest() {
	ctx := context.Background()
	suite.metaRoot = "sessionWithVersion"
	suite.serverName = "sessionComp"

	s1 := NewSessionWithEtcd(ctx, suite.metaRoot, suite.client, WithResueNodeID(false))
	s1.Version.Major, s1.Version.Minor, s1.Version.Patch = 0, 0, 0
	s1.Init(suite.serverName, "s1", false, false)
	s1.Register()

	suite.sessions = append(suite.sessions, s1)

	s2 := NewSessionWithEtcd(ctx, suite.metaRoot, suite.client, WithResueNodeID(false))
	s2.Version.Major, s2.Version.Minor, s2.Version.Patch = 2, 1, 0
	s2.Init(suite.serverName, "s2", false, false)
	s2.Register()

	suite.sessions = append(suite.sessions, s2)

	s3 := NewSessionWithEtcd(ctx, suite.metaRoot, suite.client, WithResueNodeID(false))
	s3.Version.Major, s3.Version.Minor, s3.Version.Patch = 2, 2, 0
	s3.Version.Build = []string{"dev"}
	s3.Init(suite.serverName, "s3", false, false)
	s3.Register()

	suite.sessions = append(suite.sessions, s3)
}

func (suite *SessionWithVersionSuite) TearDownTest() {
	for _, s := range suite.sessions {
		s.Stop()
	}

	suite.sessions = nil
	client, _ := kvfactory.GetEtcdAndPath()
	_, err := client.Delete(context.Background(), suite.metaRoot, clientv3.WithPrefix())
	suite.Require().NoError(err)
}

func (suite *SessionWithVersionSuite) TestGetSessionsWithRangeVersion() {
	s := NewSessionWithEtcd(context.Background(), suite.metaRoot, suite.client, WithResueNodeID(false))

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

	suite.Run(">2.2.0", func() {
		r, err := semver.ParseRange(">2.2.0")
		suite.Require().NoError(err)

		result, _, err := s.GetSessionsWithVersionRange(suite.serverName, r)
		suite.Require().NoError(err)
		suite.Equal(0, len(result))
		suite.T().Log(result)
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
	s := NewSessionWithEtcd(context.Background(), suite.metaRoot, suite.client, WithResueNodeID(false))

	suite.Run(">1.0.0 <=2.1.0", func() {
		r, err := semver.ParseRange(">1.0.0 <=2.1.0")
		suite.Require().NoError(err)

		_, rev, err := s.GetSessionsWithVersionRange(suite.serverName, r)
		suite.Require().NoError(err)

		watcher := s.WatchServicesWithVersionRange(suite.serverName, r, rev, nil)

		// remove all sessions
		go func() {
			for _, s := range suite.sessions {
				s.Stop()
			}
		}()

		select {
		case evt := <-watcher.EventChannel():
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
	ctx := context.TODO()
	// initial etcd
	paramtable.Init()
	metaRoot := fmt.Sprintf("%d/%s1", rand.Int(), DefaultServiceRoot)

	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err := etcdKV.RemoveWithPrefix(ctx, "")
	assert.NoError(t, err)

	defer etcdKV.RemoveWithPrefix(ctx, "")

	var wg sync.WaitGroup
	flag := false

	// register session 1, will be active
	ctx1 := context.Background()
	s1 := NewSessionWithEtcd(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("inittest", "testAddr", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()
	wg.Add(1)
	s1.ProcessActiveStandBy(func() error {
		log.Debug("Session 1 become active")
		wg.Done()
		return nil
	})
	wg.Wait()
	assert.False(t, s1.isStandby.Load().(bool))

	// register session 2, will be standby
	ctx2 := context.Background()
	s2 := NewSessionWithEtcd(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("inittest", "testAddr", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	wg.Add(1)
	go s2.ProcessActiveStandBy(func() error {
		log.Debug("Session 2 become active")
		wg.Done()
		return nil
	})
	assert.True(t, s2.isStandby.Load().(bool))

	// assert.True(t, s2.watchingPrimaryKeyLock)
	// stop session 1, session 2 will take over primary service
	log.Debug("Stop session 1, session 2 will take over primary service")
	assert.False(t, flag)

	s1.Stop()

	wg.Wait()
	log.Debug("session s2 wait done")
	assert.False(t, s2.isStandby.Load().(bool))
	s2.Stop()
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

func TestServerInfoOp(t *testing.T) {
	t.Run("test with specified pid", func(t *testing.T) {
		pid := 9999999
		serverID := int64(999)

		filePath := GetServerInfoFilePath(pid)
		defer os.RemoveAll(filePath)

		saveServerInfoInternal(typeutil.QueryCoordRole, serverID, pid)
		saveServerInfoInternal(typeutil.DataCoordRole, serverID, pid)
		saveServerInfoInternal(typeutil.ProxyRole, serverID, pid)

		sessions := GetSessions(pid)
		assert.Equal(t, 3, len(sessions))
		assert.ElementsMatch(t, sessions, []string{
			"querycoord-999",
			"datacoord-999",
			"proxy-999",
		})

		RemoveServerInfoFile(pid)
		sessions = GetSessions(pid)
		assert.Equal(t, 0, len(sessions))
	})

	t.Run("test with os pid", func(t *testing.T) {
		serverID := int64(9999)
		filePath := GetServerInfoFilePath(os.Getpid())
		defer os.RemoveAll(filePath)

		SaveServerInfo(typeutil.QueryCoordRole, serverID)
		sessions := GetSessions(os.Getpid())
		assert.Equal(t, 1, len(sessions))
	})
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
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.IntegrationTestCfg.IntegrationMode.Key, "true")

	endpoints := params.EtcdCfg.Endpoints.GetValue()
	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)

	etcdEndpoints := strings.Split(endpoints, ",")
	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	require.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	err = etcdKV.RemoveWithPrefix(ctx, "")
	assert.NoError(t, err)

	s1 := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
	assert.Equal(t, false, s1.reuseNodeID)
	s2 := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
	assert.Equal(t, false, s2.reuseNodeID)
	s1.Init("inittest1", "testAddr1", false, false)
	s1.Init("inittest2", "testAddr2", false, false)
	assert.NotEqual(t, s1.ServerID, s2.ServerID)
}

type SessionSuite struct {
	suite.Suite
	tmpDir string

	metaRoot   string
	serverName string
	client     *clientv3.Client
}

func (s *SessionSuite) SetupSuite() {
	paramtable.Init()
}

func (s *SessionSuite) TearDownSuite() {
}

func (s *SessionSuite) SetupTest() {
	s.client, _ = kvfactory.GetEtcdAndPath()
	s.metaRoot = fmt.Sprintf("milvus-ut/session-%s/", funcutil.GenRandomStr())
}

func (s *SessionSuite) TearDownTest() {
	_, err := s.client.Delete(context.Background(), s.metaRoot, clientv3.WithPrefix())
	s.Require().NoError(err)
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

func (s *SessionSuite) TestGoingStop() {
	ctx := context.Background()
	sdisconnect := NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	sdisconnect.SetDisconnected(true)

	sess := NewSessionWithEtcd(ctx, s.metaRoot, s.client)
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

func (s *SessionSuite) TestKeepAliveRetryActiveCancel() {
	ctx := context.Background()
	session := NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	session.Init("test", "normal", false, false)

	// Register
	err := session.registerService()
	s.Require().NoError(err)
	session.startKeepAliveLoop()
	session.Stop()

	// wait workers exit
	session.wg.Wait()
}

func (s *SessionSuite) TestKeepAliveRetryChannelClose() {
	ctx := context.Background()
	session := NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	session.Init("test", "normal", false, false)

	// Register
	err := session.registerService()
	if err != nil {
		panic(err)
	}
	closeChan := make(chan *clientv3.LeaseKeepAliveResponse)
	session.startKeepAliveLoop()
	// close channel, should retry connect
	close(closeChan)

	// sleep a while wait goroutine process
	time.Sleep(time.Millisecond * 100)
	// expected Disconnected = false, means session is not closed
	assert.Equal(s.T(), false, session.Disconnected())
	time.Sleep(time.Second * 1)
	// expected Disconnected = false, means session is not closed, keepalive keeps working
	assert.Equal(s.T(), false, session.Disconnected())
}

func (s *SessionSuite) TestGetSessions() {
	os.Setenv("MILVUS_SERVER_LABEL_key1", "value1")
	os.Setenv("MILVUS_SERVER_LABEL_key2", "value2")
	os.Setenv("MILVUS_SERVER_LABEL_key3", "value3")
	os.Setenv("MILVUS_SERVER_LABEL_qn_key3", "value33")
	os.Setenv("MILVUS_SERVER_LABEL_sn_key3", "value33")
	os.Setenv("key4", "value4")
	os.Setenv("MILVUS_SERVER_LABEL_", "value5")
	os.Setenv("MILVUS_SERVER_LABEL_qn", "value6")

	defer os.Unsetenv("MILVUS_SERVER_LABEL_key1")
	defer os.Unsetenv("MILVUS_SERVER_LABEL_key2")
	defer os.Unsetenv("MILVUS_SERVER_LABEL_qn_key3")
	defer os.Unsetenv("MILVUS_SERVER_LABEL_sn_key3")
	defer os.Unsetenv("key4")

	roles := []string{typeutil.QueryNodeRole, typeutil.MixCoordRole, typeutil.StreamingNodeRole, typeutil.ProxyRole}

	for _, role := range roles {
		ret := getServerLabelsFromEnv(role)
		switch role {
		case typeutil.QueryNodeRole, typeutil.StreamingNodeRole:
			assert.Equal(s.T(), 3, len(ret))
			assert.Equal(s.T(), "value1", ret["key1"])
			assert.Equal(s.T(), "value2", ret["key2"])
			assert.Equal(s.T(), "value33", ret["key3"], "role: %s", role)
		default:
			assert.Equal(s.T(), 3, len(ret))
			assert.Equal(s.T(), "value1", ret["key1"])
			assert.Equal(s.T(), "value2", ret["key2"])
			assert.Equal(s.T(), "value3", ret["key3"])
		}
	}
}

func (s *SessionSuite) TestVersionKey() {
	ctx := context.Background()
	session := NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	session.Init(typeutil.MixCoordRole, "normal", false, false)

	session.Register()

	resp, err := s.client.Get(ctx, session.versionKey)
	s.Require().NoError(err)
	s.Equal(1, len(resp.Kvs))
	s.Equal(common.Version.String(), string(resp.Kvs[0].Value))

	common.Version = semver.MustParse("2.5.6")

	s.Panics(func() {
		session2 := NewSessionWithEtcd(ctx, s.metaRoot, s.client)
		session2.Init(typeutil.MixCoordRole, "normal", false, false)
		session2.Register()

		resp, err = s.client.Get(ctx, session2.versionKey)
		s.Require().NoError(err)
		s.Equal(1, len(resp.Kvs))
		s.Equal(common.Version.String(), string(resp.Kvs[0].Value))
	})

	session.Stop()

	common.Version = semver.MustParse("2.6.4")
	session = NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	session.Init(typeutil.MixCoordRole, "normal", false, false)
	session.Register()

	resp, err = s.client.Get(ctx, session.versionKey)
	s.Require().NoError(err)
	s.Equal(1, len(resp.Kvs))
	s.Equal(common.Version.String(), string(resp.Kvs[0].Value))

	session.Stop()

	common.Version = semver.MustParse("2.6.7")
	session = NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	session.Init(typeutil.MixCoordRole, "normal", false, false)
	session.Register()

	resp, err = s.client.Get(ctx, session.versionKey)
	s.Require().NoError(err)
	s.Equal(1, len(resp.Kvs))
	s.Equal(common.Version.String(), string(resp.Kvs[0].Value))
	session.Stop()

	common.Version = semver.MustParse("3.0.0")
	session = NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	session.Init(typeutil.MixCoordRole, "normal", false, false)
	session.Register()

	resp, err = s.client.Get(ctx, session.versionKey)
	s.Require().NoError(err)
	s.Equal(1, len(resp.Kvs))
	s.Equal(common.Version.String(), string(resp.Kvs[0].Value))
}

func (s *SessionSuite) TestSessionLifetime() {
	ctx := context.Background()
	session := NewSessionWithEtcd(ctx, s.metaRoot, s.client)
	session.Init("test", "normal", false, false)
	session.Register()

	resp, err := s.client.Get(ctx, session.getCompleteKey())
	s.Require().NoError(err)
	s.Equal(1, len(resp.Kvs))
	str, err := json.Marshal(session.SessionRaw)
	s.Require().NoError(err)
	s.Equal(string(resp.Kvs[0].Value), string(str))

	ttlResp, err := s.client.Lease.TimeToLive(ctx, *session.LeaseID)
	s.Require().NoError(err)
	s.Greater(ttlResp.TTL, int64(0))

	session.GoingStop()
	resp, err = s.client.Get(ctx, session.getCompleteKey())
	s.Require().True(session.SessionRaw.Stopping)
	s.Require().NoError(err)
	s.Equal(1, len(resp.Kvs))
	str, err = json.Marshal(session.SessionRaw)
	s.Require().NoError(err)
	s.Equal(string(resp.Kvs[0].Value), string(str))

	session.Stop()
	session.wg.Wait()

	resp, err = s.client.Get(ctx, session.getCompleteKey())
	s.Require().NoError(err)
	s.Equal(0, len(resp.Kvs))

	ttlResp, err = s.client.Lease.TimeToLive(ctx, *session.LeaseID)
	s.Require().NoError(err)
	s.Equal(int64(-1), ttlResp.TTL)
}

func TestSessionSuite(t *testing.T) {
	suite.Run(t, new(SessionSuite))
}

func TestForceKill(t *testing.T) {
	if os.Getenv("TEST_EXIT") == "1" {
		testForceKill("testForceKill")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestForceKill") /* #nosec G204 */
	cmd.Env = append(os.Environ(), "TEST_EXIT=1")

	err := cmd.Run()

	// 子进程退出码
	if e, ok := err.(*exec.ExitError); ok {
		if e.ExitCode() != 1 {
			t.Fatalf("expected exit 1, got %d", e.ExitCode())
		}
	} else {
		t.Fatalf("unexpected error: %#v", err)
	}
}

func testForceKill(serverName string) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	session := NewSessionWithEtcd(context.Background(), "test", etcdCli)
	session.Init(serverName, "normal", false, false)
	session.Register()

	// trigger a force kill
	etcdCli.Revoke(context.Background(), *session.LeaseID)
}
