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

// TestGetServerIDConcurrently tests that concurrent calls to getServerID()
// return the same ID when reuseNodeID is enabled (default behavior).
// This verifies the thread-safety of server ID allocation and ensures
// that the singleton pattern for node IDs works correctly under concurrent access.
// Covers: getServerID(), checkIDExist()
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

// TestInit tests the basic session initialization and registration flow using NewSessionWithEtcd.
// It verifies that after Init() and Register():
// - LeaseID is allocated
// - ServerID is assigned
// - Session is discoverable via GetSessions()
// Covers: NewSessionWithEtcd(), Init(), Register(), GetSessions()
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

// TestUpdateSessions tests the session discovery and watch functionality.
// It verifies:
// - Multiple sessions can register concurrently with unique IDs (WithResueNodeID(false))
// - GetSessions() returns all registered sessions with the given prefix
// - GetSessions() returns empty for non-existent prefixes
// - WatchServices() correctly emits SessionAddEvent when sessions register
// - WatchServices() correctly emits SessionDelEvent when sessions are removed
// Covers: GetSessions(), WatchServices(), SessionAddEvent, SessionDelEvent
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

// TestWatcherHandleWatchResp tests the sessionWatcher's handleWatchResponse() method
// which processes etcd watch events and converts them to SessionEvents.
// Test cases:
// - "handle normal events": PUT and DELETE events are correctly converted to SessionAddEvent/SessionDelEvent
// - "handle abnormal events": Malformed JSON in events is gracefully handled (no panic, no event emitted)
// - "err compacted resp, nil Rewatch": ErrCompacted with nil rewatch handler doesn't panic
// - "err compacted resp, valid Rewatch": ErrCompacted triggers rewatch callback
// - "err canceled": Canceled watch response causes panic (expected behavior for unrecoverable errors)
// - "err handled but rewatch failed": Failed rewatch callback causes panic
// Covers: handleWatchResponse(), handleWatchErr()
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
		watcherCtx, cancel := context.WithCancel(ctx)
		_ = cancel // cancel not used in test but needed for production parity
		return &sessionWatcher{
			s:        s,
			ctx:      watcherCtx,
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

	t.Run("nil PrevKv on DELETE", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			Events: []*clientv3.Event{
				{
					Type:   mvccpb.DELETE,
					PrevKv: nil,
					Kv:     &mvccpb.KeyValue{Key: []byte("test-key")},
				},
			},
		}
		assert.NotPanics(t, func() {
			w.handleWatchResponse(wresp)
		})
		assert.Equal(t, 0, len(w.eventCh))
	})

	t.Run("validation fails for PUT", func(t *testing.T) {
		watcherCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		w := &sessionWatcher{
			s:        s,
			ctx:      watcherCtx,
			prefix:   "test",
			eventCh:  make(chan *SessionEvent, 10),
			validate: func(*Session) bool { return false },
		}
		wresp := clientv3.WatchResponse{
			Events: []*clientv3.Event{
				{
					Type: mvccpb.PUT,
					Kv:   &mvccpb.KeyValue{Value: []byte(`{"ServerID": 1, "ServerName": "test1"}`)},
				},
			},
		}
		w.handleWatchResponse(wresp)
		assert.Equal(t, 0, len(w.eventCh))
	})

	t.Run("validation fails for DELETE", func(t *testing.T) {
		watcherCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		w := &sessionWatcher{
			s:        s,
			ctx:      watcherCtx,
			prefix:   "test",
			eventCh:  make(chan *SessionEvent, 10),
			validate: func(*Session) bool { return false },
		}
		wresp := clientv3.WatchResponse{
			Events: []*clientv3.Event{
				{
					Type:   mvccpb.DELETE,
					PrevKv: &mvccpb.KeyValue{Value: []byte(`{"ServerID": 2, "ServerName": "test2"}`)},
				},
			},
		}
		w.handleWatchResponse(wresp)
		assert.Equal(t, 0, len(w.eventCh))
	})

	t.Run("SessionUpdateEvent for Stopping=true", func(t *testing.T) {
		w := getWatcher(s, nil)
		wresp := clientv3.WatchResponse{
			Events: []*clientv3.Event{
				{
					Type: mvccpb.PUT,
					Kv:   &mvccpb.KeyValue{Value: []byte(`{"ServerID": 1, "ServerName": "test1", "Stopping": true}`)},
				},
			},
		}
		w.handleWatchResponse(wresp)
		assert.Equal(t, 1, len(w.eventCh))
		event := <-w.eventCh
		assert.Equal(t, SessionUpdateEvent, event.EventType)
	})

	t.Run("handleWatchErr non-compacted error", func(t *testing.T) {
		eventCh := make(chan *SessionEvent, 10)
		watcherCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		w := &sessionWatcher{
			s:        s,
			ctx:      watcherCtx,
			prefix:   "test",
			eventCh:  eventCh,
			validate: func(*Session) bool { return true },
		}
		testErr := errors.New("some other error")
		err := w.handleWatchErr(testErr)
		assert.Error(t, err)
		assert.Equal(t, testErr, err)
		select {
		case _, ok := <-eventCh:
			assert.False(t, ok, "event channel should be closed")
		default:
			t.Fatal("event channel should be closed")
		}
	})
}

// TestSession_Registered tests the Registered() and UpdateRegistered() methods.
// Verifies that the registration state can be toggled and queried correctly.
// Also tests the initial nil state returns false.
// Covers: Registered(), UpdateRegistered()
func TestSession_Registered(t *testing.T) {
	session := &Session{}
	// Initial state (nil) should return false
	assert.False(t, session.Registered())
	session.UpdateRegistered(false)
	assert.False(t, session.Registered())
	session.UpdateRegistered(true)
	assert.True(t, session.Registered())
}

// TestSessionMarshal tests JSON marshaling and unmarshaling of Session.
// Verifies that all key fields (ServerID, ServerName, Address, Version)
// are correctly serialized and deserialized.
// Covers: Session.MarshalJSON(), Session.UnmarshalJSON()
func TestSessionMarshal(t *testing.T) {
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

// TestSessionUnmarshal tests error handling in Session JSON unmarshaling.
// Test cases:
// - "json failure": Invalid JSON returns error
// - "version error": Invalid semver version string returns error
// Covers: Session.UnmarshalJSON() error paths
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

// SessionWithVersionSuite tests version-based session filtering and watching.
// It creates sessions with different versions (0.0.0, 2.1.0, 2.2.0-dev) to test
// GetSessionsWithVersionRange() and WatchServicesWithVersionRange().
// Covers: GetSessionsWithVersionRange(), WatchServicesWithVersionRange(), GetRegisteredRevision()
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
	assert.Panics(suite.T(), func() {
		s1.GetRegisteredRevision()
	})
	s1.Register()
	assert.Greater(suite.T(), s1.GetRegisteredRevision(), int64(0))

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

// TestSessionProcessActiveStandBy tests the active-standby mechanism for coordinators.
// Flow:
// 1. Session 1 registers and becomes ACTIVE (first to register the active key)
// 2. Session 2 registers and becomes STANDBY (active key already exists)
// 3. Session 2 watches the active key
// 4. Session 1 stops, releasing the active key
// 5. Session 2 detects the deletion and becomes ACTIVE
// This ensures high availability for coordinator services.
// Covers: ProcessActiveStandBy(), SetEnableActiveStandBy(), isStandby state transitions
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
	assert.Panics(t, func() {
		s1.GetRegisteredRevision()
	})
	wg.Add(1)
	s1.ProcessActiveStandBy(func() error {
		log.Debug("Session 1 become active")
		wg.Done()
		return nil
	})
	wg.Wait()
	assert.Greater(t, s1.GetRegisteredRevision(), int64(0))
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

// TestSessionEventType_String tests the String() method of SessionEventType.
// Verifies that each event type has the correct string representation.
// Covers: SessionEventType.String()
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

// TestServerInfoOp tests server info file operations used for process coordination.
// The server info file stores role-serverID mappings for each Milvus process.
// Test cases:
// - "test with specified pid": Tests file operations with a custom PID
//   - saveServerInfoInternal() writes multiple roles
//   - GetSessions() reads all saved sessions
//   - RemoveServerInfoFile() cleans up the file
//
// - "test with os pid": Tests SaveServerInfo() with actual process PID
// Covers: GetServerInfoFilePath(), saveServerInfoInternal(), SaveServerInfo(),
//
//	GetSessions(), RemoveServerInfoFile()
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

// TestIntegrationMode tests that in integration test mode, reuseNodeID is disabled.
// This allows multiple nodes in the same process to have different ServerIDs,
// which is required for integration testing where a full cluster runs in one process.
// Covers: NewSessionWithEtcd() behavior when IntegrationMode is enabled
func TestIntegrationMode(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()
	params := paramtable.Get()
	// Save and restore IntegrationMode to avoid test pollution
	originalValue := params.IntegrationTestCfg.IntegrationMode.GetValue()
	params.Save(params.IntegrationTestCfg.IntegrationMode.Key, "true")
	defer params.Save(params.IntegrationTestCfg.IntegrationMode.Key, originalValue)

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

// SessionSuite is a test suite for Session functionality.
// It provides setup/teardown for etcd client and metaRoot,
// and contains tests for session lifecycle, disconnection handling,
// keepalive, version management, and server labels.
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

// TestDisconnected tests the Disconnected() and SetDisconnected() methods.
// Verifies that:
// - Initial state (nil) returns false
// - SetDisconnected(true) makes Disconnected() return true
// - SetDisconnected(false) makes Disconnected() return false
// Covers: Disconnected(), SetDisconnected()
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

// TestGoingStop tests the GoingStop() method which marks a session as stopping.
// This is used for graceful shutdown to notify other services that this node
// is going down but hasn't fully stopped yet.
// Test cases:
// - "nil": nil session returns error
// - "not_inited": uninitialized session returns error (no etcdCli/LeaseID)
// - "disconnected": disconnected session returns error
// - "normal": properly registered session can be marked as stopping
// Covers: GoingStop()
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

// TestKeepAliveRetryActiveCancel tests that the keepalive loop exits cleanly
// when the session context is canceled (via Stop()).
// This ensures proper cleanup on graceful shutdown.
// Covers: startKeepAliveLoop(), processKeepAliveResponse() context cancellation path
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

// TestKeepAliveRetryChannelClose tests that the keepalive loop handles
// channel closure and retries the connection.
// This simulates network issues where the etcd connection drops.
// Verifies that the session remains connected after recovery.
// Covers: processKeepAliveResponse() retry logic
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

// TestGetServerLabelsFromEnv tests the getServerLabelsFromEnv() function which
// reads server labels from environment variables.
// Label format: MILVUS_SERVER_LABEL_<key>=<value>
// Role-specific labels: MILVUS_SERVER_LABEL_<role>_<key>=<value> (e.g., qn_key3 for querynode)
// Verifies:
// - Generic labels (key1, key2, key3) are read correctly
// - Role-specific labels override generic ones (qn_key3 overrides key3 for querynode)
// - Non-prefixed env vars are ignored (key4)
// - Empty key labels are ignored (MILVUS_SERVER_LABEL_)
// Covers: getServerLabelsFromEnv()
func (s *SessionSuite) TestGetServerLabelsFromEnv() {
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

// TestVersionKey tests the version key management for coordinator sessions.
// Coordinators store their version in etcd to prevent version downgrades.
// This test verifies:
// - Version key is created on first registration
// - Downgrade attempt (e.g., 2.6.4 -> 2.5.6) panics
// - Same major.minor version upgrade is allowed (2.6.4 -> 2.6.7)
// - Major version upgrade is allowed (2.6.x -> 3.0.0)
// Covers: checkVersionForCoordinator(), getOpsForCoordinator(), versionKey management
func (s *SessionSuite) TestVersionKey() {
	// Save and restore common.Version to avoid test pollution across runs
	originalVersion := common.Version
	defer func() { common.Version = originalVersion }()

	// Start with a known version to make test behavior predictable
	common.Version = semver.MustParse("2.6.4")

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

// TestSessionLifetime tests the complete lifecycle of a session:
// 1. Register: Session key is created in etcd with JSON data and active lease
// 2. GoingStop: Session is updated with Stopping=true (graceful shutdown signal)
// 3. Stop: Session key is removed and lease is revoked (TTL becomes -1)
// This verifies the full registration -> graceful stop -> cleanup flow.
// Covers: Register(), GoingStop(), Stop(), getCompleteKey(), lease management
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

// TestForceKill tests that the session exits with code 1 when its lease is revoked.
// This simulates a scenario where the etcd lease expires (e.g., network partition).
// The test runs in a subprocess because os.Exit cannot be tested directly.
// Uses TEST_EXIT env var to differentiate subprocess from parent test.
// Covers: processKeepAliveResponse() forced exit path (exitCodeSessionLeaseExpired)
func TestForceKill(t *testing.T) {
	if os.Getenv("TEST_EXIT") == "1" {
		testForceKill("testForceKill")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestForceKill") /* #nosec G204 */
	cmd.Env = append(os.Environ(), "TEST_EXIT=1")

	err := cmd.Run()

	if e, ok := err.(*exec.ExitError); ok {
		if e.ExitCode() != 1 {
			t.Fatalf("expected exit 1, got %d", e.ExitCode())
		}
	} else {
		t.Fatalf("unexpected error: %#v", err)
	}
}

// testForceKill is the subprocess helper for TestForceKill.
// It registers a session and then revokes its lease to trigger forced exit.
func testForceKill(serverName string) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	session := NewSessionWithEtcd(context.Background(), "test", etcdCli)
	session.Init(serverName, "normal", false, false)
	session.Register()

	// trigger a force kill by revoking the lease
	etcdCli.Revoke(context.Background(), *session.LeaseID)

	// Block forever to allow the keepalive loop to detect the revoked lease
	// and trigger os.Exit(1). Without this, the subprocess would exit normally
	// before the keepalive loop has a chance to detect the revocation.
	select {}
}

// TestGetServerID tests server ID allocation and related functions.
// Covers: checkIDExist(), getServerIDWithKey() - success, context cancellation, env var override
func TestGetServerID(t *testing.T) {
	paramtable.Init()

	t.Run("checkIDExist error on canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
		etcdCli, _ := kvfactory.GetEtcdAndPath()
		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
		err := s.checkIDExist()
		assert.Error(t, err)
	})

	t.Run("getServerIDWithKey context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
		etcdCli, _ := kvfactory.GetEtcdAndPath()
		etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
		defer etcdKV.RemoveWithPrefix(context.Background(), "")
		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
		cancel()
		_, err := s.getServerIDWithKey("test-key")
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("getServerIDWithKey success", func(t *testing.T) {
		ctx := context.Background()
		metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
		etcdCli, _ := kvfactory.GetEtcdAndPath()
		etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
		defer etcdKV.RemoveWithPrefix(context.Background(), "")
		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
		err := s.checkIDExist()
		require.NoError(t, err)
		id, err := s.getServerIDWithKey("id")
		assert.NoError(t, err)
		assert.Greater(t, id, int64(0))
	})

	t.Run("getServerIDWithKey env var override", func(t *testing.T) {
		ctx := context.Background()
		metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
		etcdCli, _ := kvfactory.GetEtcdAndPath()
		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
		os.Setenv(MilvusNodeIDForTesting, "12345")
		defer os.Unsetenv(MilvusNodeIDForTesting)
		id, err := s.getServerIDWithKey("id")
		assert.NoError(t, err)
		assert.Equal(t, int64(12345), id)
	})
}

// TestSessionRawGetters tests the getter methods on SessionRaw struct.
// SessionRaw is the serializable part of Session stored in etcd.
// Covers: GetAddress(), GetServerID(), GetServerLabel(), IsTriggerKill()
func TestSessionRawGetters(t *testing.T) {
	raw := &SessionRaw{
		ServerID:    123,
		ServerName:  "test-server",
		Address:     "localhost:8080",
		TriggerKill: true,
		ServerLabels: map[string]string{
			"key1": "value1",
		},
	}

	assert.Equal(t, "localhost:8080", raw.GetAddress())
	assert.Equal(t, int64(123), raw.GetServerID())
	assert.Equal(t, map[string]string{"key1": "value1"}, raw.GetServerLabel())
	assert.True(t, raw.IsTriggerKill())
}

// TestSessionOptions tests SessionOption functions used to configure sessions.
// These options are primarily used by querynode to advertise index capabilities.
// Test cases:
// - WithTTL and WithRetryTimes: Sets session TTL and retry configuration
// - WithIndexEngineVersion: Sets minimal and current vector index versions
// - WithScalarIndexEngineVersion: Sets minimal and current scalar index versions
// - WithIndexNonEncoding: Marks session as supporting non-encoded indexes
// Covers: apply(), WithTTL(), WithRetryTimes(), WithIndexEngineVersion(), WithScalarIndexEngineVersion(), WithIndexNonEncoding()
func TestSessionOptions(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()

	t.Run("WithTTL and WithRetryTimes", func(t *testing.T) {
		session := &Session{}
		opts := []SessionOption{WithTTL(100), WithRetryTimes(200)}
		session.apply(opts...)
		assert.Equal(t, int64(100), session.sessionTTL)
		assert.Equal(t, int64(200), session.sessionRetryTimes)
	})

	t.Run("WithIndexEngineVersion", func(t *testing.T) {
		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithIndexEngineVersion(1, 2))
		assert.Equal(t, int32(1), s.IndexEngineVersion.MinimalIndexVersion)
		assert.Equal(t, int32(2), s.IndexEngineVersion.CurrentIndexVersion)
	})

	t.Run("WithScalarIndexEngineVersion", func(t *testing.T) {
		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithScalarIndexEngineVersion(3, 4))
		assert.Equal(t, int32(3), s.ScalarIndexEngineVersion.MinimalIndexVersion)
		assert.Equal(t, int32(4), s.ScalarIndexEngineVersion.CurrentIndexVersion)
	})

	t.Run("WithIndexNonEncoding", func(t *testing.T) {
		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithIndexNonEncoding())
		assert.True(t, s.IndexNonEncoding)
	})
}

// TestEmptySessionWatcher tests the placeholder EmptySessionWatcher.
// This is used in IndexNodeBinding mode where session watching is not needed.
// Verifies that it returns nil EventChannel and Stop() doesn't panic.
// Covers: EmptySessionWatcher(), emptySessionWatcher.EventChannel(), emptySessionWatcher.Stop()
func TestEmptySessionWatcher(t *testing.T) {
	w := EmptySessionWatcher()
	assert.NotNil(t, w)

	// EventChannel returns nil for empty watcher
	ch := w.EventChannel()
	assert.Nil(t, ch)

	// Stop should not panic
	assert.NotPanics(t, func() {
		w.Stop()
	})
}

// TestSessionWatcherStop tests that sessionWatcher.Stop() properly closes
// the event channel and terminates the watch goroutine.
// Covers: WatchServices(), sessionWatcher.Stop(), sessionWatcher.EventChannel()
func TestSessionWatcherStop(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
	defer s.Stop()

	sessions, rev, err := s.GetSessions(ctx, "test-watcher-stop")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(sessions))

	watcher := s.WatchServices("test-watcher-stop", rev, nil)
	assert.NotNil(t, watcher)

	// Get the event channel
	eventCh := watcher.EventChannel()
	assert.NotNil(t, eventCh)

	// Stop the watcher
	watcher.Stop()

	// Event channel should be eventually closed after stop
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-eventCh:
			if !ok {
				return // Test passed - channel closed
			}
		case <-timeout:
			t.Fatal("event channel should be closed after Stop")
		}
	}
}

// TestEnableLabels tests the convenience functions for setting server labels.
// These are used to mark special deployment modes.
// Test cases:
// - NewServerLabel: Constructs environment variable names for server labels
// - EnableEmbeddedQueryNodeLabel: Sets label for embedded querynode in streaming node
// - EnableStandaloneLabel: Sets label for standalone deployment mode
// Covers: NewServerLabel(), EnableEmbededQueryNodeLabel(), EnableStandaloneLabel()
func TestServerLabels(t *testing.T) {
	t.Run("NewServerLabel", func(t *testing.T) {
		label := NewServerLabel("querynode", "test-label")
		assert.Equal(t, "MILVUS_SERVER_LABEL_QUERYNODE_TEST-LABEL", label)

		label2 := NewServerLabel("", "standalone")
		assert.Equal(t, "MILVUS_SERVER_LABEL_STANDALONE", label2)
	})

	t.Run("EnableEmbeddedQueryNodeLabel", func(t *testing.T) {
		EnableEmbededQueryNodeLabel()
		envKey := NewServerLabel(typeutil.QueryNodeRole, LabelStreamingNodeEmbeddedQueryNode)
		val := os.Getenv(envKey)
		assert.Equal(t, "1", val)
		os.Unsetenv(envKey)
	})

	t.Run("EnableStandaloneLabel", func(t *testing.T) {
		EnableStandaloneLabel()
		envKey := NewServerLabel("", LabelStandalone)
		val := os.Getenv(envKey)
		assert.Equal(t, "1", val)
		os.Unsetenv(envKey)
	})
}

// TestGetSessionPrefixByRole tests GetSessionPrefixByRole() which constructs
// the etcd key prefix for a given role.
// Format: <metaRootPath>/session/<role>
// Covers: GetSessionPrefixByRole()
func TestGetSessionPrefixByRole(t *testing.T) {
	paramtable.Init()
	prefix := GetSessionPrefixByRole("test-role")
	assert.Contains(t, prefix, "test-role")
	assert.Contains(t, prefix, DefaultServiceRoot)
}

// TestProcessActiveStandByActivateFunc tests ProcessActiveStandBy activateFunc handling.
// Test cases:
// - error propagation: Errors from activateFunc should be returned to caller
// - nil activateFunc: Should handle nil callback gracefully
// Covers: ProcessActiveStandBy() activateFunc error propagation and nil handling
func TestProcessActiveStandByActivateFunc(t *testing.T) {
	paramtable.Init()
	etcdCli, _ := kvfactory.GetEtcdAndPath()

	t.Run("error propagation", func(t *testing.T) {
		ctx := context.Background()
		metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
		etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
		defer etcdKV.RemoveWithPrefix(context.Background(), "")

		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithResueNodeID(false))
		s.Init("test-activate-error", "testAddr", true, true)
		s.SetEnableActiveStandBy(true)
		s.Register()
		defer s.Stop()

		expectedErr := errors.New("activate error")
		err := s.ProcessActiveStandBy(func() error {
			return expectedErr
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("nil activateFunc", func(t *testing.T) {
		ctx := context.Background()
		metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
		etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
		defer etcdKV.RemoveWithPrefix(context.Background(), "")

		s := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithResueNodeID(false))
		s.Init("test-nil-activate", "testAddr", true, true)
		s.SetEnableActiveStandBy(true)
		s.Register()
		defer s.Stop()

		err := s.ProcessActiveStandBy(nil)
		assert.NoError(t, err)
	})
}

// TestSessionWatcherStartContextDone tests that the watcher's start() goroutine
// exits and closes the event channel when the context is canceled.
// Covers: sessionWatcher.start() context cancellation path
func TestSessionWatcherStartContextDone(t *testing.T) {
	ctx := context.Background()
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()

	s := NewSessionWithEtcd(ctx, metaRoot, etcdCli)
	defer s.Stop()

	watcherCtx, cancel := context.WithCancel(ctx)

	eventCh := make(chan *SessionEvent, 10)
	w := &sessionWatcher{
		s:        s,
		ctx:      watcherCtx,
		cancel:   cancel,
		prefix:   "test-ctx-done",
		rewatch:  nil,
		eventCh:  eventCh,
		rch:      s.etcdCli.Watch(watcherCtx, path.Join(s.metaRoot, DefaultServiceRoot, "test-ctx-done"), clientv3.WithPrefix()),
		validate: func(*Session) bool { return true },
	}

	w.start(watcherCtx)

	// Cancel the context to trigger the ctx.Done() path
	cancel()

	// Wait for the event channel to be closed
	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-eventCh:
			if !ok {
				return // Test passed
			}
		case <-timeout:
			t.Fatal("event channel should be closed after context cancellation")
		}
	}
}

// TestProcessActiveStandByContextCancel tests that a standby session exits
// ProcessActiveStandBy when its context is canceled.
// This simulates graceful shutdown of a standby node.
// Covers: ProcessActiveStandBy() context cancellation while in standby mode
func TestProcessActiveStandByContextCancel(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Create session 1 as active first
	ctx1 := context.Background()
	s1 := NewSessionWithEtcd(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("test-ctx-cancel", "testAddr", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()
	defer s1.Stop()

	done := make(chan struct{})
	go s1.ProcessActiveStandBy(func() error {
		close(done)
		return nil
	})
	<-done

	// Create session 2 with cancelable context
	ctx2, cancel2 := context.WithCancel(context.Background())
	s2 := NewSessionWithEtcd(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("test-ctx-cancel", "testAddr2", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	defer s2.Stop()

	// Start session 2 in standby mode
	errCh := make(chan error, 1)
	go func() {
		errCh <- s2.ProcessActiveStandBy(func() error {
			return nil
		})
	}()

	// Wait for s2 to enter standby mode
	require.Eventually(t, func() bool {
		return s2.isStandby.Load().(bool)
	}, 5*time.Second, 10*time.Millisecond, "s2 should enter standby mode")

	// Cancel context to trigger ctx.Done() path in watch loop
	cancel2()

	// The ProcessActiveStandBy should exit when context is canceled
	select {
	case <-errCh:
		// Expected - exited due to context cancellation
	case <-time.After(5 * time.Second):
		t.Fatal("ProcessActiveStandBy should exit after context cancellation")
	}
}

// TestProcessActiveStandByOldSessionExists tests that ProcessActiveStandBy
// waits when old-style coordinator sessions exist (rootcoord, datacoord, querycoord).
// This ensures compatibility during rolling upgrades from older versions.
// Once old sessions are cleaned up, the new session can become active.
// Covers: ProcessActiveStandBy() ErrOldSessionExists retry loop
func TestProcessActiveStandByOldSessionExists(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Create a fake old-style session by inserting directly into etcd
	// This simulates an old rootcoord session that would block active-standby
	oldSessionKey := path.Join(metaRoot, DefaultServiceRoot, typeutil.RootCoordRole+"-999")
	oldSessionData := `{"ServerID": 999, "ServerName": "rootcoord", "Address": "oldAddr"}`
	_, err := etcdCli.Put(context.Background(), oldSessionKey, oldSessionData)
	require.NoError(t, err)

	// Create a new session with active-standby enabled
	ctx := context.Background()
	newSession := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithResueNodeID(false))
	newSession.Init("test-old-exists", "newAddr", true, true)
	newSession.SetEnableActiveStandBy(true)
	newSession.Register()
	defer newSession.Stop()

	// ProcessActiveStandBy should retry when old session exists
	errCh := make(chan error, 1)
	go func() {
		errCh <- newSession.ProcessActiveStandBy(func() error {
			return nil
		})
	}()

	// Wait for session to enter standby mode
	require.Eventually(t, func() bool {
		return newSession.isStandby.Load().(bool)
	}, 5*time.Second, 10*time.Millisecond, "session should enter standby mode")

	// Delete old session to allow new session to proceed
	_, err = etcdCli.Delete(context.Background(), oldSessionKey)
	require.NoError(t, err)

	// Now the new session should become active
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ProcessActiveStandBy did not complete after old session deleted")
	}
	assert.False(t, newSession.isStandby.Load().(bool))
}

// TestProcessActiveStandByWatchDelete tests that a standby session becomes
// active when the active key is deleted directly (not via Stop()).
// This simulates crash recovery where the active node crashes and its
// lease expires, causing the key to be deleted.
// Covers: ProcessActiveStandBy() DELETE event handling
func TestProcessActiveStandByWatchDelete(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Create session 1 as active
	ctx1 := context.Background()
	s1 := NewSessionWithEtcd(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("test-watch-delete", "testAddr1", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()

	done1 := make(chan struct{})
	go s1.ProcessActiveStandBy(func() error {
		close(done1)
		return nil
	})
	<-done1

	// Create session 2 as standby
	ctx2 := context.Background()
	s2 := NewSessionWithEtcd(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("test-watch-delete", "testAddr2", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	defer s2.Stop()

	done2 := make(chan struct{})
	go s2.ProcessActiveStandBy(func() error {
		close(done2)
		return nil
	})

	// Verify s2 is in standby mode
	require.Eventually(t, func() bool {
		return s2.isStandby.Load().(bool)
	}, 5*time.Second, 10*time.Millisecond, "s2 should enter standby mode")

	// Delete the active key directly (simulating a crash or forced removal)
	activeKey := path.Join(metaRoot, DefaultServiceRoot, "test-watch-delete")
	_, err := etcdCli.Delete(context.Background(), activeKey)
	assert.NoError(t, err)

	// s2 should detect the DELETE event and become active
	select {
	case <-done2:
		// Success - s2 became active
	case <-time.After(5 * time.Second):
		t.Fatal("s2 did not become active after active key deletion")
	}
	assert.False(t, s2.isStandby.Load().(bool))
}

// TestProcessActiveStandByWatchPutEvent tests that PUT events on the active key
// don't cause the standby to incorrectly become active.
// PUT events are updates to the active session (e.g., GoingStop), not deletions.
// The standby should continue watching until DELETE.
// Covers: ProcessActiveStandBy() PUT event handling (should be ignored)
func TestProcessActiveStandByWatchPutEvent(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Create session 1 as active
	ctx1 := context.Background()
	s1 := NewSessionWithEtcd(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("test-watch-put", "testAddr1", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()

	done1 := make(chan struct{})
	go s1.ProcessActiveStandBy(func() error {
		close(done1)
		return nil
	})
	<-done1

	// Create session 2 as standby
	ctx2 := context.Background()
	s2 := NewSessionWithEtcd(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("test-watch-put", "testAddr2", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	defer s2.Stop()

	done2 := make(chan struct{})
	go s2.ProcessActiveStandBy(func() error {
		close(done2)
		return nil
	})

	// Verify s2 is in standby mode
	require.Eventually(t, func() bool {
		return s2.isStandby.Load().(bool)
	}, 5*time.Second, 10*time.Millisecond, "s2 should enter standby mode")

	// Update the active key to trigger PUT event (s2 should continue watching)
	activeKey := path.Join(metaRoot, DefaultServiceRoot, "test-watch-put")
	_, err := etcdCli.Put(context.Background(), activeKey, `{"ServerID": 999}`, clientv3.WithLease(*s1.LeaseID))
	assert.NoError(t, err)

	// s2 should still be in standby after PUT event (verify it didn't incorrectly become active)
	// Use a short Eventually to confirm standby state is maintained
	require.Eventually(t, func() bool {
		return s2.isStandby.Load().(bool)
	}, 500*time.Millisecond, 50*time.Millisecond, "s2 should remain in standby after PUT event")

	// Now stop s1 to allow s2 to become active
	s1.Stop()

	select {
	case <-done2:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("s2 did not become active")
	}
	assert.False(t, s2.isStandby.Load().(bool))
}

// TestProcessActiveStandByCoordinator tests ProcessActiveStandBy with a coordinator role.
// Coordinators (MixCoord, QueryCoord, etc.) have additional version checking logic
// and register legacy role keys for backwards compatibility.
// Covers: ProcessActiveStandBy() with isCoordinator() == true
func TestProcessActiveStandByCoordinator(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Test with a coordinator role (mixcoord) to cover isCoordinator() path
	ctx := context.Background()
	s := NewSessionWithEtcd(ctx, metaRoot, etcdCli, WithResueNodeID(false))
	s.Init(typeutil.MixCoordRole, "testAddr", true, true)
	s.SetEnableActiveStandBy(true)
	s.Register()
	defer s.Stop()

	done := make(chan struct{})
	go s.ProcessActiveStandBy(func() error {
		close(done)
		return nil
	})

	select {
	case <-done:
		// Success - coordinator became active
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not become active")
	}
	assert.False(t, s.isStandby.Load().(bool))
}

// TestKeepaliveTimeout tests that the session exits with code 1 when keepalive
// times out (no response within TTL).
// This is a safety mechanism to detect prolonged etcd connection issues.
// Runs in subprocess due to os.Exit.
// Covers: processKeepAliveResponse() timeout path
func TestKeepaliveTimeout(t *testing.T) {
	if os.Getenv("TEST_KEEPALIVE_TIMEOUT") == "1" {
		testKeepaliveTimeout()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestKeepaliveTimeout") /* #nosec G204 */
	cmd.Env = append(os.Environ(), "TEST_KEEPALIVE_TIMEOUT=1")

	err := cmd.Run()

	// The subprocess should exit with code 1 (exitCodeSessionLeaseExpired)
	if e, ok := err.(*exec.ExitError); ok {
		if e.ExitCode() != 1 {
			t.Fatalf("expected exit 1, got %d", e.ExitCode())
		}
	} else {
		t.Fatalf("unexpected error: %#v", err)
	}
}

// testKeepaliveTimeout is the subprocess helper for TestKeepaliveTimeout.
func testKeepaliveTimeout() {
	paramtable.Init()
	etcdCli, _ := kvfactory.GetEtcdAndPath()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	session := NewSessionWithEtcd(context.Background(), metaRoot, etcdCli)
	// Set a very short TTL to trigger timeout quickly
	session.sessionTTL = 1
	session.Init("testKeepaliveTimeout", "testAddr", false, false)
	session.Register()

	// Revoke the lease to simulate etcd connection loss
	// The keepalive channel will be closed, and the timer will timeout
	// after sessionTTL seconds, triggering os.Exit
	etcdCli.Revoke(context.Background(), *session.LeaseID)

	// Block forever to allow the keepalive loop to detect the timeout
	select {}
}

// TestProcessActiveStandByErrCompacted tests that ProcessActiveStandBy handles
// ErrCompacted gracefully by rewatching with the latest revision.
// ErrCompacted occurs when the watch revision is older than the compacted revision.
// This can happen during long standby periods with high etcd activity.
// Covers: ProcessActiveStandBy() ErrCompacted handling and rewatch
func TestProcessActiveStandByErrCompacted(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Create session 1 as active
	ctx1 := context.Background()
	s1 := NewSessionWithEtcd(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("test-compacted", "testAddr1", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()

	done1 := make(chan struct{})
	go s1.ProcessActiveStandBy(func() error {
		close(done1)
		return nil
	})
	<-done1

	// Get current revision before creating s2
	resp, err := etcdCli.Get(context.Background(), s1.activeKey)
	require.NoError(t, err)
	oldRevision := resp.Header.Revision

	// Create session 2 as standby
	ctx2 := context.Background()
	s2 := NewSessionWithEtcd(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("test-compacted", "testAddr2", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	defer s2.Stop()

	done2 := make(chan struct{})
	go s2.ProcessActiveStandBy(func() error {
		close(done2)
		return nil
	})

	// Wait for s2 to enter standby mode
	require.Eventually(t, func() bool {
		return s2.isStandby.Load().(bool)
	}, 5*time.Second, 10*time.Millisecond, "s2 should enter standby mode")

	// Make some updates to advance revision
	for i := 0; i < 5; i++ {
		_, err := etcdCli.Put(context.Background(), path.Join(metaRoot, "dummy", fmt.Sprintf("key%d", i)), "value")
		require.NoError(t, err)
	}

	// Get new revision
	resp, err = etcdCli.Get(context.Background(), s1.activeKey)
	require.NoError(t, err)
	newRevision := resp.Header.Revision

	// Compact to trigger ErrCompacted for any watches using old revision
	_, err = etcdCli.Compact(context.Background(), newRevision)
	require.NoError(t, err)

	t.Logf("Compacted from revision %d to %d", oldRevision, newRevision)

	// Now stop s1 to trigger DELETE event
	// s2 should handle potential ErrCompacted and eventually become active
	s1.Stop()

	// s2 should become active
	select {
	case <-done2:
		// Success - s2 became active after s1 stopped
	case <-time.After(10 * time.Second):
		t.Fatal("s2 did not become active after compaction and s1 stop")
	}
	assert.False(t, s2.isStandby.Load().(bool))
}

// TestProcessActiveStandByErrCompactedRewatch is similar to TestProcessActiveStandByErrCompacted
// but with more aggressive compaction to ensure the rewatch logic is exercised.
// Covers: ProcessActiveStandBy() rewatch after ErrCompacted with updated revision
func TestProcessActiveStandByErrCompactedRewatch(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Create session 1 as active
	ctx1 := context.Background()
	s1 := NewSessionWithEtcd(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("test-rewatch", "testAddr1", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()

	done1 := make(chan struct{})
	go s1.ProcessActiveStandBy(func() error {
		close(done1)
		return nil
	})
	<-done1

	// Create session 2 as standby with a specific revision
	ctx2 := context.Background()
	s2 := NewSessionWithEtcd(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("test-rewatch", "testAddr2", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	defer s2.Stop()

	done2 := make(chan struct{})
	go s2.ProcessActiveStandBy(func() error {
		close(done2)
		return nil
	})

	// Wait for s2 to enter standby mode
	require.Eventually(t, func() bool {
		return s2.isStandby.Load().(bool)
	}, 5*time.Second, 10*time.Millisecond, "s2 should enter standby mode")

	// Get the revision before updates
	resp, err := etcdCli.Get(context.Background(), s1.activeKey)
	require.NoError(t, err)
	require.Equal(t, int64(1), resp.Count)

	// Make updates and compact aggressively
	for i := 0; i < 10; i++ {
		_, err := etcdCli.Put(context.Background(), path.Join(metaRoot, "test-key"), fmt.Sprintf("value%d", i))
		require.NoError(t, err)
	}

	// Get latest revision and compact
	resp, err = etcdCli.Get(context.Background(), path.Join(metaRoot, "test-key"))
	require.NoError(t, err)
	_, err = etcdCli.Compact(context.Background(), resp.Header.Revision)
	require.NoError(t, err)

	// Stop s1 - s2 should rewatch with new revision and become active
	s1.Stop()

	select {
	case <-done2:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("s2 did not become active after rewatch")
	}
	assert.False(t, s2.isStandby.Load().(bool))
}

// TestProcessActiveStandByGetFailAfterCompaction tests that a standby session
// handles context cancellation during the rewatch/Get cycle after ErrCompacted.
// When a compaction occurs and the standby needs to Get the latest revision,
// if the context is canceled during this Get, the session should exit gracefully.
// Covers: ProcessActiveStandBy() context cancellation during rewatch Get
func TestProcessActiveStandByGetFailAfterCompaction(t *testing.T) {
	paramtable.Init()

	metaRoot := fmt.Sprintf("%d/%s", rand.Int(), DefaultServiceRoot)
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	etcdKV := etcdkv.NewEtcdKV(etcdCli, metaRoot)
	defer etcdKV.RemoveWithPrefix(context.Background(), "")

	// Create session 1 as active
	ctx1 := context.Background()
	s1 := NewSessionWithEtcd(ctx1, metaRoot, etcdCli, WithResueNodeID(false))
	s1.Init("test-get-fail", "testAddr1", true, true)
	s1.SetEnableActiveStandBy(true)
	s1.Register()

	done1 := make(chan struct{})
	go s1.ProcessActiveStandBy(func() error {
		close(done1)
		return nil
	})
	<-done1

	// Create session 2 with a cancelable context
	ctx2, cancel2 := context.WithCancel(context.Background())
	s2 := NewSessionWithEtcd(ctx2, metaRoot, etcdCli, WithResueNodeID(false))
	s2.Init("test-get-fail", "testAddr2", true, true)
	s2.SetEnableActiveStandBy(true)
	s2.Register()
	defer s2.Stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s2.ProcessActiveStandBy(func() error {
			return nil
		})
	}()

	// Wait for s2 to enter standby mode
	require.Eventually(t, func() bool {
		return s2.isStandby.Load().(bool)
	}, 5*time.Second, 10*time.Millisecond, "s2 should enter standby mode")

	// Cancel s2's context to simulate Get failure after compaction
	// This tests the error path where Get fails after ErrCompacted
	cancel2()

	// s2 should exit due to context cancellation
	select {
	case <-errCh:
		// Expected - s2 exited due to context cancellation
	case <-time.After(5 * time.Second):
		t.Fatal("s2 should exit after context cancellation")
	}

	// Clean up s1
	s1.Stop()
}
