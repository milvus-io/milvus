// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventlog

import (
	context "context"
	fmt "fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GrpcLoggerSuite struct {
	suite.Suite
	l    *grpcLogger
	port int
}

type localListenerClient struct {
	conn         *grpc.ClientConn
	client       EventLogServiceClient
	listenClient EventLogService_ListenClient
	result       chan *Event
}

func (c *localListenerClient) listen(t *testing.T) {
	for {
		evt, err := c.listenClient.Recv()
		if err != nil {
			return
		}

		select {
		case c.result <- evt:
		default:
		}
	}
}

func (c *localListenerClient) close() {
	if c.conn != nil {
		c.conn.Close()
	}
	if c.result != nil {
		close(c.result)
	}
}

func (s *GrpcLoggerSuite) SetupTest() {
	port, err := getGrpcLogger()
	s.Require().NoError(err)
	s.port = port

	s.l = grpcLog.Load()
	s.Require().NotNil(s.l)
}

func (s *GrpcLoggerSuite) registerClient() *localListenerClient {
	ctx := context.Background()
	addr := fmt.Sprintf("127.0.0.1:%d", s.port)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(time.Second),
	}

	conn, err := grpc.DialContext(ctx, addr, opts...)

	s.Require().NoError(err)

	client := NewEventLogServiceClient(conn)

	listenClient, err := client.Listen(ctx, &ListenRequest{})
	s.Require().NoError(err)

	c := &localListenerClient{
		conn:         conn,
		client:       client,
		listenClient: listenClient,
		result:       make(chan *Event, 100),
	}

	go c.listen(s.T())

	return c
}

func (s *GrpcLoggerSuite) TestRecord() {
	s.Run("normal_case", func() {
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.Record(NewRawEvt(Level_Info, "test"))

		evt := <-c.result
		s.Equal(Level_Info, evt.GetLevel())
		s.EqualValues("test", evt.GetData())

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)
	})

	s.Run("skip_level", func() {
		s.l.SetLevel(Level_Warn)
		defer s.l.SetLevel(Level_Debug)
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.Record(NewRawEvt(Level_Info, "test"))

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)

		var result []*Event
		for evt := range c.result {
			result = append(result, evt)
		}
		s.Equal(0, len(result))
	})
}

func (s *GrpcLoggerSuite) TestRecordFunc() {
	s.Run("normal_case", func() {
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.RecordFunc(Level_Info, func() Evt { return NewRawEvt(Level_Info, "test") })

		evt := <-c.result
		s.Equal(Level_Info, evt.GetLevel())
		s.EqualValues("test", evt.GetData())

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)
	})

	s.Run("skip_level", func() {
		s.l.SetLevel(Level_Warn)
		defer s.l.SetLevel(Level_Debug)
		c := s.registerClient()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 1
		}, time.Second, time.Millisecond*100)

		s.l.RecordFunc(Level_Info, func() Evt { return NewRawEvt(Level_Info, "test") })

		c.close()

		s.Eventually(func() bool {
			return s.l.clients.Len() == 0
		}, time.Second, time.Millisecond*100)

		var result []*Event
		for evt := range c.result {
			result = append(result, evt)
		}
		s.Equal(0, len(result))
	})
}

func (s *GrpcLoggerSuite) TestFlush() {
	s.NoError(s.l.Flush())
}

func TestGrpcListenAddress(t *testing.T) {
	t.Run("legacy mode remains remotely reachable", func(t *testing.T) {
		// Keep the flag-off behavior unchanged for compatibility with existing
		// remote eventlog clients.
		if got := grpcListenAddress(false); got != ":0" {
			t.Fatalf("legacy eventlog listen address = %q, want :0", got)
		}
	})

	t.Run("secured mode is loopback only", func(t *testing.T) {
		// The gRPC stream has no credential exchange, so secured mode must not
		// expose it on a remotely reachable interface after authenticating only
		// the HTTP discovery request.
		if got := grpcListenAddress(true); got != "127.0.0.1:0" {
			t.Fatalf("secured eventlog listen address = %q, want 127.0.0.1:0", got)
		}
	})
}

func TestListenerClientNotifyAfterStop(t *testing.T) {
	client := newListenerClient()
	client.Stop()
	client.Stop()

	// A logger Record may already hold the client after Listen has removed it
	// and called Stop. Notification after that point must be a no-op, not a
	// send-on-closed-channel panic.
	client.Notify(NewRawEvt(Level_Info, "after-stop"))
	select {
	case <-client.closed:
	default:
		t.Fatal("client stop signal was not closed")
	}
}

func resetGrpcLogger() {
	grpcLogMu.Lock()
	defer grpcLogMu.Unlock()
	if logger := grpcLog.Load(); logger != nil {
		logger.Close()
	}
	grpcLog.Store(nil)
	getGlobalLogger().loggers.GetAndRemove("grpc_logger")
	grpcListenerModeIsLocal = false
}

// Discovery must open the listener in whatever mode was configured last, not
// in whatever mode the caller happens to believe is current. There is exactly
// one source of truth for the bind address, and this pins it.
func TestConfiguredListenerModeWinsOverStaleDiscovery(t *testing.T) {
	for _, test := range []struct {
		name           string
		configuredMode bool
	}{
		{name: "enable auth", configuredMode: true},
		{name: "disable auth", configuredMode: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			resetGrpcLogger()
			t.Cleanup(resetGrpcLogger)

			if err := EnsureListenerMode(test.configuredMode); err != nil {
				t.Fatalf("configure eventlog listener mode: %v", err)
			}
			if grpcLog.Load() != nil {
				t.Fatal("configuring listener mode must not open an unused port")
			}

			if _, err := getGrpcLogger(); err != nil {
				t.Fatalf("start eventlog listener: %v", err)
			}
			logger := grpcLog.Load()
			if logger == nil || logger.localOnly != test.configuredMode {
				t.Fatalf("listener localOnly = %v, want configured mode %v", logger != nil && logger.localOnly, test.configuredMode)
			}
		})
	}
}

func TestGrpcLoggerUpgradesToLoopback(t *testing.T) {
	resetGrpcLogger()
	t.Cleanup(resetGrpcLogger)

	_, err := getGrpcLogger()
	if err != nil {
		t.Fatalf("start legacy eventlog listener: %v", err)
	}
	legacy := grpcLog.Load()
	if legacy == nil || legacy.localOnly {
		t.Fatal("legacy eventlog listener should be remotely reachable")
	}

	err = EnsureListenerMode(true)
	if err != nil {
		t.Fatalf("upgrade eventlog listener to loopback: %v", err)
	}
	secured := grpcLog.Load()
	if secured == nil || !secured.localOnly {
		t.Fatal("secured eventlog listener should be loopback-only")
	}
	if secured == legacy {
		t.Fatal("enabling admin auth must replace the existing wildcard listener")
	}
	if !secured.lis.Addr().(*net.TCPAddr).IP.IsLoopback() {
		t.Fatalf("secured eventlog listener bound to %s, want loopback", secured.lis.Addr())
	}
}

func TestGrpcLoggerConcurrentUpgradeStaysLoopbackOnly(t *testing.T) {
	resetGrpcLogger()
	originalListenFunc := grpcListenFunc
	t.Cleanup(func() {
		resetGrpcLogger()
		grpcListenFunc = originalListenFunc
	})

	listenStarted := make(chan struct{})
	releaseFirstListen := make(chan struct{})
	var blockFirstListen sync.Once
	grpcListenFunc = func(network, address string) (net.Listener, error) {
		blockFirstListen.Do(func() {
			close(listenStarted)
			<-releaseFirstListen
		})
		return net.Listen(network, address)
	}

	legacyResult := make(chan error, 1)
	go func() {
		_, err := getGrpcLogger()
		legacyResult <- err
	}()
	<-listenStarted

	securedResult := make(chan error, 1)
	go func() {
		securedResult <- EnsureListenerMode(true)
	}()

	// Give the secured request time to overlap the deliberately blocked legacy
	// start. Under the old singleflight implementation it joined the wildcard
	// creation and incorrectly returned success without upgrading the listener.
	time.Sleep(20 * time.Millisecond)
	close(releaseFirstListen)

	if err := <-legacyResult; err != nil {
		t.Fatalf("start legacy eventlog listener: %v", err)
	}
	if err := <-securedResult; err != nil {
		t.Fatalf("secure eventlog listener: %v", err)
	}

	secured := grpcLog.Load()
	if secured == nil || !secured.localOnly {
		t.Fatal("concurrent security upgrade must leave a loopback-only listener")
	}
	if !secured.lis.Addr().(*net.TCPAddr).IP.IsLoopback() {
		t.Fatalf("secured eventlog listener bound to %s, want loopback", secured.lis.Addr())
	}
}

func TestGrpcLoggerDowngradesToWildcard(t *testing.T) {
	resetGrpcLogger()
	t.Cleanup(resetGrpcLogger)

	if err := EnsureListenerMode(true); err != nil {
		t.Fatalf("configure secured mode: %v", err)
	}
	if _, err := getGrpcLogger(); err != nil {
		t.Fatalf("start secured eventlog listener: %v", err)
	}
	secured := grpcLog.Load()
	if secured == nil || !secured.localOnly {
		t.Fatal("expected loopback listener")
	}

	if err := EnsureListenerMode(false); err != nil {
		t.Fatalf("downgrade eventlog listener: %v", err)
	}
	legacy := grpcLog.Load()
	if legacy == nil || legacy.localOnly {
		t.Fatal("expected wildcard listener after disabling admin auth")
	}
	if legacy == secured {
		t.Fatal("changing listener mode must replace the listener")
	}
	if legacy.lis.Addr().(*net.TCPAddr).IP.IsLoopback() {
		t.Fatal("wildcard listener unexpectedly remained loopback-only")
	}
}

func TestGrpcLogger(t *testing.T) {
	suite.Run(t, new(GrpcLoggerSuite))
}
