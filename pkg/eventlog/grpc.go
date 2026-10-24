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
	"net"
	"sync"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// grpcLoggerKey is the global-logger key this logger registers under.
const grpcLoggerKey = "grpc_logger"

var (
	grpcLog                 atomic.Pointer[grpcLogger]
	grpcLogMu               sync.Mutex
	grpcListenerModeIsLocal bool
	grpcListenFunc          = net.Listen
)

// grpcLogger is a Logger with dispatches streaming Evt to client listeners.
type grpcLogger struct {
	level     atomic.Int32
	lis       net.Listener
	server    *grpc.Server
	port      int
	localOnly bool

	clients *typeutil.ConcurrentMap[string, *listenerClient]
}

func (l *grpcLogger) SetLevel(lvl Level) {
	l.level.Store(int32(lvl))
}

func (l *grpcLogger) GetLevel() Level {
	return Level(l.level.Load())
}

func (l *grpcLogger) Record(evt Evt) {
	if evt.Level() < Level(l.level.Load()) {
		return
	}

	l.clients.Range(func(key string, client *listenerClient) bool {
		client.Notify(evt)
		return true
	})
}

func (l *grpcLogger) RecordFunc(lvl Level, fn func() Evt) {
	if lvl < l.GetLevel() {
		return
	}

	l.Record(fn())
}

func (l *grpcLogger) Flush() error {
	return nil
}

func (l *grpcLogger) Listen(req *ListenRequest, svr EventLogService_ListenServer) error {
	client := newListenerClient()
	key := funcutil.RandomString(8)
	l.clients.Insert(key, client)
	defer func() {
		client, ok := l.clients.GetAndRemove(key)
		if ok {
			client.Stop()
		}
	}()
	for {
		select {
		case evt := <-client.ch:
			err := svr.Send(&Event{
				Level: evt.Level(),
				Type:  evt.Type(),
				Data:  evt.Raw(),
				Ts:    time.Now().UnixNano(),
			})
			if err != nil {
				return nil
			}
		case <-svr.Context().Done():
			return nil
		case <-client.closed:
			return nil
		}
	}
}

func (l *grpcLogger) Close() {
	if l.server != nil {
		l.server.Stop()
		return
	}
	if l.lis != nil {
		l.lis.Close()
	}
}

func grpcListenAddress(localOnly bool) string {
	if localOnly {
		// The HTTP /eventlog discovery endpoint is protected in secured mode,
		// but the spawned gRPC service has no authentication protocol of its
		// own. Keep that data channel local so learning/scanning the random port
		// cannot bypass the HTTP root-auth gate.
		return "127.0.0.1:0"
	}

	// Flag-off mode intentionally retains the historical remote-listener
	// behavior for compatibility.
	return ":0"
}

// EnsureListenerMode records the configured listener mode and switches an
// already-created listener when necessary. Recording the mode even before
// discovery prevents a request that observed stale configuration from opening
// a wildcard listener after management authentication has been enabled.
func EnsureListenerMode(localOnly bool) error {
	grpcLogMu.Lock()
	defer grpcLogMu.Unlock()

	grpcListenerModeIsLocal = localOnly
	current := grpcLog.Load()
	if current == nil {
		return nil
	}
	if _, err := getGrpcLoggerLocked(localOnly); err != nil {
		if localOnly && !current.localOnly {
			// Asked to secure the listener and could not. Leaving the wildcard
			// one running would mean the caller believes the stream is
			// loopback-only while it is still reachable from the network, so
			// close it: no event stream is the safe answer, and the next
			// discovery request retries.
			current.Close()
			grpcLog.Store(nil)
			getGlobalLogger().Unregister(grpcLoggerKey)
		}
		return err
	}
	return nil
}

// getGrpcLogger starts or returns the singleton grpcLogger, in whatever mode
// EnsureListenerMode last recorded. Until it is called the historical wildcard
// listener is used, which is what an embedder that never configures a mode
// gets.
func getGrpcLogger() (int, error) {
	// Serialize creation and security upgrades. A singleflight keyed only by
	// logger name is insufficient here: concurrent callers wanting different
	// modes can otherwise share the wildcard result and let the secured caller
	// return success while the listener remains remotely reachable.
	grpcLogMu.Lock()
	defer grpcLogMu.Unlock()
	return getGrpcLoggerLocked(grpcListenerModeIsLocal)
}

func getGrpcLoggerLocked(localOnly bool) (int, error) {
	current := grpcLog.Load()
	if current != nil && current.localOnly == localOnly {
		return current.port, nil
	}

	// Bind the replacement before retiring the incumbent. Closing first would
	// mean a failed Listen leaves the process with no event logger at all and a
	// stopped one still registered globally — a worse state than the one we
	// were asked to change.
	lis, err := grpcListenFunc("tcp", grpcListenAddress(localOnly))
	if err != nil {
		return -1, err
	}
	if current != nil {
		// Stop existing streams so dynamic transitions match the configured
		// mode in both directions.
		current.Close()
	}

	port := lis.Addr().(*net.TCPAddr).Port

	svr := grpc.NewServer()
	l := &grpcLogger{
		lis:       lis,
		server:    svr,
		port:      port,
		localOnly: localOnly,
		clients:   typeutil.NewConcurrentMap[string, *listenerClient](),
	}
	l.SetLevel(Level_Debug)
	RegisterEventLogServiceServer(svr, l)
	go svr.Serve(lis)

	grpcLog.Store(l)
	// Replace, not Register: switching mode at runtime has to displace the
	// wildcard logger, and Register keeps whatever is already there.
	getGlobalLogger().Replace(grpcLoggerKey, l)
	return l.port, nil
}

type listenerClient struct {
	ch     chan Evt
	closed chan struct{}
	once   sync.Once
}

func newListenerClient() *listenerClient {
	return &listenerClient{
		ch:     make(chan Evt, 100),
		closed: make(chan struct{}),
	}
}

func (c *listenerClient) Notify(l Evt) {
	select {
	case <-c.closed:
		return
	case c.ch <- l:
	default:
	}
}

func (c *listenerClient) Stop() {
	c.once.Do(func() {
		// Keep ch open: Record may already have loaded this client from the
		// concurrent map and be about to Notify it. Closing the send channel
		// would turn a normal listener shutdown or mode switch into a
		// send-on-closed-channel panic. closed is the ownership signal; queued
		// events may be discarded once the listener exits.
		close(c.closed)
	})
}
