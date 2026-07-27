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

var (
	grpcLog        atomic.Pointer[grpcLogger]
	grpcLogMu      sync.Mutex
	grpcListenFunc = net.Listen
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

// getGrpcLogger starts or returns the singleton grpcLogger listening port.
func getGrpcLogger() (int, error) {
	return getGrpcLoggerWithLocalOnly(false)
}

// EnsureLocalOnly starts a loopback listener, or replaces an existing wildcard
// listener with one. It is called when management authentication is enabled at
// runtime so a previously discovered eventlog port cannot remain remotely
// reachable until the next HTTP discovery request.
func EnsureLocalOnly() error {
	_, err := getGrpcLoggerWithLocalOnly(true)
	return err
}

// getGrpcLoggerWithLocalOnly starts or returns the singleton grpcLogger. The
// localOnly option is selected by the authenticated HTTP discovery handler;
// the parameter keeps this package independent from Milvus's config package.
func getGrpcLoggerWithLocalOnly(localOnly bool) (int, error) {
	// Serialize creation and security upgrades. A singleflight keyed only by
	// logger name is insufficient here: concurrent localOnly=false/true callers
	// can otherwise share the wildcard result and let the secured caller return
	// success while the listener remains remotely reachable.
	grpcLogMu.Lock()
	defer grpcLogMu.Unlock()

	if current := grpcLog.Load(); current != nil {
		// Enabling the gate at runtime must not leave a listener that was
		// previously opened on all interfaces. A loopback listener is still safe
		// if the flag is later disabled, so no downgrade/rebind is needed.
		if !localOnly || current.localOnly {
			return current.port, nil
		}
		// Stop existing streams as well as the wildcard listener before
		// publishing the secured replacement.
		current.Close()
		grpcLog.Store(nil)
	}

	lis, err := grpcListenFunc("tcp", grpcListenAddress(localOnly))
	if err != nil {
		return -1, err
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
	// Insert (rather than GetOrInsert) is intentional: switching secured mode
	// at runtime replaces the wildcard logger with the loopback-only logger.
	getGlobalLogger().loggers.Insert("grpc_logger", l)
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
	case c.ch <- l:
	default:
	}
}

func (c *listenerClient) Stop() {
	c.once.Do(func() {
		close(c.ch)
	})
}
