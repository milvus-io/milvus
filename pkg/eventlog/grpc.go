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

	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	grpcLog atomic.Pointer[grpcLogger]
	sf      conc.Singleflight[*grpcLogger]
)

// grpcLogger is a Logger with dispatches streaming Evt to client listeners.
type grpcLogger struct {
	level atomic.Int32
	lis   net.Listener
	port  int

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
	if l.lis != nil {
		l.lis.Close()
	}
}

// getGrpcLogger starts or returns the singleton grpcLogger listening port.
func getGrpcLogger() (int, error) {
	if l := grpcLog.Load(); l != nil {
		return l.port, nil
	}
	l, err, _ := sf.Do("grpc_evt_log", func() (*grpcLogger, error) {
		if grpcLog.Load() != nil {
			return grpcLog.Load(), nil
		}
		lis, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, err
		}

		port := lis.Addr().(*net.TCPAddr).Port

		svr := grpc.NewServer()
		l := &grpcLogger{
			lis:     lis,
			port:    port,
			clients: typeutil.NewConcurrentMap[string, *listenerClient](),
		}
		l.SetLevel(Level_Debug)
		RegisterEventLogServiceServer(svr, l)
		go svr.Serve(lis)

		grpcLog.Store(l)
		getGlobalLogger().Register("grpc_logger", l)
		return l, nil
	})
	if err != nil {
		return -1, err
	}

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
