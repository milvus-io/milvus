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

package searchutil

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

type shutdownTestStream struct {
	started    chan struct{}
	canceled   chan struct{}
	exited     chan struct{}
	release    <-chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
	closeErr   error
}

func (s *shutdownTestStream) Recv() (*internalpb.SearchResults, error) {
	close(s.started)
	defer close(s.exited)
	<-s.canceled
	<-s.release
	return nil, context.Canceled
}

func (s *shutdownTestStream) Close() error {
	s.closeCalls.Add(1)
	s.closeOnce.Do(func() { close(s.canceled) })
	return s.closeErr
}

func (*shutdownTestStream) Interrupt() (*internalpb.SearchResults, error) {
	return nil, context.Canceled
}

type shutdownErrorStream struct {
	fakeReduceStream
	waitFor <-chan struct{}
}

func (s *shutdownErrorStream) Recv() (*internalpb.SearchResults, error) {
	<-s.waitFor
	return s.fakeReduceStream.Recv()
}

func TestReduceStreamShutdownWaitsForNestedRecv(t *testing.T) {
	for _, siblingFails := range []bool{true, false} {
		name := "concurrent_close"
		if siblingFails {
			name = "sibling_failure"
		}
		t.Run(name, func(t *testing.T) {
			releaseContext, release := context.WithCancel(context.Background())
			defer release()
			closeErr := errors.New("leaf cleanup failed")
			leaf := &shutdownTestStream{
				started:  make(chan struct{}),
				canceled: make(chan struct{}),
				exited:   make(chan struct{}),
				release:  releaseContext.Done(),
				closeErr: closeErr,
			}
			request := &internalpb.SearchRequest{Nq: 1, Topk: 10, MetricType: "IP"}
			nested, err := NewReduceStream(request, []ReduceStream{leaf}, 262144)
			require.NoError(t, err)
			children := []ReduceStream{nested}
			recvErr := errors.New("worker failed")
			if siblingFails {
				children = append(children, &shutdownErrorStream{
					fakeReduceStream: fakeReduceStream{recv: []fakeStreamRecv{{err: recvErr}}},
					waitFor:          leaf.started,
				})
			}
			stream, err := NewReduceStream(request, children, 262144)
			require.NoError(t, err)
			received := make(chan error, 1)
			go func() {
				_, err := stream.Recv()
				received <- err
			}()
			select {
			case <-leaf.started:
			case <-time.After(5 * time.Second):
				t.Fatal("nested child Recv did not start")
			}

			closed := make(chan error, 4)
			if !siblingFails {
				for range cap(closed) {
					go func() { closed <- stream.Close() }()
				}
			}
			select {
			case <-leaf.canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not cancel the nested child")
			}
			select {
			case <-received:
				t.Fatal("Recv returned before its pending child receive exited")
			case <-closed:
				t.Fatal("Close returned before its pending child receive exited")
			default:
			}

			release()
			select {
			case err := <-received:
				if siblingFails {
					require.ErrorIs(t, err, recvErr)
				} else {
					require.ErrorIs(t, err, context.Canceled)
				}
				require.ErrorIs(t, err, closeErr)
			case <-time.After(5 * time.Second):
				t.Fatal("Recv did not exit after cancellation")
			}
			if !siblingFails {
				for range cap(closed) {
					select {
					case err := <-closed:
						require.ErrorIs(t, err, closeErr)
					case <-time.After(5 * time.Second):
						t.Fatal("concurrent Close did not finish")
					}
				}
			}
			select {
			case <-leaf.exited:
			default:
				t.Fatal("shutdown left a child receive running")
			}
			require.ErrorIs(t, stream.Close(), closeErr)
			require.Equal(t, int32(1), leaf.closeCalls.Load())
		})
	}
}
