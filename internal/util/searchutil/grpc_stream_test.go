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
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type shutdownClientStream struct {
	viewpb.ViewQueryService_SearchOnViewStreamClient
	ctx        context.Context
	started    chan struct{}
	closeCalls atomic.Int32
	closeErr   error
}

func (s *shutdownClientStream) Recv() (*viewpb.SearchOnViewStreamResponse, error) {
	close(s.started)
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *shutdownClientStream) CloseSend() error {
	s.closeCalls.Add(1)
	return s.closeErr
}

func TestGRPCReduceStreamConcurrentClose(t *testing.T) {
	for _, mode := range []string{"blocked_recv", "starting_recv"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closeErr := errors.New("close send failed")
			client := &shutdownClientStream{ctx: ctx, started: make(chan struct{}), closeErr: closeErr}
			stream := &grpcReduceStream{client: client, cancel: cancel}
			start := make(chan struct{})
			received := make(chan error, 1)
			go func() {
				<-start
				_, err := stream.Recv()
				received <- err
			}()
			if mode == "blocked_recv" {
				close(start)
				select {
				case <-client.started:
				case <-time.After(5 * time.Second):
					t.Fatal("gRPC receive did not start")
				}
			}
			closed := make(chan error, 4)
			for range cap(closed) {
				go func() {
					<-start
					closed <- stream.Close()
				}()
			}
			if mode == "starting_recv" {
				close(start)
			}
			select {
			case err := <-received:
				require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, io.ErrClosedPipe), "%v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("gRPC receive was not canceled")
			}
			for range cap(closed) {
				select {
				case err := <-closed:
					require.ErrorIs(t, err, closeErr)
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent gRPC Close did not finish")
				}
			}
			require.Equal(t, int32(1), client.closeCalls.Load())
			_, err := stream.Recv()
			require.ErrorIs(t, err, io.ErrClosedPipe)
		})
	}
}
