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

package streamingnode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/util/netutil"
)

func TestStartGRPCServerCancelledContextReturnsAndStopsCleanly(t *testing.T) {
	listener, err := netutil.NewListener(netutil.OptIP("127.0.0.1"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		ctx:        ctx,
		cancel:     cancel,
		listener:   listener,
		grpcServer: grpc.NewServer(),
	}

	cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.startGPRCServer(ctx)
	}()

	select {
	case startErr := <-done:
		require.ErrorIs(t, startErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("startGPRCServer did not return after startup context cancellation")
	}

	stopDone := make(chan struct{})
	go func() {
		s.stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not finish after startGPRCServer cancellation")
	}
	assert.NotNil(t, s.grpcServer)
}
