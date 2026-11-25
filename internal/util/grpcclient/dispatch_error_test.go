// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestClientBaseMarksOnlyLocalContextFailures(t *testing.T) {
	base := &ClientBase[rootcoordpb.RootCoordClient]{}
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := base.Call(expired, func(rootcoordpb.RootCoordClient) (any, error) {
		t.Fatal("expired request reached the RPC callback")
		return nil, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, IsRequestNotDispatched(err))

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	base = &ClientBase[rootcoordpb.RootCoordClient]{
		ClientMaxRecvSize: 1024, ClientMaxSendSize: 1024, DialTimeout: time.Second,
		KeepAliveTime: time.Minute, KeepAliveTimeout: time.Minute, MaxAttempts: 1,
	}
	base.SetRole(typeutil.QueryNodeRole)
	base.SetGetAddrFunc(func() (string, error) { return listener.Addr().String(), nil })
	base.SetNewGrpcClientFunc(func(conn *grpc.ClientConn) rootcoordpb.RootCoordClient { return rootcoordpb.NewRootCoordClient(conn) })
	defer base.Close()
	called := false
	_, err = base.Call(context.Background(), func(rootcoordpb.RootCoordClient) (any, error) { called = true; return nil, context.DeadlineExceeded })
	require.True(t, called)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, IsRequestNotDispatched(err), "after callback entry, a timeout cannot be assumed local")
}

func TestClientBaseLocalDeadlineAfterConnectionAcquisition(t *testing.T) {
	base := &ClientBase[rootcoordpb.RootCoordClient]{MaxAttempts: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	acquired := false
	patch := mockey.Mock((*ClientBase[rootcoordpb.RootCoordClient]).GetGrpcClient).To(func(_ *ClientBase[rootcoordpb.RootCoordClient], ctx context.Context) (*clientConnWrapper[rootcoordpb.RootCoordClient], error) {
		acquired = true
		<-ctx.Done()
		return &clientConnWrapper[rootcoordpb.RootCoordClient]{}, nil
	}).Build()
	defer patch.UnPatch()
	_, err := base.Call(ctx, func(rootcoordpb.RootCoordClient) (any, error) {
		t.Fatal("expired request reached the RPC callback")
		return nil, nil
	})
	require.True(t, acquired)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, IsRequestNotDispatched(err))
}
