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

package grpcclient

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReadConnectionFailureFastFail(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		err                     error
		enabled, cancel, marked bool
	}{
		{"unavailable", status.Error(codes.Unavailable, "error reading from server: connection timed out"), true, false, true},
		{"legacy caller", status.Error(codes.Unavailable, "unreachable"), false, false, false},
		{"caller canceled", status.Error(codes.Unavailable, "unreachable"), true, true, false},
		{"rpc canceled", status.Error(codes.Canceled, "context canceled"), true, false, false},
		{"rpc deadline", status.Error(codes.DeadlineExceeded, "slow query"), true, false, false},
		{"local close", grpc.ErrClientConnClosing, true, false, false},
		{"node identity", status.Error(codes.Unavailable, merr.ErrNodeNotMatch.Error()), true, false, false},
		{"cross cluster", status.Error(codes.Unavailable, merr.ErrServiceCrossClusterRouting.Error()), true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &ClientBase[*mockClient]{
				grpcClient:  &clientConnWrapper[*mockClient]{client: &mockClient{}},
				MaxAttempts: 1, maxCancelError: 100,
			}
			// Observe redial attempts, without creating a replacement connection.
			dial := mockey.Mock(mockey.GetMethod(base, "connect")).Return(wrapErrConnect("worker", context.Canceled)).Build()
			defer dial.UnPatch()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.enabled {
				ctx = WithFailFastOnConnectionError(ctx)
			}
			calls := 0
			_, err := base.Call(ctx, func(*mockClient) (any, error) {
				calls++
				if tc.cancel {
					cancel()
				}
				return nil, tc.err
			})
			require.Error(t, err)
			require.Equal(t, tc.marked, IsConnectionFailure(err))
			require.Equal(t, 1, calls)
			if tc.marked {
				require.Zero(t, dial.Times(), "do not redial before returning the original failure")
				require.ErrorIs(t, err, tc.err)
				require.Nil(t, base.grpcClient, "retire the failed connection for the next call")
			}
			if tc.name == "legacy caller" {
				require.Positive(t, dial.Times())
			}
		})
	}
}

func TestReadConnectionFailureDial(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cause          error
		cancel, marked bool
	}{
		{"dial deadline", context.DeadlineExceeded, false, true},
		{"parent canceled", context.Canceled, true, false},
		{"local cancel", context.Canceled, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &ClientBase[*mockClient]{MaxAttempts: 1}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			dial := mockey.Mock(mockey.GetMethod(base, "connect")).To(func(context.Context) error {
				if tc.cancel {
					cancel()
				}
				return wrapErrConnect("worker", tc.cause)
			}).Build()
			defer dial.UnPatch()
			_, err := base.Call(WithFailFastOnConnectionError(ctx), func(*mockClient) (any, error) {
				t.Fatal("RPC must not be invoked when dialing failed")
				return nil, nil
			})
			require.Error(t, err)
			require.Equal(t, tc.marked, IsConnectionFailure(err))
			if tc.marked {
				require.Equal(t, 1, dial.Times())
			}
		})
	}
}

func TestReadConnectionFailureLeavesSlowAndApplicationCallsAlone(t *testing.T) {
	base := &ClientBase[*mockClient]{
		grpcClient:  &clientConnWrapper[*mockClient]{client: &mockClient{}},
		MaxAttempts: 1,
	}
	ctx := WithFailFastOnConnectionError(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := base.Call(ctx, func(call *mockClient) (any, error) {
			close(entered)
			<-release
			return merr.Success(), nil
		})
		done <- err
	}()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("pending healthy read was stopped: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-done)
	for _, appErr := range []error{merr.ErrServiceUnavailable, merr.ErrServiceTooManyRequests, merr.ErrParameterInvalid} {
		ret, err := base.Call(ctx, func(*mockClient) (any, error) { return merr.Status(appErr), nil })
		if err == nil {
			err = merr.Error(ret.(*commonpb.Status))
		}
		require.ErrorIs(t, err, appErr)
		require.False(t, IsConnectionFailure(err))
	}
	require.False(t, IsConnectionFailure(errors.Wrap(context.Canceled, "outer")))
}
