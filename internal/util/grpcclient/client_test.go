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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientBase_SetRole(t *testing.T) {
	base := ClientBase{}
	expect := "abc"
	base.SetRole("abc")
	assert.Equal(t, expect, base.GetRole())
}

func TestClientBase_GetRole(t *testing.T) {
	base := ClientBase{}
	assert.Equal(t, "", base.GetRole())
}

func TestClientBase_connect(t *testing.T) {
	t.Run("failed to connect", func(t *testing.T) {
		base := ClientBase{
			getAddrFunc: func() (string, error) {
				return "", nil
			},
			DialTimeout: time.Millisecond,
		}
		err := base.connect(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrConnect))
	})

	t.Run("failed to get addr", func(t *testing.T) {
		errMock := errors.New("mocked")
		base := ClientBase{
			getAddrFunc: func() (string, error) {
				return "", errMock
			},
			DialTimeout: time.Millisecond,
		}
		err := base.connect(context.Background())
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
	})
}

func TestClientBase_Call(t *testing.T) {
	// mock client with nothing
	base := ClientBase{}
	base.grpcClientMtx.Lock()
	base.grpcClient = struct{}{}
	base.grpcClientMtx.Unlock()

	t.Run("Call normal return", func(t *testing.T) {
		_, err := base.Call(context.Background(), func(client interface{}) (interface{}, error) {
			return struct{}{}, nil
		})
		assert.NoError(t, err)
	})

	t.Run("Call with canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := base.Call(ctx, func(client interface{}) (interface{}, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("Call canceled in caller func", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		errMock := errors.New("mocked")
		_, err := base.Call(ctx, func(client interface{}) (interface{}, error) {
			cancel()
			return nil, errMock
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	t.Run("Call canceled in caller func", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		errMock := errors.New("mocked")
		_, err := base.Call(ctx, func(client interface{}) (interface{}, error) {
			cancel()
			return nil, errMock
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	t.Run("Call returns non-grpc error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errMock := errors.New("mocked")
		_, err := base.Call(ctx, func(client interface{}) (interface{}, error) {
			return nil, errMock
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errMock))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	t.Run("Call returns grpc error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errGrpc := status.Error(codes.Unknown, "mocked")
		_, err := base.Call(ctx, func(client interface{}) (interface{}, error) {
			return nil, errGrpc
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, errGrpc))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.Nil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()

	})

	base.grpcClientMtx.Lock()
	base.grpcClient = nil
	base.grpcClientMtx.Unlock()
	base.SetGetAddrFunc(func() (string, error) { return "", nil })

	t.Run("Call with connect failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := base.Call(ctx, func(client interface{}) (interface{}, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrConnect))
	})
}

func TestClientBase_Recall(t *testing.T) {
	// mock client with nothing
	base := ClientBase{}
	base.grpcClientMtx.Lock()
	base.grpcClient = struct{}{}
	base.grpcClientMtx.Unlock()

	t.Run("Recall normal return", func(t *testing.T) {
		_, err := base.ReCall(context.Background(), func(client interface{}) (interface{}, error) {
			return struct{}{}, nil
		})
		assert.NoError(t, err)
	})

	t.Run("ReCall with canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := base.ReCall(ctx, func(client interface{}) (interface{}, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("ReCall fails first and success second", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		flag := false
		var mut sync.Mutex
		_, err := base.ReCall(ctx, func(client interface{}) (interface{}, error) {
			mut.Lock()
			defer mut.Unlock()
			if flag {
				return struct{}{}, nil
			}
			flag = true
			return nil, errors.New("mock first")
		})
		assert.NoError(t, err)
	})

	t.Run("ReCall canceled in caller func", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		errMock := errors.New("mocked")
		_, err := base.ReCall(ctx, func(client interface{}) (interface{}, error) {
			cancel()
			return nil, errMock
		})

		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
		base.grpcClientMtx.RLock()
		// client shall not be reset
		assert.NotNil(t, base.grpcClient)
		base.grpcClientMtx.RUnlock()
	})

	base.grpcClientMtx.Lock()
	base.grpcClient = nil
	base.grpcClientMtx.Unlock()
	base.SetGetAddrFunc(func() (string, error) { return "", nil })

	t.Run("ReCall with connect failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := base.ReCall(ctx, func(client interface{}) (interface{}, error) {
			return struct{}{}, nil
		})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrConnect))
	})

}
