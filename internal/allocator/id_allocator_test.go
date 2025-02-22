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

package allocator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type mockIDAllocator struct{}

func (tso *mockIDAllocator) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	return &rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     int64(1),
		Count:  req.Count,
	}, nil
}

func newMockIDAllocator() *mockIDAllocator {
	return &mockIDAllocator{}
}

func TestIDAllocator(t *testing.T) {
	ctx := context.Background()
	mockIDAllocator := newMockIDAllocator()

	idAllocator, err := NewIDAllocator(ctx, mockIDAllocator, int64(1))
	assert.NoError(t, err)
	err = idAllocator.Start()
	assert.NoError(t, err)

	idStart, idEnd, err := idAllocator.Alloc(20000)
	assert.NoError(t, err)
	assert.Equal(t, idStart, int64(1))
	assert.Equal(t, idEnd, int64(20001))

	id, err := idAllocator.AllocOne()
	assert.NoError(t, err)
	assert.Equal(t, id, int64(20001))

	id, err = idAllocator.AllocOne()
	assert.NoError(t, err)
	assert.Equal(t, id, int64(20002))
}

func TestIDAllocatorClose(t *testing.T) {
	a, err := NewIDAllocator(context.TODO(), newMockIDAllocator(), 1)
	require.NoError(t, err)

	err = a.Start()
	assert.NoError(t, err)

	a.Close()

	_, _, err = a.Alloc(10)
	assert.Error(t, err)
}
