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

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestGetGenerator(t *testing.T) {
	tests := []struct {
		isvalid  bool
		innumber int

		expectedNo  int
		description string
	}{
		{true, 1, 1, "valid input n 1"},
		{true, 3, 3, "valid input n 3 with cancel"},
	}

	for _, test := range tests {
		rc := &RootCoordFactory{ID: 11111}
		alloc, err := New(context.TODO(), rc, 100)
		require.NoError(t, err)
		err = alloc.Start()
		require.NoError(t, err)

		t.Run(test.description, func(t *testing.T) {
			done := make(chan struct{})
			gen, err := alloc.GetGenerator(test.innumber, done)
			assert.NoError(t, err)

			r := make([]UniqueID, 0)
			for i := range gen {
				r = append(r, i)
			}

			assert.Equal(t, test.expectedNo, len(r))

			if test.innumber > 1 {
				donedone := make(chan struct{})
				gen, err := alloc.GetGenerator(test.innumber, donedone)
				assert.NoError(t, err)

				_, ok := <-gen
				assert.True(t, ok)

				donedone <- struct{}{}

				_, ok = <-gen
				assert.False(t, ok)
			}
		})
	}
}

type RootCoordFactory struct {
	types.RootCoordClient
	ID UniqueID
}

func (m *RootCoordFactory) AllocID(ctx context.Context, in *rootcoordpb.AllocIDRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
	resp := &rootcoordpb.AllocIDResponse{
		ID:     m.ID,
		Count:  in.GetCount(),
		Status: merr.Success(),
	}
	return resp, nil
}
