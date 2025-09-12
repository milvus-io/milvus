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

package session

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	indexnodeclient "github.com/milvus-io/milvus/internal/distributed/indexnode/client"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	typeutil "github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestIndexNodeManager_AddNode(t *testing.T) {
	paramtable.Init()
	nm := NewNodeManager(context.Background(), defaultIndexNodeCreatorFunc)

	t.Run("success", func(t *testing.T) {
		err := nm.AddNode(1, "indexnode-1")
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		err := nm.AddNode(2, "")
		assert.Error(t, err)
	})
}

func TestIndexNodeManager_PickClient(t *testing.T) {
	paramtable.Init()
	getMockedGetJobStatsClient := func(resp *workerpb.GetJobStatsResponse, err error) types.IndexNodeClient {
		ic := mocks.NewMockIndexNodeClient(t)
		ic.EXPECT().GetJobStats(mock.Anything, mock.Anything, mock.Anything).Return(resp, err)
		return ic
	}

	err := errors.New("error")

	t.Run("multiple unavailable IndexNode", func(t *testing.T) {
		nm := &IndexNodeManager{
			ctx: context.TODO(),
			nodeClients: map[typeutil.UniqueID]types.IndexNodeClient{
				1: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					Status: merr.Status(err),
				}, err),
				2: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					Status: merr.Status(err),
				}, err),
				3: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					Status: merr.Status(err),
				}, err),
				4: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					Status: merr.Status(err),
				}, err),
				5: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					Status: merr.Status(err),
				}, nil),
				6: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					Status: merr.Status(err),
				}, nil),
				7: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					Status: merr.Status(err),
				}, nil),
				8: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					AvailableSlots: 1,
					Status:         merr.Success(),
				}, nil),
				9: getMockedGetJobStatsClient(&workerpb.GetJobStatsResponse{
					AvailableSlots: 10,
					Status:         merr.Success(),
				}, nil),
			},
		}

		selectNodeID, client := nm.PickClient()
		assert.NotNil(t, client)
		assert.Contains(t, []typeutil.UniqueID{8, 9}, selectNodeID)
	})
}

func TestNodeManager_StoppingNode(t *testing.T) {
	paramtable.Init()
	nm := NewNodeManager(context.Background(), defaultIndexNodeCreatorFunc)
	err := nm.AddNode(1, "indexnode-1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(nm.GetAllClients()))

	nm.StoppingNode(1)
	assert.Equal(t, 0, len(nm.GetAllClients()))
	assert.Equal(t, 1, len(nm.stoppingNodes))

	nm.RemoveNode(1)
	assert.Equal(t, 0, len(nm.GetAllClients()))
	assert.Equal(t, 0, len(nm.stoppingNodes))
}

func TestNodeManager_Startup_NewNodes(t *testing.T) {
	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.IndexNodeClient, error) {
		return indexnodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	ctx := context.Background()
	nm := NewNodeManager(ctx, nodeCreator)

	// Define test nodes
	nodes := []*NodeInfo{
		{NodeID: 1, Address: "localhost:8080"},
		{NodeID: 2, Address: "localhost:8081"},
	}

	err := nm.Startup(nodes)
	assert.NoError(t, err)

	// Verify nodes were added
	ids := nm.GetAllClients()
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, int64(1))
	assert.Contains(t, ids, int64(2))

	// Verify clients are accessible
	_, ok := nm.GetClientByID(1)
	assert.True(t, ok)

	_, ok = nm.GetClientByID(2)
	assert.True(t, ok)
}

func TestNodeManager_Startup_RemoveOldNodes(t *testing.T) {
	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.IndexNodeClient, error) {
		return indexnodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	ctx := context.Background()
	nm := NewNodeManager(ctx, nodeCreator)

	// Add initial nodes
	err := nm.AddNode(1, "localhost:8080")
	assert.NoError(t, err)
	err = nm.AddNode(2, "localhost:8081")
	assert.NoError(t, err)

	// Startup with new set of nodes (removes node 1, keeps node 2, adds node 3)
	newNodes := []*NodeInfo{
		{NodeID: 2, Address: "localhost:8081"}, // existing node
		{NodeID: 3, Address: "localhost:8082"}, // new node
	}

	err = nm.Startup(newNodes)
	assert.NoError(t, err)

	// Verify final state
	ids := nm.GetAllClients()
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, int64(2))
	assert.Contains(t, ids, int64(3))
	assert.NotContains(t, ids, int64(1))

	// Verify node 1 is removed
	_, ok := nm.GetClientByID(1)
	assert.False(t, ok)

	// Verify nodes 2 and 3 are accessible
	_, ok = nm.GetClientByID(2)
	assert.True(t, ok)

	_, ok = nm.GetClientByID(3)
	assert.True(t, ok)
}

func TestNodeManager_Startup_EmptyNodes(t *testing.T) {
	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.IndexNodeClient, error) {
		return indexnodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	ctx := context.Background()
	nm := NewNodeManager(ctx, nodeCreator)

	// Add initial node
	err := nm.AddNode(1, "localhost:8080")
	assert.NoError(t, err)

	// Startup with empty nodes (should remove all existing nodes)
	err = nm.Startup(nil)
	assert.NoError(t, err)

	// Verify all nodes are removed
	ids := nm.GetAllClients()
	assert.Empty(t, ids)
}

func TestNodeManager_Startup_AddNodeError(t *testing.T) {
	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.IndexNodeClient, error) {
		if nodeID == 1 {
			return nil, assert.AnError
		}
		return indexnodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	ctx := context.Background()
	nm := NewNodeManager(ctx, nodeCreator)

	nodes := []*NodeInfo{
		{NodeID: 1, Address: "localhost:8080"}, // This will fail
		{NodeID: 2, Address: "localhost:8081"},
	}

	err := nm.Startup(nodes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "assert.AnError")
}
