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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	datanodeclient "github.com/milvus-io/milvus/internal/distributed/datanode/client"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestNodeManager_AddNode(t *testing.T) {
	t.Run("add node successfully", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		nm := NewNodeManager(nodeCreator)
		err := nm.AddNode(1, "localhost:8080")
		assert.NoError(t, err)

		client, err := nm.GetClient(1)
		assert.NoError(t, err)
		assert.Equal(t, mockClient, client)
	})

	t.Run("add node with error", func(t *testing.T) {
		nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return nil, assert.AnError
		}

		nm := NewNodeManager(nodeCreator)
		err := nm.AddNode(1, "localhost:8080")
		assert.Error(t, err)
	})
}

func TestNodeManager_RemoveNode(t *testing.T) {
	t.Run("remove existing node", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockClient.EXPECT().Close().Return(nil)

		nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		nm := NewNodeManager(nodeCreator)
		err := nm.AddNode(1, "localhost:8080")
		assert.NoError(t, err)

		nm.RemoveNode(1)

		client, err := nm.GetClient(1)
		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("remove non-existing node", func(t *testing.T) {
		nm := NewNodeManager(nil)
		nm.RemoveNode(1) // Should not panic
	})
}

func TestNodeManager_GetClient(t *testing.T) {
	t.Run("get existing client", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		nm := NewNodeManager(nodeCreator)
		err := nm.AddNode(1, "localhost:8080")
		assert.NoError(t, err)

		client, err := nm.GetClient(1)
		assert.NoError(t, err)
		assert.Equal(t, mockClient, client)
	})

	t.Run("get non-existing client", func(t *testing.T) {
		nm := NewNodeManager(nil)
		client, err := nm.GetClient(1)
		assert.Error(t, err)
		assert.True(t, merr.WrapErrNodeNotFound(1) != nil)
		assert.Nil(t, client)
	})
}

func TestNodeManager_GetClientIDs(t *testing.T) {
	t.Run("get client IDs", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
			return mockClient, nil
		}

		nm := NewNodeManager(nodeCreator)
		err := nm.AddNode(1, "localhost:8080")
		assert.NoError(t, err)
		err = nm.AddNode(2, "localhost:8081")
		assert.NoError(t, err)

		ids := nm.GetClientIDs()
		assert.Len(t, ids, 2)
		assert.Contains(t, ids, int64(1))
		assert.Contains(t, ids, int64(2))
	})

	t.Run("get empty client IDs", func(t *testing.T) {
		nm := NewNodeManager(nil)
		ids := nm.GetClientIDs()
		assert.Empty(t, ids)
	})
}

func TestNodeManager_Startup_NewNodes(t *testing.T) {
	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return datanodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	nm := NewNodeManager(nodeCreator)

	// Define test nodes
	nodes := []*NodeInfo{
		{NodeID: 1, Address: "localhost:8080"},
		{NodeID: 2, Address: "localhost:8081"},
	}

	err := nm.Startup(context.Background(), nodes)
	assert.NoError(t, err)

	// Verify nodes were added
	ids := nm.GetClientIDs()
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, int64(1))
	assert.Contains(t, ids, int64(2))

	// Verify clients are accessible
	_, err = nm.GetClient(1)
	assert.NoError(t, err)

	_, err = nm.GetClient(2)
	assert.NoError(t, err)
}

func TestNodeManager_Startup_RemoveOldNodes(t *testing.T) {
	// Mock the Close method using mockey
	mockClose := mockey.Mock((*mocks.MockDataNodeClient).Close).Return(nil).Build()
	defer mockClose.UnPatch()

	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return datanodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	nm := NewNodeManager(nodeCreator)

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

	err = nm.Startup(context.Background(), newNodes)
	assert.NoError(t, err)

	// Verify final state
	ids := nm.GetClientIDs()
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, int64(2))
	assert.Contains(t, ids, int64(3))
	assert.NotContains(t, ids, int64(1))

	// Verify node 1 is removed
	_, err = nm.GetClient(1)
	assert.Error(t, err)

	// Verify nodes 2 and 3 are accessible
	_, err = nm.GetClient(2)
	assert.NoError(t, err)

	_, err = nm.GetClient(3)
	assert.NoError(t, err)
}

func TestNodeManager_Startup_EmptyNodes(t *testing.T) {
	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		return datanodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	nm := NewNodeManager(nodeCreator)

	// Add initial node
	err := nm.AddNode(1, "localhost:8080")
	assert.NoError(t, err)

	// Startup with empty nodes (should remove all existing nodes)
	err = nm.Startup(context.Background(), []*NodeInfo{})
	assert.NoError(t, err)

	// Verify all nodes are removed
	ids := nm.GetClientIDs()
	assert.Empty(t, ids)
}

func TestNodeManager_Startup_AddNodeError(t *testing.T) {
	nodeCreator := func(ctx context.Context, addr string, nodeID int64) (types.DataNodeClient, error) {
		if nodeID == 1 {
			return nil, assert.AnError
		}
		return datanodeclient.NewClient(ctx, addr, nodeID, paramtable.Get().DataCoordCfg.WithCredential.GetAsBool())
	}

	nm := NewNodeManager(nodeCreator)

	nodes := []*NodeInfo{
		{NodeID: 1, Address: "localhost:8080"}, // This will fail
		{NodeID: 2, Address: "localhost:8081"},
	}

	err := nm.Startup(context.Background(), nodes)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "assert.AnError")
}
