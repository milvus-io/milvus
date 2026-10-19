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

package checkers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestIndexCheckerDropBeforeReload(t *testing.T) {
	paramtable.Init()
	for _, balancing := range []bool{false, true} {
		t.Run(map[bool]string{false: "single copy", true: "balancing copies"}[balancing], func(t *testing.T) {
			testIndexCheckerDropBeforeReload(t, balancing)
		})
	}
}

func testIndexCheckerDropBeforeReload(t *testing.T, balancing bool) {
	t.Helper()
	ctx := context.Background()
	nodeMgr := session.NewNodeManager()
	dist := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	checker := NewIndexChecker(nil, dist, broker, nodeMgr, nil)
	collection := utils.CreateTestCollection(1, 1)
	replica := utils.CreateTestReplica(200, 1, []int64{1, 2})
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, DataType: schemapb.DataType_VarChar},
	}}
	wanted := []*indexpb.IndexInfo{{FieldID: 101, IndexID: 1001}}
	segment := utils.CreateTestSegment(1, 1, 2, 1, 1, "channel")
	segment.IndexInfo = map[int64]*querypb.FieldIndexInfo{
		1000: {FieldID: 101, IndexID: 1000, EnableIndex: true},
	}
	dist.SegmentDistManager.Update(1, segment)
	// During balancing, another node can report the same segment without the
	// old index. It must not generate a load that blocks deletion on node 1.
	if balancing {
		dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 2, 2, 1, "channel"))
	}
	// The replacement is already built before the first check. No RPC failure
	// or node change is needed to reproduce the load-before-drop conflict.
	broker.EXPECT().GetIndexInfo(mock.Anything, int64(1), int64(2)).Return(
		map[int64][]*querypb.FieldIndexInfo{2: {
			{FieldID: 101, IndexID: 1001, EnableIndex: true, IndexFilePaths: []string{"new-index"}},
		}}, nil).Maybe()

	// Unchanged reports cover a failed deletion and a successful deletion whose
	// distribution update has not reached QueryCoord yet. Neither permits load.
	for round := 0; round < 3; round++ {
		tasks := checker.checkReplica(ctx, collection, replica, wanted, schema)
		require.Len(t, tasks, 1)
		drop, ok := tasks[0].(*task.DropIndexTask)
		require.True(t, ok, "round %d must only schedule deletion", round)
		require.Equal(t, int64(2), drop.SegmentID())
		require.Equal(t, []int64{1000}, drop.Actions()[0].(*task.DropIndexAction).IndexIDs())
	}
	broker.AssertNotCalled(t, "GetIndexInfo", mock.Anything, int64(1), int64(2))

	// Only the QN's report that A is gone allows B to load.
	dist.SegmentDistManager.Update(2)
	segment = utils.CreateTestSegment(1, 1, 2, 1, 1, "channel")
	dist.SegmentDistManager.Update(1, segment)
	tasks := checker.checkReplica(ctx, collection, replica, wanted, schema)
	require.Len(t, tasks, 1)
	require.Equal(t, task.ActionTypeUpdate, tasks[0].Actions()[0].Type())

	segment.IndexInfo = map[int64]*querypb.FieldIndexInfo{
		1001: {FieldID: 101, IndexID: 1001, EnableIndex: true},
	}
	dist.SegmentDistManager.Update(1, segment)
	require.Empty(t, checker.checkReplica(ctx, collection, replica, wanted, schema))
}
