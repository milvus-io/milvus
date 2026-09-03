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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// newRedundantIndexChecker builds a checker whose one loaded segment still
// holds an index (1001) that the index metadata no longer lists, exactly the
// state a dropped index leaves behind.
func newRedundantIndexChecker(t *testing.T) *IndexChecker {
	t.Helper()
	ctx := context.Background()

	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()

	nodeMgr := session.NewNodeManager()
	metaMgr := meta.NewMeta(params.RandomIncrementIDAllocator(), catalog, nodeMgr)
	distManager := meta.NewDistributionManager(nodeMgr)
	broker := meta.NewMockBroker(t)
	targetMgr := meta.NewMockTargetManager(t)
	checker := NewIndexChecker(metaMgr, distManager, broker, nodeMgr, targetMgr)
	targetMgr.EXPECT().GetSealedSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cid, sid int64, i3 int32) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{ID: sid, Level: datapb.SegmentLevel_L1}
	}).Maybe()

	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	coll.Schema = &schemapb.CollectionSchema{
		Name: "test_dropped_index",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_JSON, Name: "JSON"},
		},
	}
	require.NoError(t, checker.meta.PutCollection(ctx, coll))
	require.NoError(t, checker.meta.Put(ctx, utils.CreateTestReplica(200, 1, []int64{1, 2})))
	for _, nodeID := range []int64{1, 2} {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID, Address: "localhost", Hostname: "localhost"}))
		checker.meta.HandleNodeUp(ctx, nodeID)
	}

	segment := utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel")
	segment.IndexInfo = map[int64]*querypb.FieldIndexInfo{
		1000: {FieldID: 101, IndexID: 1000, EnableIndex: true},
		1001: {FieldID: 102, IndexID: 1001, EnableIndex: true},
	}
	checker.dist.SegmentDistManager.Update(1, segment)

	broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return([]*indexpb.IndexInfo{
		{FieldID: 101, IndexID: 1000},
	}, nil)
	broker.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).Return([]*datapb.SegmentInfo{}, nil).Maybe()
	return checker
}

func TestReloadSegmentOnIndexDropIsOnByDefault(t *testing.T) {
	paramtable.Init()
	require.True(t, paramtable.Get().QueryCoordCfg.ReloadSegmentOnIndexDrop.GetAsBool())
	checker := newRedundantIndexChecker(t)
	tasks := checker.Check(context.Background())
	require.Len(t, tasks, 1, "natively the segment holding the dropped index is reopened without it")
}

func TestReloadSegmentOnIndexDropOffLeavesTheSegmentAlone(t *testing.T) {
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.ReloadSegmentOnIndexDrop.Key
	paramtable.Get().Save(key, "false")
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	checker := newRedundantIndexChecker(t)
	tasks := checker.Check(context.Background())
	require.Empty(t, tasks, "with the switch off a dropped index is left on the segments until the collection is reloaded")
}
