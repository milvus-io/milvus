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

package observers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitLoadFixture drives the CollectionObserver's progress counters over a next
// target that lists a split source and its two window targets, with only the
// source in dist -- the state a collection is in for the whole split window.
type splitLoadFixture struct {
	t            *testing.T
	ctx          context.Context
	collectionID int64
	partitionID  int64

	meta      *meta.Meta
	targetMgr *meta.MockTargetManager
	dist      *meta.DistributionManager
	ob        *CollectionObserver

	// exclusions is what the target manager grants for this collection; the
	// fixture serves it from the mocked GetSplitWindowExclusions, so a test can
	// drive the real observeLoadStatus wiring instead of hand-feeding the two
	// counters.
	exclusions typeutil.Set[string]
}

func newSplitLoadFixture(t *testing.T, segmentChannels map[int64]string) *splitLoadFixture {
	paramtable.Init()
	f := &splitLoadFixture{t: t, ctx: context.Background(), collectionID: 1000, partitionID: 10}

	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1}))
	f.dist = meta.NewDistributionManager(nodeMgr)

	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SavePartition(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	f.meta = meta.NewMeta(RandomIncrementIDAllocator(), catalog, nodeMgr)
	assert.NoError(t, f.meta.PutCollection(f.ctx,
		utils.CreateTestCollection(f.collectionID, 1),
		utils.CreateTestPartition(f.collectionID, f.partitionID)))
	assert.NoError(t, f.meta.Put(f.ctx, meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: f.collectionID, ResourceGroup: meta.DefaultResourceGroupName, Nodes: []int64{1},
	})))

	channels := map[string]*meta.DmChannel{}
	for _, name := range []string{"src", "t1", "t2"} {
		channels[name] = meta.DmChannelFromVChannel(&datapb.VchannelInfo{
			CollectionID: f.collectionID, ChannelName: name,
		})
	}
	segments := map[int64]*datapb.SegmentInfo{}
	loadedOnSource := map[int64]*querypb.SegmentDist{}
	for id, channel := range segmentChannels {
		segments[id] = &datapb.SegmentInfo{
			ID: id, CollectionID: f.collectionID, PartitionID: f.partitionID, InsertChannel: channel,
		}
		if channel == "src" {
			loadedOnSource[id] = &querypb.SegmentDist{NodeID: 1}
		}
	}

	f.targetMgr = meta.NewMockTargetManager(t)
	f.targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, f.collectionID, meta.NextTarget).Return(channels).Maybe()
	f.targetMgr.EXPECT().GetSealedSegmentsByPartition(mock.Anything, f.collectionID, f.partitionID, meta.NextTarget).Return(segments).Maybe()
	f.targetMgr.EXPECT().IsCurrentTargetExist(mock.Anything, f.collectionID, mock.Anything).Return(true).Maybe()
	f.targetMgr.EXPECT().IsNextTargetExist(mock.Anything, f.collectionID).Return(true).Maybe()
	f.targetMgr.EXPECT().GetSplitWindowExclusions(mock.Anything, f.collectionID, meta.NextTarget).RunAndReturn(
		func(context.Context, int64, int32) (typeutil.Set[string], bool) {
			return f.exclusions, true
		}).Maybe()

	// only the source has a delegator: the unadopted window targets are hidden
	// from GetDataDistribution.
	f.dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: f.collectionID, ChannelName: "src"},
		Node:         1,
		View: &meta.LeaderView{
			ID: 1, CollectionID: f.collectionID, Channel: "src", Segments: loadedOnSource,
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	proxyManager := proxyutil.NewMockProxyClientManager(t)
	proxyManager.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	targetObserver := NewTargetObserver(f.meta, f.targetMgr, f.dist, nil, nil, nodeMgr)
	f.ob = NewCollectionObserver(f.dist, f.meta, f.targetMgr, targetObserver, &checkers.CheckerController{}, proxyManager)
	return f
}

// observeLoadTask runs one round of the real observer loop -- the path
// production takes, which derives the exclusion from the target manager itself
// -- and returns the partition's load percentage after it. NewCollectionObserver
// registered a load task for the collection, so observeLoadStatus has something
// to observe.
func (f *splitLoadFixture) observeLoadTask() int32 {
	f.ob.observeLoadStatus(f.ctx)
	return f.meta.GetPartitionLoadPercentage(f.ctx, f.partitionID)
}

// observe runs one progress round with the given exclusion and returns the
// partition's load percentage after it.
func (f *splitLoadFixture) observe(excluded typeutil.Set[string]) int32 {
	channelTargetNum, subChannelCount := f.ob.observeChannelStatus(f.ctx, f.collectionID, excluded)
	partition := f.meta.GetPartition(f.ctx, f.partitionID)
	assert.NotNil(f.t, partition)
	f.ob.observePartitionLoadStatus(f.ctx, partition, 1, channelTargetNum, subChannelCount, excluded)
	return f.meta.GetPartitionLoadPercentage(f.ctx, f.partitionID)
}

// TestLoadProgressExcludesSplitWindowTargets pins Task 17's progress rule: while
// a split window is open the load is counted on the channels the read path is
// served from, so a collection whose source delegator is loaded reaches 100%
// instead of sitting short for the length of the whole rewrite.
func TestLoadProgressExcludesSplitWindowTargets(t *testing.T) {
	// both outputs are attributed to the still-listed source.
	segmentChannels := map[int64]string{1: "src", 2: "src"}

	t.Run("counting the window targets holds the load short", func(t *testing.T) {
		f := newSplitLoadFixture(t, segmentChannels)
		// 3 channels + 2 segments = 5 units, of which 1 channel + 2 segments are
		// loaded: the two window targets can never be.
		assert.EqualValues(t, 60, f.observe(nil))
	})

	t.Run("excluding them lets the load finish against the source", func(t *testing.T) {
		f := newSplitLoadFixture(t, segmentChannels)
		assert.EqualValues(t, 100, f.observe(typeutil.NewSet("t1", "t2")))
	})

	t.Run("a segment attributed to an excluded channel is not counted either", func(t *testing.T) {
		// a pull that still places a segment under a window target: nothing can
		// load it, because the channel has no delegator.
		f := newSplitLoadFixture(t, map[int64]string{1: "src", 2: "src", 3: "t1"})
		assert.EqualValues(t, 50, f.observe(nil))
		f = newSplitLoadFixture(t, map[int64]string{1: "src", 2: "src", 3: "t1"})
		assert.EqualValues(t, 100, f.observe(typeutil.NewSet("t1", "t2")))
	})

	t.Run("the observer loop derives the exclusion from the target manager", func(t *testing.T) {
		// The wiring, not the arithmetic: observeLoadStatus must ask the target
		// manager for the exclusion and pass it to both counters. Dropping that
		// one call leaves the load short for the whole window, which is the bug
		// this task exists to fix.
		f := newSplitLoadFixture(t, segmentChannels)
		f.exclusions = nil
		assert.EqualValues(t, 60, f.observeLoadTask())

		f = newSplitLoadFixture(t, segmentChannels)
		f.exclusions = typeutil.NewSet("t1", "t2")
		assert.EqualValues(t, 100, f.observeLoadTask())
		// the collection follows its partitions, so load() returns.
		assert.EqualValues(t, 100, f.meta.CalculateLoadPercentage(f.ctx, f.collectionID))
	})

	t.Run("excluding every channel leaves nothing to observe", func(t *testing.T) {
		f := newSplitLoadFixture(t, segmentChannels)
		channelTargetNum, subChannelCount := f.ob.observeChannelStatus(f.ctx, f.collectionID,
			typeutil.NewSet("src", "t1", "t2"))
		assert.Equal(t, 0, channelTargetNum)
		assert.Equal(t, 0, subChannelCount)
	})
}
