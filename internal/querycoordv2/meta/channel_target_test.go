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

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	chTargetCollection = int64(1000)
	chTargetPartition  = int64(100)
	channelA           = "channel-a"
	channelB           = "channel-b"
)

// ChannelTargetSuite covers the two properties channel-level targets rest on:
//   - a channel is promoted on its own, without waiting for the other channels;
//   - a target version that leaves the current/next slots is retained as long as a delegator may
//     still be reading it -- whether it was promoted out or discarded.
type ChannelTargetSuite struct {
	suite.Suite

	kv     kv.MetaKv
	broker *MockBroker
	meta   *Meta
	mgr    *TargetManager
	ctx    context.Context
}

func (suite *ChannelTargetSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ChannelTargetSuite) SetupTest() {
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.ctx = context.Background()

	idAllocator := RandomIncrementIDAllocator()
	suite.meta = NewMeta(idAllocator, querycoord.NewCatalog(suite.kv), session.NewNodeManager())
	suite.broker = NewMockBroker(suite.T())
	suite.mgr = NewTargetManager(suite.broker, suite.meta)

	suite.meta.PutCollection(suite.ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: chTargetCollection, ReplicaNumber: 1},
	})
	suite.meta.PutPartition(suite.ctx, &Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: chTargetCollection, PartitionID: chTargetPartition},
	})
}

func (suite *ChannelTargetSuite) TearDownTest() {
	suite.kv.Close()
}

// recoveryInfo makes the broker answer with the given segments, keyed by channel.
func (suite *ChannelTargetSuite) recoveryInfo(segmentsOfChannel map[string][]int64) *mock.Call {
	channels := []*datapb.VchannelInfo{
		{CollectionID: chTargetCollection, ChannelName: channelA},
		{CollectionID: chTargetCollection, ChannelName: channelB},
	}
	segments := make([]*datapb.SegmentInfo, 0)
	for channel, ids := range segmentsOfChannel {
		for _, id := range ids {
			segments = append(segments, &datapb.SegmentInfo{
				ID: id, CollectionID: chTargetCollection, PartitionID: chTargetPartition,
				InsertChannel: channel, NumOfRows: 100,
			})
		}
	}
	return suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, chTargetCollection).
		Return(channels, segments, nil).Once()
}

func (suite *ChannelTargetSuite) segmentIDs(target *CollectionTarget, channel string) []int64 {
	ids := make([]int64, 0)
	for _, segment := range target.GetChannelSegments(channel) {
		ids = append(ids, segment.GetID())
	}
	return ids
}

// A channel promotes on its own: the other channel keeps serving what it had, at the version it had.
func (suite *ChannelTargetSuite) TestPromoteOneChannelLeavesTheOthersAlone() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}, channelB: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))
	v1 := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, CurrentTarget)
	suite.EqualValues(v1, suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelB, CurrentTarget))

	// compaction on both channels: 1 -> 3, 2 -> 4
	suite.recoveryInfo(map[string][]int64{channelA: {3}, channelB: {4}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))

	// only channel A's delegators became ready
	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelA))

	current := suite.mgr.current.getCollectionTarget(chTargetCollection)
	suite.ElementsMatch([]int64{3}, suite.segmentIDs(current, channelA), "A must have moved to its next target")
	suite.ElementsMatch([]int64{2}, suite.segmentIDs(current, channelB), "B must be untouched")

	vA := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, CurrentTarget)
	vB := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelB, CurrentTarget)
	suite.Greater(vA, v1, "A advanced")
	suite.EqualValues(v1, vB, "B did not")
	suite.EqualValues(vB, suite.mgr.GetCollectionTargetVersion(ctx, chTargetCollection, CurrentTarget),
		"the collection version is the version no channel has moved past")
}

// The version a channel is promoted OUT of stays resolvable: a delegator that has not been synced
// yet is still reading it.
func (suite *ChannelTargetSuite) TestPromotedOutVersionIsRetained() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))
	v1 := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, CurrentTarget)

	suite.recoveryInfo(map[string][]int64{channelA: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelA))

	// segment 1 is gone from both current and next, but a delegator still on v1 serves it
	inVersion, known := suite.mgr.IsSegmentInLiveVersion(chTargetCollection, channelA, v1, 1)
	suite.True(known, "coord must still hold the version it promoted out of")
	suite.True(inVersion, "and it must say the segment is in it")

	inVersion, known = suite.mgr.IsSegmentInLiveVersion(chTargetCollection, channelA, v1, 2)
	suite.True(known)
	suite.False(inVersion, "the new segment was not in the old version")
}

// The version a NEXT target is DISCARDED into nothing (rebuilt before the promote lands) must be
// retained too -- that discarded version is exactly the one delegators were just synced to, and
// forgetting it is the orphan this design exists to remove.
func (suite *ChannelTargetSuite) TestDiscardedNextVersionIsRetained() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))

	// next is rebuilt: delegators get synced to v2 (segment 2)...
	suite.recoveryInfo(map[string][]int64{channelA: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	v2 := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, NextTarget)

	// ... and before the promote lands, next is rebuilt again (out-of-band update): v2 is discarded
	suite.recoveryInfo(map[string][]int64{channelA: {3}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	v3 := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, NextTarget)
	suite.NotEqualValues(v2, v3)

	// a delegator synced to v2 is now on a version that is neither current nor next
	inVersion, known := suite.mgr.IsSegmentInLiveVersion(chTargetCollection, channelA, v2, 2)
	suite.True(known, "the discarded next must still be resolvable")
	suite.True(inVersion, "segment 2 was in it, so it may not be released")
}

// A retained version is dropped once no delegator points at it, and kept while one does.
func (suite *ChannelTargetSuite) TestLiveVersionGC() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))
	v1 := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, CurrentTarget)

	suite.recoveryInfo(map[string][]int64{channelA: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelA))

	_, known := suite.mgr.IsSegmentInLiveVersion(chTargetCollection, channelA, v1, 1)
	suite.True(known)

	// disable the grace window: this test controls readability explicitly
	defer func(g time.Duration) { liveVersionGCGrace = g }(liveVersionGCGrace)
	liveVersionGCGrace = 0

	// a delegator still reads v1: keep it
	suite.mgr.GCLiveVersions(chTargetCollection, channelA, typeutil.NewUniqueSet(v1))
	_, known = suite.mgr.IsSegmentInLiveVersion(chTargetCollection, channelA, v1, 1)
	suite.True(known, "a version a delegator still reads must not be dropped")

	// every delegator has moved on: drop it
	v2 := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, CurrentTarget)
	suite.mgr.GCLiveVersions(chTargetCollection, channelA, typeutil.NewUniqueSet(v2))
	_, known = suite.mgr.IsSegmentInLiveVersion(chTargetCollection, channelA, v1, 1)
	suite.False(known, "an unread version must be dropped")
}

// A version that just stopped being referenced is kept until the grace window elapses, so a GC round
// that races the delegator's heartbeat does not drop a version about to be reported again.
func (suite *ChannelTargetSuite) TestLiveVersionGCGraceWindow() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))
	v1 := suite.mgr.GetChannelTargetVersion(ctx, chTargetCollection, channelA, CurrentTarget)

	suite.recoveryInfo(map[string][]int64{channelA: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelA))

	// v1 is younger than the (default 30s) grace window and unreadable this round: it must survive
	suite.mgr.GCLiveVersions(chTargetCollection, channelA, typeutil.NewUniqueSet(int64(-1)))
	_, known := suite.mgr.IsSegmentInLiveVersion(chTargetCollection, channelA, v1, 1)
	suite.True(known, "a freshly retired version must survive the grace window even when unread")
}

// A partition loaded onto an already-loaded collection lands in next first; promoting a channel must
// carry it into current, or IsCurrentTargetExist(newPartition) stays false forever and the load
// hangs. This falsifies a WithChannelFrom that keeps only the old current's partitions.
func (suite *ChannelTargetSuite) TestPromoteCarriesNewPartitionIntoCurrent() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}, channelB: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))

	// a second partition is loaded; the next target rebuilt from meta now covers it
	const newPartition = int64(200)
	suite.meta.PutPartition(ctx, &Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: chTargetCollection, PartitionID: newPartition},
	})
	suite.recoveryInfo(map[string][]int64{channelA: {3}, channelB: {4}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.False(suite.mgr.IsCurrentTargetExist(ctx, chTargetCollection, newPartition),
		"new partition is only in next before the promote")

	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelA))
	suite.True(suite.mgr.IsCurrentTargetExist(ctx, chTargetCollection, newPartition),
		"the promoted current must carry the partition that was only in next")
}

// Next is dropped once every channel has been promoted, so the next tick rebuilds it immediately
// instead of waiting out NextTargetSurviveTime.
func (suite *ChannelTargetSuite) TestNextDroppedAfterAllChannelsPromoted() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}, channelB: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))

	suite.recoveryInfo(map[string][]int64{channelA: {3}, channelB: {4}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))

	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelA))
	suite.True(suite.mgr.IsNextTargetExist(ctx, chTargetCollection), "next stays while B is unpromoted")

	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelB))
	suite.False(suite.mgr.IsNextTargetExist(ctx, chTargetCollection), "next is dropped once all channels are promoted")
}

// liveVersions must be released when the collection is, since its GC is driven by a dist heartbeat
// that stops arriving after release.
func (suite *ChannelTargetSuite) TestLiveVersionsClearedOnRemoveCollection() {
	ctx := suite.ctx

	suite.recoveryInfo(map[string][]int64{channelA: {1}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, chTargetCollection))
	suite.recoveryInfo(map[string][]int64{channelA: {2}})
	suite.Require().NoError(suite.mgr.UpdateCollectionNextTarget(ctx, chTargetCollection))
	suite.True(suite.mgr.UpdateChannelCurrentTarget(ctx, chTargetCollection, channelA))

	suite.mgr.liveVersionsMut.RLock()
	suite.NotEmpty(suite.mgr.liveVersions[chTargetCollection], "a version was retained")
	suite.mgr.liveVersionsMut.RUnlock()

	suite.mgr.RemoveCollection(ctx, chTargetCollection)

	suite.mgr.liveVersionsMut.RLock()
	_, ok := suite.mgr.liveVersions[chTargetCollection]
	suite.mgr.liveVersionsMut.RUnlock()
	suite.False(ok, "retained versions must be cleared on release")
}

// Targets persisted before this change carry no per-channel version: every channel takes the
// collection's, which is what they were promoted at.
func (suite *ChannelTargetSuite) TestRecoverOldTargetWithoutChannelVersions() {
	target := FromPbCollectionTarget(&querypb.CollectionTarget{
		CollectionID: chTargetCollection,
		Version:      777,
		ChannelTargets: []*querypb.ChannelTarget{
			{ChannelName: channelA},
			{ChannelName: channelB},
		},
	})
	suite.EqualValues(777, target.GetChannelTargetVersion(channelA))
	suite.EqualValues(777, target.GetChannelTargetVersion(channelB))
	suite.EqualValues(777, target.GetTargetVersion())
}

func TestChannelTarget(t *testing.T) {
	suite.Run(t, new(ChannelTargetSuite))
}
