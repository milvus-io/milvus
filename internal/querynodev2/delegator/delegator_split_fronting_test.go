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

package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func pinnedIDs(sealed []SnapshotItem, growing []SegmentEntry) []int64 {
	ids := make([]int64, 0)
	for _, item := range sealed {
		for _, entry := range item.Segments {
			ids = append(ids, entry.SegmentID)
		}
	}
	for _, entry := range growing {
		ids = append(ids, entry.SegmentID)
	}
	return ids
}

// Between adoption and the current-target flip a relabeled sealed segment S is
// readable in both the source's view (it still holds S) and the adopted child's
// (the leader checker has synced S into it), and the source keeps fronting the
// child until it is released. A read through the source must count S once: the
// child skips every segment the source already pinned for the same read.
func TestFrontedReadSkipsSegmentsTheSourceAlreadyPinned(t *testing.T) {
	const (
		shared        = int64(100) // relabeled sealed segment, in both views
		childOnly     = int64(200) // sealed, only the child holds it
		sharedGrowing = int64(300) // growing, in both views
		childGrowing  = int64(400) // growing, only the child holds it
	)
	sealedEntry := func(id int64) SegmentEntry {
		return SegmentEntry{NodeID: 1, SegmentID: id, PartitionID: 1, Version: 1, TargetVersion: initialTargetVersion, Level: datapb.SegmentLevel_L1}
	}

	// A sealed segment is added unreadable until querycoord syncs a target
	// version; stand in for that sync.
	makeReadable := func(d *distribution, ids ...int64) {
		d.mut.Lock()
		defer d.mut.Unlock()
		for _, id := range ids {
			entry := d.sealedSegments[id]
			entry.TargetVersion = initialTargetVersion
			d.sealedSegments[id] = entry
		}
		d.genSnapshot()
	}

	sourceView := NewChannelQueryView(nil, nil, []int64{1}, 1)
	sourceView.loadedRatio.Store(1.0)
	sourceView.syncedByCoord = true
	source := &shardDelegator{vchannelName: "v0", distribution: NewDistribution("v0", sourceView)}
	source.distribution.AddDistributions(sealedEntry(shared))
	makeReadable(source.distribution, shared)
	source.distribution.AddGrowing(sealedEntry(sharedGrowing))

	child := &shardDelegator{vchannelName: "v1", distribution: NewDistribution("v1", NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion))}
	child.distribution.AddDistributions(sealedEntry(shared), sealedEntry(childOnly))
	makeReadable(child.distribution, shared, childOnly)
	child.distribution.AddGrowing(sealedEntry(sharedGrowing), sealedEntry(childGrowing))

	sourceScope := splitReadScope{pinned: typeutil.NewUniqueSet()}
	sealed, growing, _, version, err := source.pinReadableSegments(sourceScope, 1.0)
	require.NoError(t, err)
	defer source.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{shared, sharedGrowing}, pinnedIDs(sealed, growing))

	childScope := sourceScope.forChild(nil)
	sealed, growing, _, version, err = child.pinReadableSegments(childScope, 1.0)
	require.NoError(t, err)
	defer child.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{childOnly, childGrowing}, pinnedIDs(sealed, growing),
		"a segment the source already pinned must not be read again through its child")
}

// A read with no fronting in play pins exactly what it pinned before.
func TestUnfrontedReadPinsEverything(t *testing.T) {
	child := &shardDelegator{vchannelName: "v1", distribution: NewDistribution("v1", NewChannelQueryView(nil, nil, []int64{1}, initialTargetVersion))}
	child.distribution.AddGrowing(SegmentEntry{NodeID: 1, SegmentID: 7, PartitionID: 1, TargetVersion: initialTargetVersion, Level: datapb.SegmentLevel_L1})

	_, growing, _, version, err := child.pinReadableSegments(splitReadScope{asChild: true}, 1.0)
	require.NoError(t, err)
	defer child.distribution.Unpin(version)
	assert.ElementsMatch(t, []int64{7}, pinnedIDs(nil, growing))

	_, _, _, _, err = child.pinReadableSegments(splitReadScope{}, 1.0)
	assert.Error(t, err, "without the fronting bypass a non-serviceable child refuses the pin")
}

// The source records its pins only when it has children to exclude them from,
// so an ordinary read allocates nothing extra.
func TestFrontingSourceScope(t *testing.T) {
	plain := frontingSourceScope(nil)
	assert.Nil(t, plain.pinned)
	assert.False(t, plain.asChild)

	assert.Nil(t, plain.family.fronted())

	leaf := &familyNode{sd: &shardDelegator{vchannelName: "v0"}}
	assert.Nil(t, frontingSourceScope(leaf).pinned, "a read with no fronted delegator allocates no pin set")

	grandchild := &familyNode{sd: &shardDelegator{vchannelName: "v3"}}
	child := &familyNode{sd: &shardDelegator{vchannelName: "v1"}, children: []*familyNode{grandchild}}
	sibling := &familyNode{sd: &shardDelegator{vchannelName: "v2"}}
	root := &familyNode{sd: &shardDelegator{vchannelName: "v0"}, children: []*familyNode{child, sibling}}
	fronting := frontingSourceScope(root)
	assert.NotNil(t, fronting.pinned)
	assert.False(t, fronting.asChild)
	assert.Same(t, root, fronting.family)
	assert.Equal(t, []*familyNode{child, grandchild, sibling}, root.descendants(), "depth first, grandchildren included")

	childScope := fronting.forChild(child)
	assert.True(t, childScope.asChild)
	assert.Same(t, child, childScope.family)
	// every delegator of the read shares one pin set: each skips what any
	// delegator read before it pinned, and records its own pins for the rest.
	childScope.pinned.Insert(7)
	assert.True(t, fronting.pinned.Contain(7))
	assert.True(t, childScope.exclude.Contain(7))
}
