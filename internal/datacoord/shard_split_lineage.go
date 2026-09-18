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

package datacoord

import (
	"slices"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The lineage window of a shard split (design doc §6.3 step 4, §8.1).
//
// From the fence until the adoption delists the source, the source delegator
// keeps serving the source's key space. The rewrite drops each source segment
// in the commit that publishes its outputs on the targets, and data written
// through the targets is flushed there, so for as long as the source is listed
// its recovery view takes in the targets' flushed data and reports it under the
// source:
//
//   - the source's merged view (GetQueryVChanPositionsOfSplitFamily) holds the
//     targets' flushed non-L0 segments next to its own, so a rewrite's input and
//     its outputs sit in one compaction frontier, which reports one or the
//     other, never both;
//   - GetRecoveryInfoV2 reports those segments under the source
//     (splitAttributedInsertChannel), which is the channel QueryCoord groups
//     them by;
//   - the family is resolved once per call, so the view and the attribution
//     can never disagree on which tasks open a window.
//
// The window ends with the DescribeCollection snapshot that delists the source:
// a call that no longer lists the source attributes nothing to it.

// resolveSplitFamilies maps every source vchannel accepted by listed to the
// target vchannels of the split task that is not Done or Aborted and fences it.
// A task whose targets are not allocated yet has nothing to take in.
func (m *shardSplitManager) resolveSplitFamilies(listed func(vchannel string) bool) map[string][]string {
	families := make(map[string][]string)
	for _, task := range m.store.list() {
		if !isSplitShardTaskActive(task) || !splitTargetsAllocated(task) {
			continue
		}
		source := splitTaskSource(task)
		if source == "" || !listed(source) {
			continue
		}
		families[source] = append(families[source], splitTaskTargetVChannels(task)...)
	}
	for _, targets := range families {
		slices.Sort(targets)
	}
	return families
}

// SplitTargetsOfSource returns the target vchannels whose flushed segments the
// given source's merged recovery view takes in, or nil when it is the source of
// no split in flight.
func (m *shardSplitManager) SplitTargetsOfSource(vchannel string) []string {
	return m.resolveSplitFamilies(func(source string) bool { return source == vchannel })[vchannel]
}

// SplitLineageOfListedSources maps every target vchannel of a lineage window
// whose source is in listed to that source. listed is the vchannel set of one
// recovery-info call, one DescribeCollection snapshot, so the mapping ends
// exactly when the adoption delists the source. A caller that also builds the
// sources' views builds them from this one result (splitFamiliesOf).
func (m *shardSplitManager) SplitLineageOfListedSources(listed typeutil.Set[string]) map[string]string {
	lineage := make(map[string]string)
	for source, targets := range m.resolveSplitFamilies(func(vchannel string) bool { return listed.Contain(vchannel) }) {
		for _, target := range targets {
			lineage[target] = source
		}
	}
	return lineage
}

// splitFamiliesOf inverts a target -> source lineage into source -> targets,
// with each source's targets sorted.
func splitFamiliesOf(lineage map[string]string) map[string][]string {
	families := make(map[string][]string)
	for target, source := range lineage {
		families[source] = append(families[source], target)
	}
	for _, targets := range families {
		slices.Sort(targets)
	}
	return families
}

// inheritedBySplitSource reports whether a split target's segment belongs in
// its source's merged view: settled, visible, non-L0 data only.
//
//   - Growing (and invisible) data stays with the child consuming the target
//     WAL; the source serving it too would read it twice.
//   - L0 stays with the target: the source applies the target's deletes through
//     forwarding, and a target L0 in the source's view would move the source's
//     delete checkpoint past deletes its later-loaded segments still need.
//   - An importing segment is still being written.
func inheritedBySplitSource(segment *SegmentInfo) bool {
	return isFlushState(segment.GetState()) &&
		segment.GetLevel() != datapb.SegmentLevel_L0 &&
		!segment.GetIsImporting() &&
		!segment.GetIsInvisible()
}

// splitAttributedInsertChannel is the channel a segment is reported under to
// QueryCoord: its split source while the lineage window is open, its own
// channel otherwise. An L0 keeps its own channel; deletes reach the source by
// forwarding, not by loading a target's L0.
func splitAttributedInsertChannel(segment *SegmentInfo, lineage map[string]string) string {
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		return segment.GetInsertChannel()
	}
	if source, ok := lineage[segment.GetInsertChannel()]; ok {
		return source
	}
	return segment.GetInsertChannel()
}
