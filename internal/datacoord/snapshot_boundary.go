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
	"math"
	"sort"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SnapshotBoundary is where a snapshot cuts each of its collection's vchannels:
// the position the CreateSnapshot message itself was appended at.
//
// It is not derived from channel checkpoints. A checkpoint says how far data has
// been persisted, which lags writes by a flush and drifts on its own; the
// message position says where the snapshot sits in the write order, which is
// fixed the moment it is appended. Because the shard interceptor seals every
// growing segment at that position, no segment straddles the boundary, and
// membership can be decided per segment instead of per row.
type SnapshotBoundary struct {
	// SeekPositions is one position per data vchannel, sorted by channel name.
	// It doubles as the snapshot's restore point: content ends here, replay
	// resumes here.
	SeekPositions []*msgpb.MsgPosition
	// SnapshotTs is min over SeekPositions. Each vchannel gets its own timetick
	// from a broadcast, so this is a summary, not a global cross-channel instant.
	SnapshotTs uint64
}

// NewSnapshotBoundary builds a boundary from a CreateSnapshot broadcast result.
// The control-channel copy is dropped: it carries no data and its position is
// not comparable with segments on the data vchannels.
func NewSnapshotBoundary(results map[string]*message.AppendResult) (*SnapshotBoundary, error) {
	positions := make([]*msgpb.MsgPosition, 0, len(results))
	snapshotTs := uint64(math.MaxUint64)
	for vchannel, result := range results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		if result == nil || result.MessageID == nil {
			return nil, merr.WrapErrServiceInternalMsg("missing append result for snapshot channel %s", vchannel)
		}
		positions = append(positions, &msgpb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       []byte(result.MessageID.Marshal()),
			Timestamp:   result.TimeTick,
		})
		if result.TimeTick < snapshotTs {
			snapshotTs = result.TimeTick
		}
	}
	if len(positions) == 0 {
		return nil, merr.WrapErrServiceInternal("no data vchannel in snapshot broadcast result")
	}
	sort.Slice(positions, func(i, j int) bool {
		return positions[i].GetChannelName() < positions[j].GetChannelName()
	})
	return &SnapshotBoundary{SeekPositions: positions, SnapshotTs: snapshotTs}, nil
}

// SeekTs returns the boundary timestamp for a channel, and whether the channel
// is part of this boundary at all. A segment on a channel the boundary does not
// cover cannot be placed, and callers must treat that as an error rather than
// silently including or excluding it.
func (b *SnapshotBoundary) SeekTs(channel string) (uint64, bool) {
	for _, position := range b.SeekPositions {
		if position.GetChannelName() == channel {
			return position.GetTimestamp(), true
		}
	}
	return 0, false
}

// filterDeltalogsBefore keeps the delete logs whose contents predate the
// snapshot boundary.
//
// File granularity is exact here, not an approximation: the CreateSnapshot
// message fences its collection at the boundary, so a delete written before it
// is sealed into a file that ends before it, and a delete written after starts a
// new one. No file spans the boundary, and TimestampTo is the file's last row.
//
// A file with no TimestampTo is kept. Those are legacy or not-yet-populated
// binlogs, and dropping deletes because their metadata is incomplete would
// resurrect rows -- the wrong direction to fail in.
func filterDeltalogsBefore(deltalogs []*datapb.FieldBinlog, seekTs uint64) []*datapb.FieldBinlog {
	kept := func(binlog *datapb.Binlog) bool {
		return binlog.GetTimestampTo() == 0 || binlog.GetTimestampTo() < seekTs
	}
	// The common case is that nothing is dropped, since deletes issued after the
	// snapshot are rare relative to the ones already in the segment. Return the
	// input untouched there rather than rebuilding it.
	if lo.EveryBy(deltalogs, func(fieldBinlog *datapb.FieldBinlog) bool {
		return lo.EveryBy(fieldBinlog.GetBinlogs(), kept)
	}) {
		return deltalogs
	}

	filtered := make([]*datapb.FieldBinlog, 0, len(deltalogs))
	for _, fieldBinlog := range deltalogs {
		binlogs := lo.Filter(fieldBinlog.GetBinlogs(), func(binlog *datapb.Binlog, _ int) bool {
			return kept(binlog)
		})
		if len(binlogs) == 0 {
			continue
		}
		cloned := proto.Clone(fieldBinlog).(*datapb.FieldBinlog)
		cloned.Binlogs = binlogs
		filtered = append(filtered, cloned)
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}
