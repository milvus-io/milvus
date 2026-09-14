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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	msgadaptor "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SnapshotBoundary records the CreateSnapshot flush watermark on each channel.
// Capture waits for these positions to be persisted. Compaction and deletes may
// advance before capture, so these positions do not define a historical read.
type SnapshotBoundary struct {
	// SeekPositions is sorted by channel name and persisted as flush provenance.
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
		msgID, walName, err := snapshotSeekMsgID(result.MessageID)
		if err != nil {
			return nil, err
		}
		positions = append(positions, &msgpb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       msgID,
			Timestamp:   result.TimeTick,
			WALName:     walName,
		})
		if result.TimeTick < snapshotTs {
			snapshotTs = result.TimeTick
		}
	}
	if len(positions) == 0 {
		// Legacy CChannel-only requests have no flush watermark.
		return nil, nil
	}
	sort.Slice(positions, func(i, j int) bool {
		return positions[i].GetChannelName() < positions[j].GetChannelName()
	})
	return &SnapshotBoundary{SeekPositions: positions, SnapshotTs: snapshotTs}, nil
}

// snapshotSeekMsgID renders a MessageID into the wire form a MsgPosition
// consumer expects: the bytes, and the WAL that produced them.
//
// Two things have to be right here and both are easy to get wrong.
//
// The bytes must be Serialize(), not Marshal(). Every other MsgPosition
// producer in the repo uses Serialize(), and the decoder side
// (MustGetMessageIDFromMQWrapperIDBytesWithWALName) assumes it. Marshal() is a
// different format per WAL: for rocksmq it is ASCII decimal while the decoder
// reads a big-endian uint64, and for pulsar and woodpecker it is base64 of the
// bytes the decoder wants raw. Neither round-trips.
//
// WALName must be stamped. Left at its zero value it decodes as
// WALNameUnknown, and the decoder then falls back to whatever WAL is currently
// registered as the default -- which AlterWAL rewrites cluster-wide, so a
// position written before a WAL switch would be handed to the new WAL's
// decoder afterwards.
//
// Returns an error rather than letting the adaptor panic on a MessageID it does
// not recognize: this runs inside the DDL ack callback, where a panic takes
// DataCoord down with it.
//
// The allow-list is exactly what MustGetMQWrapperIDAndWALNameFromMessage
// handles, so today it can only reject WALNameTest, which is build-tagged out
// of production binaries. NOTE for whoever adds a fifth WAL: extend the adaptor
// AND add a matching pre-broadcast check. Failing here is after the append, and
// an error after the append is retried forever without releasing the
// collection's DDL resource key -- the silent wedge checkSnapshotVisibilityReachable
// exists to prevent.
func snapshotSeekMsgID(messageID message.MessageID) ([]byte, commonpb.WALName, error) {
	switch messageID.WALName() {
	case message.WALNameRocksmq, message.WALNameKafka, message.WALNamePulsar, message.WALNameWoodpecker:
	default:
		return nil, commonpb.WALName_Unknown, merr.WrapErrServiceInternalMsg(
			"snapshot boundary cannot encode a %s message id", messageID.WALName().String())
	}
	mqID, walName := msgadaptor.MustGetMQWrapperIDAndWALNameFromMessage(messageID)
	return mqID.Serialize(), walName, nil
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
