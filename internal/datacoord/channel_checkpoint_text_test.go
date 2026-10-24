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
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestChannelCheckpointAdvancesPastIdleGrowingSegments(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "single"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				fieldType schemapb.DataType
				startOnly bool
			}{
				{"text_persisted_rows", schemapb.DataType_Text, false},
				{"text_empty_segment", schemapb.DataType_Text, true},
				{"varchar_persisted_rows", schemapb.DataType_VarChar, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					testIdleGrowingSegmentCheckpoint(t, batch, tc.fieldType, tc.startOnly)
				})
			}
		})
	}
}

func testIdleGrowingSegmentCheckpoint(t *testing.T, batch bool, fieldType schemapb.DataType, startOnly bool) {
	t.Helper()
	ctx := context.Background()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	const channel = "by-dev-rootcoord-dml_0_1v0"
	position := func(ts uint64) *msgpb.MsgPosition {
		return &msgpb.MsgPosition{
			ChannelName: channel,
			MsgID:       binary.LittleEndian.AppendUint64(nil, ts),
			Timestamp:   ts,
			WALName:     commonpb.WALName_RocksMQ,
		}
	}
	m.AddCollection(&collectionInfo{
		ID: 1,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: fieldType},
		}},
	})
	// Neither a previous data flush nor an empty segment's start
	// position bounds the writer's subsequently reported safe point.
	for id := int64(1); id <= 2; id++ {
		seg := &datapb.SegmentInfo{
			ID: id, CollectionID: 1, PartitionID: 1,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Growing,
			Level:         datapb.SegmentLevel_L1,
			StartPosition: position(uint64(id) * 100),
		}
		if !startOnly {
			seg.DmlPosition = position(uint64(id)*100 + 100)
			seg.NumOfRows = 3
		}
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(seg)))
	}
	update := func(pos *msgpb.MsgPosition) {
		if batch {
			positions := []*msgpb.MsgPosition{pos}
			require.NoError(t, m.UpdateChannelCheckpoints(ctx, positions))
			require.Same(t, pos, positions[0], "must not replace the caller's position")
		} else {
			require.NoError(t, m.UpdateChannelCheckpoint(ctx, channel, pos))
		}
	}
	// Successive idle TimeTicks must advance without another insert,
	// segment flush or transition out of Growing.
	for _, ts := range []uint64{500, 600, 700} {
		pos := position(ts)
		update(pos)
		require.True(t, proto.Equal(pos, m.GetChannelCheckpoint(channel)))
		persisted, err := m.catalog.ListChannelCheckpoint(ctx)
		require.NoError(t, err)
		require.True(t, proto.Equal(pos, persisted[channel]), "persist the entire reported position")
	}
	update(position(600))
	update(position(700))
	require.True(t, proto.Equal(position(700), m.GetChannelCheckpoint(channel)), "late/duplicate reports must not rewind")
	for id := int64(1); id <= 2; id++ {
		seg := m.GetSegment(ctx, id)
		require.Equal(t, commonpb.SegmentState_Growing, seg.GetState())
		if startOnly {
			require.Nil(t, seg.GetDmlPosition())
		} else {
			require.True(t, proto.Equal(position(uint64(id)*100+100), seg.GetDmlPosition()))
			require.EqualValues(t, 3, seg.GetNumOfRows())
		}
	}
}
