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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type TestSegmentOperatorSuite struct {
	suite.Suite
}

func (s *TestSegmentOperatorSuite) TestSetMaxRowCount() {
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			MaxRowNum: 300,
		},
	}

	ops := SetMaxRowCount(20000)
	updated := ops(segment)
	s.Require().True(updated)
	s.EqualValues(20000, segment.GetMaxRowNum())

	updated = ops(segment)
	s.False(updated)
}

func TestSegmentOperators(t *testing.T) {
	suite.Run(t, new(TestSegmentOperatorSuite))
}

func TestUpdateStartPosition(t *testing.T) {
	for _, tc := range []struct {
		name     string
		level    datapb.SegmentLevel
		position *msgpb.MsgPosition
		accepted bool
	}{
		{"L0 timestamp only", datapb.SegmentLevel_L0, &msgpb.MsgPosition{Timestamp: 100}, true},
		{"L0 WAL position", datapb.SegmentLevel_L0, &msgpb.MsgPosition{MsgID: []byte{1}, Timestamp: 100}, true},
		{"L0 missing position", datapb.SegmentLevel_L0, nil, false},
		{"L0 empty position", datapb.SegmentLevel_L0, &msgpb.MsgPosition{}, false},
		{"Woodpecker first entry", datapb.SegmentLevel_L1, &msgpb.MsgPosition{Timestamp: 100, WALName: commonpb.WALName_WoodPecker}, true},
		{"Woodpecker missing position", datapb.SegmentLevel_L1, &msgpb.MsgPosition{WALName: commonpb.WALName_WoodPecker}, false},
		{"L1 timestamp only", datapb.SegmentLevel_L1, &msgpb.MsgPosition{Timestamp: 100}, false},
		{"L1 Woodpecker zero ID", datapb.SegmentLevel_L1, &msgpb.MsgPosition{Timestamp: 100, WALName: commonpb.WALName_WoodPecker}, false},
		{"L1 missing position", datapb.SegmentLevel_L1, nil, false},
		{"L1 WAL position", datapb.SegmentLevel_L1, &msgpb.MsgPosition{MsgID: []byte{1}, Timestamp: 100}, true},
		{"L1 message ID only", datapb.SegmentLevel_L1, &msgpb.MsgPosition{MsgID: []byte{1}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := &msgpb.MsgPosition{ChannelName: "ch", MsgID: []byte{2}, Timestamp: 50}
			segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID: 1, Level: tc.level, StartPosition: original,
			}}
			pack := &UpdateSegmentPack{Segments: map[int64]*SegmentInfo{1: segment}}
			if tc.position != nil {
				tc.position.ChannelName = "ch"
			}
			require.True(t, UpdateStartPosition([]*datapb.SegmentStartPosition{{
				SegmentID: 1, StartPosition: tc.position,
			}})(pack))
			want := original
			if tc.accepted {
				want = tc.position
			}
			require.True(t, proto.Equal(want, segment.GetStartPosition()))
			if tc.level != datapb.SegmentLevel_L0 {
				require.Equal(t, tc.position.GetTimestamp(), segment.GetTransformStartAfterTimetick())
			} else {
				require.Zero(t, segment.GetTransformStartAfterTimetick())
			}
		})
	}

	t.Run("missing segment", func(t *testing.T) {
		pack := &UpdateSegmentPack{
			Meta:     &Meta{Ctx: context.Background(), Segments: NewSegmentsInfo(metacache.NewMetaStore(nil))},
			Segments: make(map[int64]*SegmentInfo),
		}
		require.True(t, UpdateStartPosition([]*datapb.SegmentStartPosition{{
			SegmentID: 1, StartPosition: &msgpb.MsgPosition{Timestamp: 100},
		}})(pack))
		require.Empty(t, pack.Segments)
	})
}

func TestFirstPackTransformCoveragePersistence(t *testing.T) {
	ctx := context.Background()
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 10, InsertChannel: "ch",
		Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Growing,
	})))
	first := UpdateStartPosition([]*datapb.SegmentStartPosition{{
		SegmentID: 1, StartPosition: &msgpb.MsgPosition{
			ChannelName: "ch", Timestamp: 100, WALName: commonpb.WALName_WoodPecker,
		},
	}})
	// A failed catalog write must leave both persisted and in-memory coverage
	// unknown, so the caller retries before acknowledging the first data pack.
	failure := mockey.Mock((*catalogkv.Catalog).AlterSegments).Return(merr.ErrServiceUnavailable).Build()
	t.Cleanup(func() { failure.UnPatch() })
	require.ErrorIs(t, m.UpdateSegmentsInfo(ctx, first), merr.ErrServiceUnavailable)
	require.Zero(t, m.GetSegment(ctx, 1).GetTransformStartAfterTimetick())
	stored, err := m.Catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Zero(t, stored[0].GetTransformStartAfterTimetick())
	failure.UnPatch()

	require.NoError(t, m.UpdateSegmentsInfo(ctx, first))
	require.NoError(t, m.UpdateSegmentsInfo(ctx, first), "first-pack replay is idempotent")
	stored, err = m.Catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, uint64(100), stored[0].GetTransformStartAfterTimetick())

	// Reload through the real metadata loader. Final publication must resolve
	// coverage from the persisted field even without a physical StartPosition.
	recovered, err := NewMeta(ctx, m.Catalog, nil, NewMetaStoreWithCollections(m.Catalog, 100), nil, NoDomainMetas)
	require.NoError(t, err)
	t.Cleanup(recovered.CloseManifestCommitExecutor)
	segment := recovered.GetSegment(ctx, 1)
	require.NotNil(t, segment)
	require.Nil(t, segment.GetStartPosition())
	require.Equal(t, uint64(100), SegmentTransformStart(segment))
	require.NoError(t, recovered.UpdateSegmentsInfo(ctx, UpdateStartPosition([]*datapb.SegmentStartPosition{{
		SegmentID: 1, StartPosition: &msgpb.MsgPosition{Timestamp: 200, MsgID: []byte{1}},
	}})))
	require.Equal(t, uint64(100), SegmentTransformStart(recovered.GetSegment(ctx, 1)), "later positions cannot skip earlier transforms")

	importing := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L1, IsImporting: true}}
	require.True(t, first(&UpdateSegmentPack{Segments: map[int64]*SegmentInfo{1: importing}}))
	require.Zero(t, importing.GetTransformStartAfterTimetick(), "import coverage comes from its commit")
}

func TestUpdateImportSegmentPosition(t *testing.T) {
	t.Run("segment not found", func(t *testing.T) {
		// Create a meta with empty segments to properly test the "not found" case
		store := metacache.NewMetaStore(nil)
		segments := NewSegmentsInfo(store)
		m := &Meta{Segments: segments, MetaStore: store}
		modPack := &UpdateSegmentPack{
			Meta:     m,
			Segments: make(map[int64]*SegmentInfo),
		}
		op := UpdateImportSegmentPosition(100, 1000, 2000)
		result := op(modPack)
		assert.False(t, result)
	})

	t.Run("update position successfully", func(t *testing.T) {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            100,
				InsertChannel: "test_channel",
			},
		}
		modPack := &UpdateSegmentPack{
			Segments: map[int64]*SegmentInfo{
				100: segment,
			},
		}
		op := UpdateImportSegmentPosition(100, 1000, 2000)
		result := op(modPack)
		assert.True(t, result)

		// Verify StartPosition
		assert.NotNil(t, segment.GetStartPosition())
		assert.Equal(t, "test_channel", segment.GetStartPosition().GetChannelName())
		assert.Nil(t, segment.GetStartPosition().GetMsgID())
		assert.Equal(t, uint64(1000), segment.GetStartPosition().GetTimestamp())

		// Verify DmlPosition
		assert.NotNil(t, segment.GetDmlPosition())
		assert.Equal(t, "test_channel", segment.GetDmlPosition().GetChannelName())
		assert.Nil(t, segment.GetDmlPosition().GetMsgID())
		assert.Equal(t, uint64(2000), segment.GetDmlPosition().GetTimestamp())
	})

	t.Run("update position with zero timestamps", func(t *testing.T) {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            101,
				InsertChannel: "channel_2",
			},
		}
		modPack := &UpdateSegmentPack{
			Segments: map[int64]*SegmentInfo{
				101: segment,
			},
		}
		op := UpdateImportSegmentPosition(101, 0, 0)
		result := op(modPack)
		assert.True(t, result)

		assert.NotNil(t, segment.GetStartPosition())
		assert.Equal(t, uint64(0), segment.GetStartPosition().GetTimestamp())
		assert.NotNil(t, segment.GetDmlPosition())
		assert.Equal(t, uint64(0), segment.GetDmlPosition().GetTimestamp())
	})
}
