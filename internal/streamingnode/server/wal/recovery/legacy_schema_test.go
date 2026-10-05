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

package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func legacySchemaMigrationVChannel() *streamingpb.VChannelMeta {
	initial := &schemapb.CollectionSchema{Version: 0, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 120, Name: "original", DataType: schemapb.DataType_Int64},
	}}
	added := proto.Clone(initial).(*schemapb.CollectionSchema)
	added.Version = 1
	added.Fields = append(added.Fields, &schemapb.FieldSchema{FieldID: 121, Name: "added", DataType: schemapb.DataType_Int64})
	latest := proto.Clone(added).(*schemapb.CollectionSchema)
	latest.Version = 2
	latest.Fields = append(latest.Fields, &schemapb.FieldSchema{FieldID: 122, Name: "latest", DataType: schemapb.DataType_Int64})
	return &streamingpb.VChannelMeta{
		Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 200,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 10,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{{
				PartitionId: 20, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL,
			}},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{Schema: initial, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 10},
				{Schema: added, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 50},
				{Schema: latest, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 200},
			},
		},
	}
}

func legacySchemaMigrationSegment(start uint64, version int32) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID: 1, CollectionID: 10, PartitionID: 20, InsertChannel: "v1",
		State: commonpb.SegmentState_Growing, NumOfRows: 5, StorageVersion: 2, SchemaVersion: version,
		StartPosition: &msgpb.MsgPosition{Timestamp: start}, DmlPosition: &msgpb.MsgPosition{Timestamp: 100},
		Binlogs: []*datapb.FieldBinlog{{FieldID: 121, ChildFields: []int64{121}, Binlogs: []*datapb.Binlog{{EntriesNum: 5}}}},
	}
}

func TestLegacyMigrationPersistsResolvedSchemaVersion(t *testing.T) {
	for _, test := range []struct {
		name              string
		start             uint64
		allocationTime    uint64
		durableVersion    int32
		allocationVersion int32
		alterHistory      func(*streamingpb.VChannelMeta)
		wantVersion       int32
	}{
		{name: "genuine initial version remains zero", start: 20, wantVersion: 0},
		{name: "datacoord only segment after add field", start: 80, wantVersion: 1},
		{name: "original allocation time takes precedence", start: 80, allocationTime: 20, wantVersion: 0},
		{name: "retained history starts after segment", start: 20, alterHistory: func(v *streamingpb.VChannelMeta) { v.CollectionInfo.Schemas = v.CollectionInfo.Schemas[1:] }, wantVersion: 2},
		{name: "dropped schema does not resurrect earlier version", start: 80, alterHistory: func(v *streamingpb.VChannelMeta) {
			v.CollectionInfo.Schemas[1].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
		}, wantVersion: 2},
		{name: "known datacoord version is preserved", start: 20, durableVersion: 1, wantVersion: 1},
		{name: "known allocation version is preserved", start: 20, allocationTime: 20, allocationVersion: 1, wantVersion: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Magic: utility.RecoveryMagicStreamingInitialized})
			vchannel := legacySchemaMigrationVChannel()
			if test.alterHistory != nil {
				test.alterHistory(vchannel)
			}
			segments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
			if test.allocationTime != 0 {
				segments[1] = &streamingpb.SegmentAssignmentMeta{
					CollectionId: 10, PartitionId: 20, SegmentId: 1, Vchannel: "v1",
					State:         streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
					SchemaVersion: test.allocationVersion, CheckpointTimeTick: 900,
					Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: test.allocationTime},
				}
			}
			seek := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 100}
			checkpointCalls, segmentCalls := 0, 0
			checkpointPatch := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).To(func(_ *recoveryStorageImpl, _ context.Context, channel string) (*utility.WALCheckpoint, []int64, error) {
				checkpointCalls++
				require.Equal(t, "v1", channel)
				return seek, []int64{1}, nil
			}).Build()
			defer checkpointPatch.UnPatch()
			durable := legacySchemaMigrationSegment(test.start, test.durableVersion)
			if test.wantVersion == 0 {
				durable.Binlogs[0].FieldID = 120
				durable.Binlogs[0].ChildFields = []int64{120}
			}
			segmentPatch := mockey.Mock((*recoveryStorageImpl).getLegacySegmentInfo).To(func(_ *recoveryStorageImpl, _ context.Context, id int64) (*datapb.SegmentInfo, error) {
				segmentCalls++
				require.EqualValues(t, 1, id)
				return durable, nil
			}).Build()
			defer segmentPatch.UnPatch()
			catalog := &legacyMigrationCatalog{}
			resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
			var persisted []byte
			saves := 0
			catalogPatch := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, _ string, snapshot *metastore.WALRecoverySnapshot) error {
				saves++
				if saves == 1 {
					require.Empty(t, snapshot.SegmentAssignments)
					require.EqualValues(t, utility.RecoveryMagicStreamingInitialized, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
					return nil
				}
				require.EqualValues(t, utility.RecoveryMagicRecoveryStorageV2, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
				var err error
				persisted, err = proto.Marshal(snapshot.SegmentAssignments[1])
				return err
			}).Build()
			defer catalogPatch.UnPatch()

			migrated, err := store.migrateLegacyRecoveryInfo(ctx, map[string]*streamingpb.VChannelMeta{"v1": vchannel}, segments)
			require.NoError(t, err)
			require.True(t, migrated)
			require.Equal(t, 2, saves)
			restored := &streamingpb.SegmentAssignmentMeta{}
			require.NoError(t, proto.Unmarshal(persisted, restored))
			require.Equal(t, test.wantVersion, restored.GetSchemaVersion(), "the selected version must survive protobuf persistence")
			require.Equal(t, test.wantVersion, segments[1].GetSchemaVersion())
			require.Equal(t, test.durableVersion, durable.GetSchemaVersion(), "migration must not rewrite the DataCoord response")

			// A native-format restart must keep even a genuine zero version instead
			// of running the legacy resolver against a newer collection schema.
			restart := newTestRecoveryStorage(t, store.checkpoint.Clone())
			migrated, err = restart.migrateLegacyRecoveryInfo(ctx, map[string]*streamingpb.VChannelMeta{"v1": vchannel}, map[int64]*streamingpb.SegmentAssignmentMeta{1: restored})
			require.NoError(t, err)
			require.False(t, migrated)
			require.Equal(t, 2, saves)
			require.Equal(t, 1, checkpointCalls)
			require.Equal(t, 1, segmentCalls)
			require.Equal(t, test.wantVersion, restored.GetSchemaVersion())
		})
	}
}

func TestLegacyMigrationRetryKeepsResolvedSchemaVersion(t *testing.T) {
	ctx := context.Background()
	store := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Magic: utility.RecoveryMagicStreamingInitialized})
	seek := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(10), TimeTick: 100}
	checkpointPatch := mockey.Mock((*recoveryStorageImpl).getLegacyVChannelCheckpoint).Return(seek, []int64{1}, nil).Build()
	defer checkpointPatch.UnPatch()
	durable := legacySchemaMigrationSegment(80, 0)
	segmentPatch := mockey.Mock((*recoveryStorageImpl).getLegacySegmentInfo).Return(durable, nil).Build()
	defer segmentPatch.UnPatch()
	catalog := &legacyMigrationCatalog{}
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	var partial []byte
	saves := 0
	catalogPatch := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, _ string, snapshot *metastore.WALRecoverySnapshot) error {
		saves++
		if len(snapshot.SegmentAssignments) == 0 {
			require.EqualValues(t, utility.RecoveryMagicStreamingInitialized, snapshot.ConsumeCheckpoint.GetRecoveryMagic())
			return nil
		}
		var err error
		partial, err = proto.Marshal(snapshot.SegmentAssignments[1])
		if err != nil {
			return err
		}
		if saves == 2 {
			// The component batch reached etcd, but the V2 marker did not.
			return context.DeadlineExceeded
		}
		return nil
	}).Build()
	defer catalogPatch.UnPatch()
	vchannels := map[string]*streamingpb.VChannelMeta{"v1": legacySchemaMigrationVChannel()}
	_, err := store.migrateLegacyRecoveryInfo(ctx, vchannels, make(map[int64]*streamingpb.SegmentAssignmentMeta))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	partiallyPersisted := &streamingpb.SegmentAssignmentMeta{}
	require.NoError(t, proto.Unmarshal(partial, partiallyPersisted))
	require.EqualValues(t, 1, partiallyPersisted.GetSchemaVersion())

	// Restart at the legacy rewind saved before the component batch. DataCoord
	// still reports its old default zero, so rebuilding must not erase version 1.
	restartCheckpoint := seek.Clone()
	restartCheckpoint.Magic = utility.RecoveryMagicStreamingInitialized
	restart := newTestRecoveryStorage(t, restartCheckpoint)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	restoredSegments := map[int64]*streamingpb.SegmentAssignmentMeta{1: partiallyPersisted}
	migrated, err := restart.migrateLegacyRecoveryInfo(ctx, vchannels, restoredSegments)
	require.NoError(t, err)
	require.True(t, migrated)
	require.Equal(t, 4, saves)
	require.EqualValues(t, 1, restoredSegments[1].GetSchemaVersion())
	require.EqualValues(t, 0, durable.GetSchemaVersion())
	final := &streamingpb.SegmentAssignmentMeta{}
	require.NoError(t, proto.Unmarshal(partial, final))
	require.EqualValues(t, 1, final.GetSchemaVersion())
}
