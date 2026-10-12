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

package vchannel

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func restoreSchemaTestMeta() *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 30,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{Schema: &schemapb.CollectionSchema{Version: 0, Fields: []*schemapb.FieldSchema{{FieldID: 100}}}, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 10},
				{Schema: &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 121}}}, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 20},
			},
		},
	}
}

func TestModuleRestoresGrowingEncodingSchemaVersion(t *testing.T) {
	for _, tc := range []struct {
		name           string
		schemaVersion  int32
		createTimeTick uint64
		retireInitial  bool
		sealed         bool
	}{
		{name: "native version one", schemaVersion: 1, createTimeTick: 25},
		{name: "migrated version with retired allocation history", schemaVersion: 1, createTimeTick: 15, retireInitial: true},
		{name: "real version zero", schemaVersion: 0, createTimeTick: 15},
		{name: "sealed version lookup unchanged", schemaVersion: 1, createTimeTick: 15, sealed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := restoreSchemaTestMeta()
			if tc.retireInitial {
				meta.CollectionInfo.Schemas[0].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
				meta.CollectionInfo.Schemas[0].Schema = nil
			}
			state := streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
			if tc.sealed {
				state = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED
			}
			// A migrated allocation's time lookup can resolve a retired entry
			// while its reconstructed encoding version remains available.
			assignment := &streamingpb.SegmentAssignmentMeta{
				CollectionId: 1, PartitionId: 10, SegmentId: 1000, Vchannel: "v1",
				State: state, SchemaVersion: tc.schemaVersion,
				Stat: &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: tc.createTimeTick},
			}
			var restoredSchema *schemapb.CollectionSchema
			var original func(*streamingpb.SegmentAssignmentMeta, *schemapb.CollectionSchema, segment.ViewConfig) *segment.SegmentView
			patch := mockey.Mock(segment.NewSegmentViewFromMetaWithConfig).Origin(&original).
				To(func(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, config segment.ViewConfig) *segment.SegmentView {
					restoredSchema = schema
					return original(meta, schema, config)
				}).Build()
			defer patch.UnPatch()
			module, err := NewModule(ModuleConfig{
				PChannel: "p1", VChannel: "v1", VChannelMeta: meta,
				Segments: map[int64]*streamingpb.SegmentAssignmentMeta{1000: assignment},
			})
			require.NoError(t, err)
			require.NotNil(t, module.segments[1000])
			require.NotNil(t, restoredSchema)
			require.Equal(t, tc.schemaVersion, restoredSchema.GetVersion())
			expected := meta.CollectionInfo.Schemas[tc.schemaVersion].Schema
			require.True(t, proto.Equal(expected, restoredSchema), "the actual SegmentView must receive the complete encoding schema")
		})
	}
}

func TestRestoreSegmentSchemaPreservesOwnerLifetimeFilters(t *testing.T) {
	for _, tc := range []struct {
		name            string
		collectionState streamingpb.VChannelState
		partitionState  streamingpb.PartitionState
		missingOwner    bool
		closingOwner    int64
		timeTick        uint64
		wantSchema      bool
	}{
		{name: "normal", timeTick: 15, wantSchema: true},
		{name: "missing partition", missingOwner: true, timeTick: 15},
		{name: "collection tombstone", collectionState: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, timeTick: 15},
		{name: "collection dropped before boundary", collectionState: streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, timeTick: 20, wantSchema: true},
		{name: "collection dropped after boundary", collectionState: streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, timeTick: 21},
		{name: "partition tombstone", partitionState: streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, timeTick: 15},
		{name: "partition dropped before boundary", partitionState: streamingpb.PartitionState_PARTITION_STATE_DROPPED, timeTick: 20, wantSchema: true},
		{name: "partition dropped after boundary", partitionState: streamingpb.PartitionState_PARTITION_STATE_DROPPED, timeTick: 21},
		{name: "collection closing", closingOwner: -1, timeTick: 15},
		{name: "partition closing", closingOwner: 10, timeTick: 15},
		{name: "other partition closing", closingOwner: 11, timeTick: 15, wantSchema: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := restoreSchemaTestMeta()
			meta.CheckpointTimeTick = 20
			if tc.collectionState != streamingpb.VChannelState_VCHANNEL_STATE_UNKNOWN {
				meta.State = tc.collectionState
			}
			partition := meta.CollectionInfo.Partitions[0]
			partition.CheckpointTimeTick = 20
			if tc.partitionState != streamingpb.PartitionState_PARTITION_STATE_UNKNOWN {
				partition.State = tc.partitionState
			}
			if tc.missingOwner {
				meta.CollectionInfo.Partitions = nil
			}
			view := NewVChannelViewFromMeta(meta)
			if tc.closingOwner != 0 {
				partitionID := tc.closingOwner
				if partitionID < 0 {
					partitionID = 0
				}
				view.pendingDrops = []pendingDrop{{partitionID: partitionID, timeTick: 20}}
			}
			schema := view.restoreSegmentSchema(10, tc.timeTick, 1)
			if tc.wantSchema {
				require.NotNil(t, schema)
				require.EqualValues(t, 1, schema.GetVersion())
			} else {
				require.Nil(t, schema)
			}
		})
	}
}

func TestRestoreSegmentSchemaDoesNotGuessMissingEncodingVersion(t *testing.T) {
	meta := restoreSchemaTestMeta()
	meta.CollectionInfo.Schemas[0].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
	view := NewVChannelViewFromMeta(meta)
	require.Nil(t, view.restoreSegmentSchema(10, 15, 0), "a retired version zero must not become the latest version")
	require.Nil(t, view.restoreSegmentSchema(10, 15, 2), "an absent encoding version must not become the latest version")
}

func TestCreateSegmentSchemaStillUsesMessageTime(t *testing.T) {
	view := NewVChannelViewFromMeta(restoreSchemaTestMeta())
	createdSchema := view.CreateSegmentSchema(10, 15)
	require.NotNil(t, createdSchema)
	require.Zero(t, createdSchema.GetVersion())
	restoredSchema := view.restoreSegmentSchema(10, 15, 1)
	require.NotNil(t, restoredSchema)
	require.EqualValues(t, 1, restoredSchema.GetVersion())
}

func TestLegacyCreateSegmentEncodingVersionSurvivesNativeRestart(t *testing.T) {
	ctx := context.Background()
	meta := restoreSchemaTestMeta()
	scheduler := &recordingVChannelScheduler{}
	lifecycle := segment.NewSegmentLifecycleWriter(nil, 1)
	ensure := mockey.Mock(mockey.GetMethod(lifecycle, "EnsureGrowingSegment")).Return(nil).Build()
	defer ensure.UnPatch()
	module, err := NewModule(ModuleConfig{
		PChannel: "p1", VChannel: "v1", VChannelMeta: meta,
		Runtime: moduleapi.Runtime{Scheduler: scheduler}, SegmentLifecycle: lifecycle,
	})
	require.NoError(t, err)
	raw := message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 10, SegmentId: 1000}).
		WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(35).
		WithLastConfirmed(walimplstest.NewTestMessageID(34)).IntoImmutableMessage(walimplstest.NewTestMessageID(35))
	owner := message.NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	require.True(t, module.ObserveMessage(ctx, retained))
	retained.Release()
	owner.Release()
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(ctx))

	// Persist the completed CreateSegment effect through the same protobuf
	// representation the catalog reads on a subsequent native-format restart.
	snapshot := module.segments[1000].ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	require.EqualValues(t, 1, snapshot.GetSchemaVersion())
	encoded, err := proto.Marshal(snapshot)
	require.NoError(t, err)
	recovered := new(streamingpb.SegmentAssignmentMeta)
	require.NoError(t, proto.Unmarshal(encoded, recovered))
	module.segments[1000].MarkSnapshotPersisted(snapshot)

	var restoredSchema *schemapb.CollectionSchema
	var original func(*streamingpb.SegmentAssignmentMeta, *schemapb.CollectionSchema, segment.ViewConfig) *segment.SegmentView
	patch := mockey.Mock(segment.NewSegmentViewFromMetaWithConfig).Origin(&original).
		To(func(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, config segment.ViewConfig) *segment.SegmentView {
			restoredSchema = schema
			return original(meta, schema, config)
		}).Build()
	defer patch.UnPatch()
	_, err = NewModule(ModuleConfig{
		PChannel: "p1", VChannel: "v1", VChannelMeta: proto.Clone(meta).(*streamingpb.VChannelMeta),
		Segments: map[int64]*streamingpb.SegmentAssignmentMeta{1000: recovered},
	})
	require.NoError(t, err)
	require.NotNil(t, restoredSchema)
	require.True(t, proto.Equal(meta.CollectionInfo.Schemas[1].Schema, restoredSchema))
}

func TestCreateSegmentEncodingVersionPreservesKnownHeaderAndRealZero(t *testing.T) {
	for _, tc := range []struct {
		name          string
		headerVersion int32
		schema        *schemapb.CollectionSchema
		wantVersion   int32
	}{
		{name: "real zero", schema: &schemapb.CollectionSchema{Version: 0}},
		{name: "known nonzero header", headerVersion: 2, schema: &schemapb.CollectionSchema{Version: 1}, wantVersion: 2},
		{name: "unresolved schema", wantVersion: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").
				WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 10, SegmentId: 1000, SchemaVersion: tc.headerVersion}).
				WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(35).
				WithLastConfirmed(walimplstest.NewTestMessageID(34)).IntoImmutableMessage(walimplstest.NewTestMessageID(35))
			view := segment.NewSegmentViewFromCreateSegmentMessageWithConfig(
				message.MustAsImmutableCreateSegmentMessageV2(raw), tc.schema, segment.ViewConfig{})
			require.Equal(t, tc.wantVersion, view.AssignmentMeta().GetSchemaVersion())
		})
	}
}
