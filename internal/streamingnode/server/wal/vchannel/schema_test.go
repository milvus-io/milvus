package vchannel

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestExactSchemaVersionAfterRecovery(t *testing.T) {
	meta := restoreSchemaTestMeta()
	encoded, err := proto.Marshal(meta)
	require.NoError(t, err)
	recovered := new(streamingpb.VChannelMeta)
	require.NoError(t, proto.Unmarshal(encoded, recovered))
	module, err := NewModule(ModuleConfig{PChannel: "p1", VChannel: "v1", VChannelMeta: recovered})
	require.NoError(t, err)
	for _, version := range []int32{0, 1} {
		snapshot, err := module.GetSchemaVersion(1, version)
		require.NoError(t, err)
		require.Equal(t, version, snapshot.Schema.Version)
		require.Equal(t, uint64(version+1)*10, snapshot.CheckpointTimeTick)
		snapshot.Schema.Fields[0].FieldID = 999
		again, err := module.GetSchemaVersion(1, version)
		require.NoError(t, err)
		require.EqualValues(t, 100, again.Schema.Fields[0].FieldID, "callers must own their snapshot")
	}
	state, ok := module.vchannelView.WritePathRecoveryState()
	require.True(t, ok)
	require.EqualValues(t, 1, state.Schema.Version)
	require.EqualValues(t, 20, state.SchemaTimeTick, "write admission recovery must preserve the schema's WAL boundary")
}

func TestExactSchemaVersionAvailability(t *testing.T) {
	for _, tc := range []struct {
		name         string
		collectionID int64
		version      int32
		mutate       func(*streamingpb.VChannelMeta)
		retriable    bool
	}{
		{name: "future schema", collectionID: 1, version: 2, retriable: true},
		{name: "different collection", collectionID: 2},
		{name: "retired schema", collectionID: 1, mutate: func(m *streamingpb.VChannelMeta) {
			m.CollectionInfo.Schemas[0].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
		}},
		{name: "missing body is not version zero", collectionID: 1, mutate: func(m *streamingpb.VChannelMeta) { m.CollectionInfo.Schemas[0].Schema = nil }},
		{name: "dropped collection", collectionID: 1, mutate: func(m *streamingpb.VChannelMeta) { m.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := restoreSchemaTestMeta()
			if tc.mutate != nil {
				tc.mutate(meta)
			}
			view := NewVChannelViewFromMeta(meta)
			got, err := view.GetSchemaVersion(tc.collectionID, tc.version)
			require.Nil(t, got)
			require.Error(t, err)
			require.Equal(t, !tc.retriable, status.AsStreamingError(err).IsUnrecoverable())
		})
	}
}

func TestExactSchemaVersionBeforeRecovery(t *testing.T) {
	module := &VChannelRecoveryModule{vchannel: "v1"}
	_, err := module.GetSchemaVersion(1, 0)
	require.Error(t, err)
	require.False(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestSchemaRemovalUsesObservedBusinessChannelBoundary(t *testing.T) {
	for _, boundary := range []uint64{100, 130} {
		meta := restoreSchemaTestMeta()
		old := meta.CollectionInfo.Schemas[0]
		old.Schema.Fields = append(old.Schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "removed", DataType: schemapb.DataType_Int64})
		next := meta.CollectionInfo.Schemas[1]
		next.CheckpointTimeTick = boundary
		next.Schema = proto.Clone(old.Schema).(*schemapb.CollectionSchema)
		next.Schema.Version = 1
		next.Schema.Fields = next.Schema.Fields[:len(next.Schema.Fields)-1]
		module := &VChannelRecoveryModule{vchannelView: NewVChannelViewFromMeta(meta), queryObservedTimeTick: boundary + 10}
		require.NoError(t, module.ValidateSchemaFields(1, 0, boundary-1, []int64{101}))
		require.ErrorIs(t, module.ValidateSchemaFields(1, 0, boundary, []int64{101}), merr.ErrFieldNotFound)
		require.ErrorIs(t, module.ValidateSchemaFields(1, 0, boundary+1, []int64{101}), merr.ErrFieldNotFound)
		require.NoError(t, module.ValidateSchemaFields(1, 0, boundary+1, []int64{100}), "unrelated fields remain queryable")
		module.queryObservedTimeTick = boundary - 1
		err := module.ValidateSchemaFields(1, 0, boundary, []int64{100})
		require.True(t, viewerror.AsViewError(err).IsRetryable(), "write MVCC cannot substitute for ordered schema observation")
	}
}

func TestLegacyRepeatedSchemaVersionHasOneReadIdentity(t *testing.T) {
	meta := restoreSchemaTestMeta()
	first := meta.CollectionInfo.Schemas[0]
	second := proto.Clone(first).(*streamingpb.CollectionSchemaOfVChannel)
	second.CheckpointTimeTick = 15
	second.Schema.Description = "legacy semantic update without a version bump"
	second.Schema.Fields = append(second.Schema.Fields, &schemapb.FieldSchema{FieldID: 101, Name: "legacy", DataType: schemapb.DataType_Int64})
	meta.CollectionInfo.Schemas = []*streamingpb.CollectionSchemaOfVChannel{first, second}
	data, err := proto.Marshal(meta)
	require.NoError(t, err)
	recovered := &streamingpb.VChannelMeta{}
	require.NoError(t, proto.Unmarshal(data, recovered))
	view := NewVChannelViewFromMeta(recovered)
	read, err := view.GetSchemaVersion(1, 0)
	require.NoError(t, err)
	require.Equal(t, second.Schema.Description, read.Schema.Description)
	require.NoError(t, view.validateSchemaFields(1, 0, 20, []int64{101}), "validation must use the same canonical payload as exact lookup")
	require.Equal(t, first.Schema.Description, view.restoreSegmentSchema(10, 12, 0).Description)
	require.Equal(t, second.Schema.Description, view.restoreSegmentSchema(10, 17, 0).Description)
}
