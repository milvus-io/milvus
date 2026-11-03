package messageutil

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestQueryTransformMessageClassification(t *testing.T) {
	for _, tc := range []struct {
		typ      message.MessageType
		advances bool
		global   bool
	}{
		{message.MessageTypeCreateCollection, true, false},
		{message.MessageTypeDelete, true, false},
		{message.MessageTypeCommitTxn, true, false},
		{message.MessageTypeCommitImport, true, false},
		{message.MessageTypeFlush, true, false},
		{message.MessageTypeManualFlush, true, false},
		{message.MessageTypeDropPartition, true, false},
		{message.MessageTypeDropCollection, true, false},
		{message.MessageTypeTruncateCollection, true, false},
		{message.MessageTypeFlushAll, true, true},
		{message.MessageTypeAlterWAL, true, true},
		{message.MessageTypeInsert, false, false},
		{message.MessageTypeTimeTick, false, false},
		{message.MessageTypeRecoveryBarrier, false, false},
		{message.MessageTypeBeginTxn, false, false},
		{message.MessageTypeRollbackTxn, false, false},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			msg := message.NewMutableMessageBeforeAppend(nil, map[string]string{"_t": strconv.Itoa(int(tc.typ))})
			require.Equal(t, tc.advances, AdvancesQueryTransformMVCC(msg))
			require.Equal(t, tc.global, IsPChannelTransformBarrier(tc.typ))
		})
	}
}

func TestQueryTransformSchemaChangeClassification(t *testing.T) {
	for _, path := range []string{message.FieldMaskCollectionSchema, "properties"} {
		t.Run(path, func(t *testing.T) {
			msg := message.NewAlterCollectionMessageBuilderV2().WithVChannel("v1").
				WithHeader(&message.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{path}}}).
				WithBody(&message.AlterCollectionMessageBody{}).MustBuildMutable()
			want := path == message.FieldMaskCollectionSchema
			require.Equal(t, want, AdvancesQueryTransformMVCC(msg))
			immutable := msg.WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(1)).
				IntoImmutableMessage(walimplstest.NewTestMessageID(2))
			require.Equal(t, want, AdvancesQueryTransformMVCC(immutable))
		})
	}
}

// TestIsVChannelGenesisMessage: CreateCollection and the TARGET replica of a
// SplitShard are a vchannel's genesis; the source replica, the control-channel
// replica and ordinary records are not. Both the mutable and the immutable
// forms of the replica answer the same.
func TestIsVChannelGenesisMessage(t *testing.T) {
	broadcast := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     2,
			SourceVchannel:  "p0_1v0",
			TargetVchannels: []string{"p1_1v1", "p2_1v2"},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		WithBroadcast([]string{"p0_1v0", "p1_1v1", "p2_1v2"}, message.OptBuildBroadcastAppendFirst("p0_1v0")).
		MustBuildBroadcast().
		WithBroadcastID(1)
	want := map[string]bool{"p0_1v0": false, "p1_1v1": true, "p2_1v2": true}
	seen := 0
	for _, replica := range broadcast.SplitIntoMutableMessage() {
		expected, ok := want[replica.VChannel()]
		if !ok {
			continue
		}
		seen++
		assert.Equal(t, expected, IsVChannelGenesisMessage(replica), replica.VChannel())
		immutable := replica.WithTimeTick(10).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		assert.Equal(t, expected, IsVChannelGenesisMessage(immutable), replica.VChannel())
		assert.True(t, AdvancesQueryTransformMVCC(replica))
	}
	assert.Equal(t, 3, seen)

	create := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		WithVChannel("p0_1v0").
		MustBuildMutable()
	assert.True(t, IsVChannelGenesisMessage(create))
	insert := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("p0_1v0").
		MustBuildMutable()
	assert.False(t, IsVChannelGenesisMessage(insert))
}
