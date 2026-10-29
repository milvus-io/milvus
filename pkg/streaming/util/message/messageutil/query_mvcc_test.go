package messageutil

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

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
