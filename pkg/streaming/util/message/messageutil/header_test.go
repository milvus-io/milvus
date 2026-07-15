package messageutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestIsSchemaChange(t *testing.T) {
	header := &message.AlterCollectionMessageHeader{
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{},
		},
	}
	assert.False(t, IsSchemaChange(header))

	header.UpdateMask.Paths = []string{message.FieldMaskCollectionSchema}
	assert.True(t, IsSchemaChange(header))
}

func TestIsTimeTickConfirmBarrier(t *testing.T) {
	assert.True(t, IsTimeTickConfirmBarrier(message.MessageTypeTimeTick))
	assert.True(t, IsTimeTickConfirmBarrier(message.MessageTypeRecoveryBarrier))
	assert.False(t, IsTimeTickConfirmBarrier(message.MessageTypeInsert))
	assert.False(t, IsTimeTickConfirmBarrier(message.MessageTypeDelete))
}
