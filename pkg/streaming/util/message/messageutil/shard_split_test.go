package messageutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestRetiresVChannel(t *testing.T) {
	routing := &message.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}}}
	other := &message.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionProperties}}}
	updates := &message.AlterCollectionMessageUpdates{VirtualChannelNames: []string{"p1_1v1", "p2_1v2"}}

	assert.True(t, RetiresVChannel(routing, updates, "p0_1v0"), "delisted under the routing mask")
	assert.False(t, RetiresVChannel(routing, updates, "p1_1v1"), "still listed")
	assert.False(t, RetiresVChannel(other, updates, "p0_1v0"), "not a routing commit")
	assert.False(t, RetiresVChannel(routing, &message.AlterCollectionMessageUpdates{}, "p0_1v0"), "an empty list is not a delist")
	assert.False(t, RetiresVChannel(routing, updates, "p0_vcchan"), "the control channel is never retired")
}
