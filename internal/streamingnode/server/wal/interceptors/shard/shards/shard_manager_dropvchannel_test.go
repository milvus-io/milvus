package shards

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// TestDropVChannelIsGuardedByTheVChannelName pins the property that makes
// reclamation safe.
//
// m.collections is keyed by COLLECTION id, one entry per pchannel. Once the
// coordinator reclaims a retired source's slot, a later vchannel of the same
// collection can be allocated onto this pchannel and take over that entry. A
// DropVChannel for the old vchannel arriving after that must not delete the new
// one's registration, or the new shard would be left with no segment assignment
// at all.
func TestDropVChannelIsGuardedByTheVChannelName(t *testing.T) {
	const (
		collectionID = 991
		oldVChannel  = "by-dev-rootcoord-dml_0_991v0"
		newVChannel  = "by-dev-rootcoord-dml_0_991v7"
	)

	newManager := func(registered string) *shardManagerImpl {
		m := newTestShardManagerWithVChannelState(t,
			streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 50).(*shardManagerImpl)
		m.collections[collectionID] = newCollectionInfo(registered, []int64{1})
		return m
	}
	dropMsg := func(vchannel string) message.ImmutableDropVChannelMessageV2 {
		msg, err := message.NewDropVChannelMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&message.DropVChannelMessageHeader{CollectionId: collectionID}).
			WithBody(&message.DropVChannelMessageBody{}).
			BuildMutable()
		require.NoError(t, err)
		return message.MustAsImmutableDropVChannelMessageV2(
			msg.WithTimeTick(200).WithLastConfirmedUseMessageID().
				IntoImmutableMessage(rmq.NewRmqID(2)))
	}

	t.Run("drops the entry it names", func(t *testing.T) {
		m := newManager(oldVChannel)
		m.DropVChannel(dropMsg(oldVChannel))
		_, ok := m.collections[collectionID]
		assert.False(t, ok, "the retired vchannel's registration must be gone")
	})

	t.Run("leaves a successor on the same pchannel alone", func(t *testing.T) {
		m := newManager(newVChannel)
		// A late or replayed teardown for the vchannel that USED to hold this
		// pchannel's entry.
		m.DropVChannel(dropMsg(oldVChannel))
		info, ok := m.collections[collectionID]
		require.True(t, ok, "the successor's registration must survive")
		assert.Equal(t, newVChannel, info.VChannel)
	})

	t.Run("is idempotent once the entry is gone", func(t *testing.T) {
		m := newManager(oldVChannel)
		m.DropVChannel(dropMsg(oldVChannel))
		assert.NotPanics(t, func() { m.DropVChannel(dropMsg(oldVChannel)) })
	})
}
