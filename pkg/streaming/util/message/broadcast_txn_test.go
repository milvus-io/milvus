package message

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBroadcastTxnHeaderAndAdmission(t *testing.T) {
	msg := NewDropCollectionMessageBuilderV1().WithHeader(&DropCollectionMessageHeader{CollectionId: 1}).WithBody(&msgpb.DropCollectionRequest{}).WithBroadcast([]string{"v1"}).MustBuildBroadcast()
	key := NewIdempotencyResourceKey("import", NewCollectionScopedIdempotencyKey(1, "secret"))
	require.NotEqual(t, key, NewIdempotencyResourceKey("update", NewCollectionScopedIdempotencyKey(1, "secret")))
	require.NotContains(t, key.String(), "secret")
	require.Empty(t, NewIdempotencyResourceKey("import", "").Key)
	initialHeader := msg.BroadcastHeader()
	initialHeader.BroadcastID = 5
	initialHeader.ResourceKeys = typeutil.NewSet(NewSharedCollectionNameResourceKey("db", "c"))
	initialHeader.AckSyncUp = true
	msg.OverwriteBroadcastHeader(initialHeader)
	originalHeader := msg.BroadcastHeader()
	tc := &messagespb.BroadcastTxnContext{TxnId: 99, Kind: messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BEGIN}
	initialHeader.Txn = tc
	require.Same(t, msg, msg.OverwriteBroadcastHeader(initialHeader))
	require.Same(t, msg, msg.OverwriteBroadcastAdmissionKey(key.Key))
	header := msg.BroadcastHeader()
	require.True(t, proto.Equal(tc, header.Txn))
	require.Equal(t, originalHeader.BroadcastID, header.BroadcastID)
	require.Equal(t, originalHeader.ResourceKeys, header.ResourceKeys)
	require.Equal(t, originalHeader.VChannels, header.VChannels)
	require.Equal(t, originalHeader.AckSyncUp, header.AckSyncUp)
	for _, part := range msg.SplitIntoMutableMessage() {
		require.Equal(t, uint64(99), part.BroadcastHeader().Txn.TxnId)
		require.Equal(t, key.Key, BroadcastAdmissionKeyOf(part))
	}
	// The encoded header does not alias the caller's proto.
	tc.TxnId = 100
	require.Equal(t, uint64(99), msg.BroadcastHeader().Txn.TxnId)
	header.Txn = &messagespb.BroadcastTxnContext{TxnId: 2, Kind: messagespb.BroadcastTxnKind_BROADCAST_TXN_KIND_BODY, Sequence: 1}
	msg.OverwriteBroadcastHeader(header)
	require.Equal(t, uint64(2), msg.BroadcastHeader().Txn.TxnId)
	msg.OverwriteBroadcastAdmissionKey("replacement")
	require.Equal(t, "replacement", BroadcastAdmissionKeyOf(msg))
	header.Txn = nil
	header.BroadcastID = 0
	header.ResourceKeys = nil
	header.AckSyncUp = false
	header.VChannels = []string{"v2"}
	msg.OverwriteBroadcastHeader(header).OverwriteBroadcastAdmissionKey("")
	require.Nil(t, msg.BroadcastHeader().Txn)
	require.Zero(t, msg.BroadcastHeader().BroadcastID)
	require.Empty(t, msg.BroadcastHeader().ResourceKeys)
	require.False(t, msg.BroadcastHeader().AckSyncUp)
	require.Equal(t, []string{"v2"}, msg.BroadcastHeader().VChannels)
	require.False(t, msg.Properties().Exist(broadcastAdmissionKey))
}
