package message

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

func TestIdempotencyKeyProperty(t *testing.T) {
	require.Empty(t, IdempotencyKeyOf(nil))

	// 携带 key 的广播消息，通过统一 accessor 读出。
	msg := NewImportMessageBuilderV1().
		WithHeader(&ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey("import/1/key-1").
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	require.Equal(t, "import/1/key-1", IdempotencyKeyOf(msg))

	// 空 key 必须完全不产生 property，非幂等广播的属性集合与本特性出现前一致。
	keyless := NewImportMessageBuilderV1().
		WithHeader(&ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey("").
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	require.Empty(t, IdempotencyKeyOf(keyless))
	require.NotContains(t, keyless.Properties().ToRawMap(), messageIdempotencyKey)
}

func TestIdempotencyKeySurvivesSplit(t *testing.T) {
	// SplitIntoMutableMessage 拆出的每条 per-vchannel 消息都必须仍带着 key，
	// 否则 WAL 侧和恢复侧都读不到它。
	msg := NewImportMessageBuilderV1().
		WithHeader(&ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey("import/1/key-1").
		WithBroadcast([]string{"v1", "v2"}).
		MustBuildBroadcast().
		WithBroadcastID(1)
	splitted := msg.SplitIntoMutableMessage()
	require.Len(t, splitted, 2)
	for _, m := range splitted {
		require.Equal(t, "import/1/key-1", IdempotencyKeyOf(m))
	}
}

func TestIdempotencyKeyFingerprint(t *testing.T) {
	fp := IdempotencyKeyFingerprint("tenant-secret-key")
	require.Len(t, fp, idempotencyKeyFingerprintBytes*2)
	require.NotContains(t, fp, "tenant")
	require.Equal(t, fp, IdempotencyKeyFingerprint("tenant-secret-key"))
	require.NotEqual(t, fp, IdempotencyKeyFingerprint("other-key"))
}
