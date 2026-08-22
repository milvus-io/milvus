package message

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

func TestIdempotencyKeyProperty(t *testing.T) {
	require.Empty(t, IdempotencyKeyOf(nil))

	// A broadcast message carrying a key is read back through the same accessor
	// that serves every other message type and stage.
	msg := NewImportMessageBuilderV1().
		WithHeader(&ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey("import/1/key-1").
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	require.Equal(t, "import/1/key-1", IdempotencyKeyOf(msg))

	// An empty key must not materialize the property at all, so a non-idempotent
	// broadcast carries exactly the properties it carried before this feature.
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
	// Every per-vchannel message that SplitIntoMutableMessage produces must still
	// carry the key: neither the WAL append path nor the recovery side can read it
	// otherwise, which would silently disable deduplication.
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
