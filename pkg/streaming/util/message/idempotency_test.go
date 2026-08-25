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
		WithIdempotencyKey(NewCollectionScopedIdempotencyKey(1, "key-1")).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()
	require.Equal(t, IdempotencyKey("3:1:key-1"), IdempotencyKeyOf(msg))

	// An empty key must not materialize the property at all, so a non-idempotent
	// broadcast carries exactly the properties it carried before this feature.
	keyless := NewImportMessageBuilderV1().
		WithHeader(&ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{}).
		WithIdempotencyKey(NewCollectionScopedIdempotencyKey(1, "")).
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
		WithIdempotencyKey(NewCollectionScopedIdempotencyKey(1, "key-1")).
		WithBroadcast([]string{"v1", "v2"}).
		MustBuildBroadcast().
		WithBroadcastID(1)
	splitted := msg.SplitIntoMutableMessage()
	require.Len(t, splitted, 2)
	for _, m := range splitted {
		require.Equal(t, IdempotencyKey("3:1:key-1"), IdempotencyKeyOf(m))
	}
}

func TestIdempotencyKeyFingerprint(t *testing.T) {
	fp := IdempotencyKeyFingerprint("tenant-secret-key")
	require.Len(t, fp, idempotencyKeyFingerprintBytes*2)
	require.NotContains(t, fp, "tenant")
	require.Equal(t, fp, IdempotencyKeyFingerprint("tenant-secret-key"))
	require.NotEqual(t, fp, IdempotencyKeyFingerprint("other-key"))
}

// TestScopedIdempotencyKeyEncoding pins what makes two client keys the same
// operation, and what keeps them apart.
func TestScopedIdempotencyKeyEncoding(t *testing.T) {
	base := NewCollectionScopedIdempotencyKey(449988, "k")

	// The same key against the same collection is the same operation.
	require.Equal(t, base, NewCollectionScopedIdempotencyKey(449988, "k"))

	// A different collection, a different client key, or a different scope kind is
	// a different operation. The scope kind matters even though collection ids and
	// database ids come from one allocator today: the encoding must not depend on
	// that.
	require.NotEqual(t, base, NewCollectionScopedIdempotencyKey(449989, "k"))
	require.NotEqual(t, base, NewCollectionScopedIdempotencyKey(449988, "k2"))
	require.NotEqual(t, base, NewDatabaseScopedIdempotencyKey(449988, "k"))
	require.NotEqual(t, base, NewClusterScopedIdempotencyKey("k"))

	// Cluster scope has no object to name, so it encodes a scope id of 0. It is the
	// domain, not the id, that keeps it apart from a real object.
	require.Equal(t, IdempotencyKey("1:0:k"), NewClusterScopedIdempotencyKey("k"))
}

// TestScopedIdempotencyKeyResistsCraftedClientKey proves a client cannot reach
// another scope's entry by embedding the encoding's separator in its key. The
// client key is the unbounded tail, so it is inert no matter what it holds.
func TestScopedIdempotencyKeyResistsCraftedClientKey(t *testing.T) {
	// A client key crafted to look like "collection 7, key v" when appended.
	crafted := NewClusterScopedIdempotencyKey("3:7:v")
	legit := NewCollectionScopedIdempotencyKey(7, "v")
	require.NotEqual(t, crafted, legit)

	// The same in the other direction: the crafted key cannot pose as cluster scope.
	require.NotEqual(t, NewCollectionScopedIdempotencyKey(7, "1:0:v"), NewClusterScopedIdempotencyKey("v"))
}

// TestIdempotencyKeyClientKey covers what bounds and fingerprints are taken over:
// the client's own bytes, never the scope this package prepended.
func TestIdempotencyKeyClientKey(t *testing.T) {
	require.Equal(t, "k", NewCollectionScopedIdempotencyKey(449988, "k").ClientKey())
	require.Equal(t, "k", NewClusterScopedIdempotencyKey("k").ClientKey())

	// A client key containing the separator round-trips whole.
	require.Equal(t, "a:b:c", NewCollectionScopedIdempotencyKey(1, "a:b:c").ClientKey())

	// A value this package did not produce has no recoverable client portion, so it
	// is returned whole rather than reported as empty -- a bound taken over it must
	// stay conservative.
	require.Equal(t, "garbage", IdempotencyKey("garbage").ClientKey())
}

// TestZeroClientKeyNeverEncodesAScope guards the trap that would make every
// keyless broadcast of one message type deduplicate against every other: an empty
// client key must produce the zero key, not a non-empty scope prefix.
func TestZeroClientKeyNeverEncodesAScope(t *testing.T) {
	require.Empty(t, NewCollectionScopedIdempotencyKey(449988, ""))
	require.Empty(t, NewDatabaseScopedIdempotencyKey(12, ""))
	require.Empty(t, NewClusterScopedIdempotencyKey(""))
}
