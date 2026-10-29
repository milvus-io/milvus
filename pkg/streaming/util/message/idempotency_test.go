package message

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	// An insert and a commit-txn message expose their key through the same
	// accessor: the key is a message property, not a per-type header field.
	insert := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithIdempotencyKey("key-1").
		MustBuildMutable()
	require.Equal(t, IdempotencyKey("key-1"), IdempotencyKeyOf(insert))
	require.Equal(t, "key-1", insert.Properties().ToRawMap()[messageIdempotencyKey])

	commit := NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&CommitTxnMessageHeader{}).
		WithBody(&CommitTxnMessageBody{}).
		WithIdempotencyKey("key-1").
		MustBuildMutable()
	require.Equal(t, IdempotencyKey("key-1"), IdempotencyKeyOf(commit))

	// An empty key must not materialize the property at all: a non-idempotent
	// write must carry no idempotency property, not an empty-valued one.
	keyless := NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithIdempotencyKey("").
		MustBuildMutable()
	require.Empty(t, IdempotencyKeyOf(keyless))
	require.NotContains(t, keyless.Properties().ToRawMap(), messageIdempotencyKey)

	// The key counts toward the estimated message size, so the proxy's
	// max-message-size guard still accounts for the idempotency overhead.
	require.Greater(t, insert.EstimateSize(), keyless.EstimateSize())
}

func TestMergeIdempotentInsertResults(t *testing.T) {
	merged, hadAny, err := MergeIdempotentInsertResults(
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}},
			},
		},
		nil, // nil elements are skipped
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{2, 1},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{12, 11}}},
			},
		},
	)
	require.NoError(t, err)
	require.True(t, hadAny)
	require.Equal(t, []uint32{0, 2, 1}, merged.GetRowOffsets())
	require.Equal(t, []int64{10, 12, 11}, merged.GetIds().GetIntId().GetData())
}

func TestMergeIdempotentInsertResultsEmpty(t *testing.T) {
	merged, hadAny, err := MergeIdempotentInsertResults()
	require.NoError(t, err)
	require.False(t, hadAny)
	require.Nil(t, merged)

	merged, hadAny, err = MergeIdempotentInsertResults(nil, nil)
	require.NoError(t, err)
	require.False(t, hadAny)
	require.Nil(t, merged)
}

func TestMergeIdempotentInsertResultsRejectsMixedIDTypes(t *testing.T) {
	_, hadAny, err := MergeIdempotentInsertResults(
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{0},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
		},
		&messagespb.IdempotentInsertResult{
			RowOffsets: []uint32{1},
			Ids:        &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk"}}}},
		},
	)
	require.Error(t, err)
	require.False(t, hadAny)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestValidateIdempotentInsertResult(t *testing.T) {
	require.NoError(t, ValidateIdempotentInsertResult(nil))
	require.NoError(t, ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{}))

	// row offsets but no ids
	err := ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
	})
	require.Error(t, err)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	// ids but no row offsets
	err = ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
	})
	require.Error(t, err)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	// ids present but neither int nor string field populated
	err = ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0},
		Ids:        &schemapb.IDs{},
	})
	require.Error(t, err)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	// length mismatch
	err = ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{0, 1},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10}}}},
	})
	require.Error(t, err)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestInsertHeaderIdempotentInsertResult(t *testing.T) {
	header := &InsertMessageHeader{}
	result := &messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{1, 0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"pk-1", "pk-0"}}},
		},
	}
	SetInsertHeaderIdempotentInsertResult(header, result)
	require.NotNil(t, header.GetIdempotentResult())

	roundTrip, ok := IdempotentInsertResultFromInsertHeader(header)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 0}, roundTrip.GetRowOffsets())
	require.Equal(t, []string{"pk-1", "pk-0"}, roundTrip.GetIds().GetStrId().GetData())
	require.NoError(t, ValidateIdempotentInsertResult(roundTrip))
}
