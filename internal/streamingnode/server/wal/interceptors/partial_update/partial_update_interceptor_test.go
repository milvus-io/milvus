package partial_update

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// buildInsertMessageWithOCC builds a mutable insert message carrying an OCC
// header. expected[i] is consumed as (pk, ts, exists) triples.
func buildInsertMessageWithOCC(t *testing.T, pks []int64, ts []uint64, exists []bool) message.MutableMessage {
	t.Helper()
	header := &message.InsertMessageHeader{
		CollectionId: 1,
		Partitions: []*message.PartitionSegmentAssignment{
			{PartitionId: 2, Rows: uint64(len(pks)), BinarySize: 1},
		},
		OccMode:               messagespb.OCCMode_OCC_MODE_CAS,
		ExpectedPks:           &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
		ExpectedRowTimestamps: ts,
		ExpectedRowExists:     exists,
	}
	body := &msgpb.InsertRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: 1},
		ShardName:      "v1",
		DbName:         "db",
		CollectionName: "c",
		PartitionName:  "p",
		DbID:           1,
		CollectionID:   1,
		PartitionID:    2,
		SegmentID:      0,
		Version:        msgpb.InsertDataVersion_ColumnBased,
		FieldsData:     nil,
		RowIDs:         make([]int64, len(pks)),
		Timestamps:     make([]uint64, len(pks)),
		NumRows:        uint64(len(pks)),
	}
	msg, err := message.NewInsertMessageBuilderV1().
		WithHeader(header).
		WithBody(body).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	// Set a deterministic time tick so the interceptor can populate the
	// version cache after a successful append.
	msg.WithTimeTick(1234)
	return msg
}

func newTestInterceptor() *pkStateInterceptor {
	ready := make(chan struct{})
	close(ready)
	return &pkStateInterceptor{
		cache: NewPKVersionCache(0),
		ready: ready,
	}
}

func TestIsPKStateConflict(t *testing.T) {
	assert.False(t, IsPKStateConflict(nil))
	assert.False(t, IsPKStateConflict(errors.New("plain error")))
	assert.False(t, IsPKStateConflict(status.NewUnrecoverableError("not me")))
	conflict := newConflictError("pk=1 mismatch")
	assert.True(t, IsPKStateConflict(conflict))
	// Wrapping in fmt.Errorf still preserves detection.
	wrapped := fmt.Errorf("oops: %w", conflict)
	assert.True(t, IsPKStateConflict(wrapped))
}

func TestPKStateInterceptor_FirstWritePath(t *testing.T) {
	itc := newTestInterceptor()
	msg := buildInsertMessageWithOCC(t, []int64{10}, []uint64{0}, []bool{false})

	called := false
	id, err := itc.DoAppend(context.Background(), msg, func(ctx context.Context, m message.MutableMessage) (message.MessageID, error) {
		called = true
		return nil, nil
	})
	assert.NoError(t, err)
	assert.Nil(t, id)
	assert.True(t, called, "appendOp should be invoked when CAS succeeds")
}

func TestPKStateInterceptor_ReinsertAfterDeleteAccepted(t *testing.T) {
	itc := newTestInterceptor()
	// Pre-populate the cache to simulate a stale post-delete remnant: the row
	// was written (ts=100) and later deleted, but the cache entry lingers.
	{
		unlock := itc.cache.Lock([]pkKey{{collectionID: 1, pk: int64(11)}})
		itc.cache.Update(pkKey{collectionID: 1, pk: int64(11)}, 100)
		unlock()
	}
	// A re-insert (expectedExists=false) of the same PK must be accepted, not
	// rejected as a conflict: the caller observed the row as non-existent, so
	// the lingering entry can only be a stale post-delete remnant.
	msg := buildInsertMessageWithOCC(t, []int64{11}, []uint64{0}, []bool{false})

	called := false
	_, err := itc.DoAppend(context.Background(), msg, func(ctx context.Context, m message.MutableMessage) (message.MessageID, error) {
		called = true
		return nil, nil
	})
	assert.NoError(t, err)
	assert.True(t, called, "appendOp should be invoked for a re-insert after delete")
}

func TestPKStateInterceptor_VersionMismatch(t *testing.T) {
	itc := newTestInterceptor()
	{
		unlock := itc.cache.Lock([]pkKey{{collectionID: 1, pk: int64(12)}})
		itc.cache.Update(pkKey{collectionID: 1, pk: int64(12)}, 200)
		unlock()
	}
	// Caller thinks the row is at ts=199, actual cache has 200 -> conflict.
	msg := buildInsertMessageWithOCC(t, []int64{12}, []uint64{199}, []bool{true})

	_, err := itc.DoAppend(context.Background(), msg, func(ctx context.Context, m message.MutableMessage) (message.MessageID, error) {
		t.Fatal("appendOp must NOT be called on conflict")
		return nil, nil
	})
	assert.Error(t, err)
	assert.True(t, IsPKStateConflict(err))
}

func TestPKStateInterceptor_PassThroughNonOCC(t *testing.T) {
	itc := newTestInterceptor()
	header := &message.InsertMessageHeader{
		CollectionId: 1,
		Partitions: []*message.PartitionSegmentAssignment{
			{PartitionId: 2, Rows: 1, BinarySize: 1},
		},
		// No OccMode, no expected* fields.
	}
	body := &msgpb.InsertRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, SourceID: 1},
		ShardName:      "v1",
		DbName:         "db",
		CollectionName: "c",
		PartitionName:  "p",
		CollectionID:   1,
		PartitionID:    2,
		Version:        msgpb.InsertDataVersion_ColumnBased,
		RowIDs:         []int64{0},
		Timestamps:     []uint64{0},
		NumRows:        1,
	}
	msg, err := message.NewInsertMessageBuilderV1().
		WithHeader(header).
		WithBody(body).
		WithVChannel("v1").
		BuildMutable()
	assert.NoError(t, err)
	msg.WithTimeTick(1)

	called := false
	_, err = itc.DoAppend(context.Background(), msg, func(ctx context.Context, m message.MutableMessage) (message.MessageID, error) {
		called = true
		return nil, nil
	})
	assert.NoError(t, err)
	assert.True(t, called, "non-OCC inserts must pass through")
}
