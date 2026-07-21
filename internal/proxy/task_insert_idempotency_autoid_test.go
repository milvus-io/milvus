package proxy

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReassignAutoIDByOffsetChannelsRoutesEachOffsetToStableBucket(t *testing.T) {
	channels := []string{"ch0", "ch1", "ch2", "ch3"}
	rowIDs := make([]int64, 9)
	for offset := range rowIDs {
		rowIDs[offset] = firstInt64IDNotRoutingToOffsetBucket(t, channels, offset, int64(1000+offset*10000))
	}
	original := slices.Clone(rowIDs)

	nextID := int64(100000)
	allocCalls := 0
	alloc := func(count uint32) (int64, int64, error) {
		allocCalls++
		begin := nextID
		nextID += int64(count)
		return begin, nextID, nil
	}

	require.NoError(t, reassignAutoIDByOffsetChannels(rowIDs, schemapb.DataType_Int64, channels, 0, alloc))
	requireOffsetRoutesToModuloChannels(t, &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: rowIDs}},
	}, channels)
	require.NotEqual(t, original, rowIDs, "test setup should require at least one replacement")
	require.Positive(t, allocCalls, "test setup should require additional candidates")
}

func TestReassignAutoIDByOffsetChannelsSupportsVarCharPrimary(t *testing.T) {
	rowIDs := []int64{3000, 3001, 3002, 3003, 3004, 3005}
	channels := []string{"ch0", "ch1", "ch2"}

	nextID := int64(200000)
	alloc := func(count uint32) (int64, int64, error) {
		begin := nextID
		nextID += int64(count)
		return begin, nextID, nil
	}

	require.NoError(t, reassignAutoIDByOffsetChannels(rowIDs, schemapb.DataType_VarChar, channels, 0, alloc))
	ids, err := autoIDCandidatesToPrimaryIDs(rowIDs, schemapb.DataType_VarChar)
	require.NoError(t, err)
	requireOffsetRoutesToModuloChannels(t, ids, channels)
}

func TestReassignAutoIDByOffsetChannelsRoutesAgainstGivenChannelOrder(t *testing.T) {
	// The collection's stored vchannel order is not lexicographic in general, and it
	// is the order delete hashes against, so the routing must follow it as given.
	channels := []string{"ch2", "ch0", "ch1"}
	firstRowIDs := nonMatchingRowIDsForOffsets(t, channels, 6, 1000)
	secondRowIDs := nonMatchingRowIDsForOffsets(t, channels, 6, 900000)
	newRangeAlloc := func(nextID int64) func(uint32) (int64, int64, error) {
		return func(count uint32) (int64, int64, error) {
			begin := nextID
			nextID += int64(count)
			return begin, nextID, nil
		}
	}

	require.NoError(t, reassignAutoIDByOffsetChannels(
		firstRowIDs,
		schemapb.DataType_Int64,
		channels,
		0,
		newRangeAlloc(100000),
	))
	require.NoError(t, reassignAutoIDByOffsetChannels(
		secondRowIDs,
		schemapb.DataType_Int64,
		channels,
		0,
		newRangeAlloc(500000),
	))

	firstIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: firstRowIDs}},
	}
	secondIDs := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: secondRowIDs}},
	}
	require.NotEqual(t, firstRowIDs, secondRowIDs, "retry auto IDs should be allowed to differ")
	requireOffsetRoutesToModuloChannels(t, firstIDs, channels)
	requireOffsetRoutesToModuloChannels(t, secondIDs, channels)
	require.Equal(t,
		rowChannelsByPK(firstIDs, channels),
		rowChannelsByPK(secondIDs, channels),
	)
}

func TestReassignAutoIDByOffsetChannelsNoop(t *testing.T) {
	t.Run("empty row ids", func(t *testing.T) {
		called := false
		alloc := func(uint32) (int64, int64, error) {
			called = true
			return 0, 0, nil
		}
		require.NoError(t, reassignAutoIDByOffsetChannels(nil, schemapb.DataType_Int64, []string{"ch0", "ch1"}, 0, alloc))
		require.False(t, called)
	})

	t.Run("single channel", func(t *testing.T) {
		rowIDs := []int64{10, 11, 12}
		original := slices.Clone(rowIDs)
		called := false
		alloc := func(uint32) (int64, int64, error) {
			called = true
			return 0, 0, nil
		}
		require.NoError(t, reassignAutoIDByOffsetChannels(rowIDs, schemapb.DataType_Int64, []string{"ch0"}, 0, alloc))
		require.Equal(t, original, rowIDs)
		require.False(t, called)
	})
}

func TestReassignAutoIDByOffsetChannelsErrors(t *testing.T) {
	t.Run("nil allocator", func(t *testing.T) {
		err := reassignAutoIDByOffsetChannels([]int64{1, 2}, schemapb.DataType_Int64, []string{"ch0", "ch1"}, 0, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "id allocator is nil")
	})

	t.Run("unsupported primary type", func(t *testing.T) {
		err := reassignAutoIDByOffsetChannels([]int64{1, 2}, schemapb.DataType_Float, []string{"ch0", "ch1"}, 0,
			func(uint32) (int64, int64, error) {
				return 10, 20, nil
			})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported auto id primary field type")
	})

	t.Run("allocator error", func(t *testing.T) {
		channels := []string{"ch0", "ch1", "ch2"}
		rowIDs := []int64{firstInt64IDNotRoutingToOffsetBucket(t, channels, 0, 1)}
		allocErr := errors.New("alloc failed")
		err := reassignAutoIDByOffsetChannels(rowIDs, schemapb.DataType_Int64, channels, 0,
			func(uint32) (int64, int64, error) {
				return 0, 0, allocErr
			})
		require.ErrorIs(t, err, allocErr)
	})

	t.Run("unfillable bucket gives up instead of looping forever", func(t *testing.T) {
		// Duplicate PKs hash to the same channel, leaving the other channel's
		// bucket permanently short. An allocator that returns no new ids (an
		// empty range) can never fill it, so the stabilization loop must give up
		// rather than spin forever and burn the global id space.
		channels := []string{"ch0", "ch1"}
		rowIDs := []int64{7, 7}
		err := reassignAutoIDByOffsetChannels(rowIDs, schemapb.DataType_Int64, channels, 0,
			func(uint32) (int64, int64, error) {
				return 100000, 100000, nil
			})
		require.Error(t, err)
		require.Contains(t, err.Error(), "stabilize")
	})
}

func TestInsertTaskReassignAutoIDForStableIdempotency(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)
	channels := []string{"ch0", "ch1", "ch2"}
	primary := &schemapb.FieldSchema{
		FieldID:      1,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
		AutoID:       true,
	}
	task := insertTask{
		ctx:            ctx,
		idAllocator:    idAllocator,
		idempotencyKey: "stable-key",
		// result is initialized by PreExecute in the real flow; this test calls
		// reassignAutoIDForStableIdempotency directly, so initialize it here.
		result: &milvuspb.MutationResult{},
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base:    &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
				NumRows: 6,
				RowIDs:  []int64{1000, 1001, 1002, 1003, 1004, 1005},
				FieldsData: []*schemapb.FieldData{
					int64FieldData("value", 2, []int64{10, 11, 12, 13, 14, 15}),
					int64FieldData("pk", 1, []int64{-1, -1, -1, -1, -1, -1}),
				},
			},
		},
	}

	require.NoError(t, task.reassignAutoIDForStableIdempotency(primary, channels))
	require.Equal(t, task.insertMsg.GetRowIDs(), task.result.GetIDs().GetIntId().GetData())
	requireOffsetRoutesToModuloChannels(t, task.result.GetIDs(), channels)

	var pkFields int
	for _, fieldData := range task.insertMsg.GetFieldsData() {
		if fieldData.GetFieldId() == primary.GetFieldID() || fieldData.GetFieldName() == primary.GetName() {
			pkFields++
			require.Equal(t, task.insertMsg.GetRowIDs(), fieldData.GetScalars().GetLongData().GetData())
		}
	}
	require.Equal(t, 1, pkFields, "existing primary field should be replaced, not appended")
}

func TestInsertTaskReassignAutoIDForStableIdempotencyKeepsVChannelOrder(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "true"))

	ctx := context.Background()
	oldCache := globalMetaCache
	defer func() { globalMetaCache = oldCache }()

	schema, err := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	})
	require.NoError(t, err)
	globalMetaCache = newInsertTaskIdempotencyMockCache(t, schema, true)
	idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)
	// The stored vchannel order follows pchannel load at allocation time, so it is
	// not lexicographic; the insert must route against it verbatim.
	storedChannels := []string{"ch2", "ch0", "ch1"}

	var firstRowIDs []int64
	var firstRoutes []string
	var firstKey string
	for idx := 0; idx < 2; idx++ {
		chMgr := NewMockChannelsMgr(t)
		chMgr.EXPECT().getVChannels(UniqueID(100)).Return(slices.Clone(storedChannels), nil)
		task := newInsertTaskForIdempotencyAutoIDTest(idAllocator, chMgr)

		require.NoError(t, task.PreExecute(ctx))
		require.Equal(t, storedChannels, task.vChannels)
		require.Equal(t, task.insertMsg.GetRowIDs(), task.result.GetIDs().GetIntId().GetData())
		requireOffsetRoutesToModuloChannels(t, task.result.GetIDs(), storedChannels)
		requireInsertRoutesMatchDeleteRoutes(t, task.result.GetIDs(), task.vChannels)

		routes := rowChannelsByPK(task.result.GetIDs(), storedChannels)
		if idx == 0 {
			firstRowIDs = slices.Clone(task.insertMsg.GetRowIDs())
			firstRoutes = slices.Clone(routes)
			firstKey = task.idempotencyKey
			continue
		}
		require.NotEqual(t, firstRowIDs, task.insertMsg.GetRowIDs(), "second retry should use different auto IDs")
		require.Equal(t, firstKey, task.idempotencyKey)
		require.Equal(t, firstRoutes, routes)
	}
}

// requireInsertRoutesMatchDeleteRoutes asserts that every PK produced by the
// idempotent autoID assignment lands on the vchannel that a later delete of that
// same PK would target: delete hashes against the vchannel list from
// chMgr.getVChannels and indexes it directly (task_delete.go), so the insert must
// use the very same list, unpermuted.
func requireInsertRoutesMatchDeleteRoutes(t *testing.T, ids *schemapb.IDs, vChannels []string) {
	t.Helper()
	hashValues, err := typeutil.HashPK2Channels(ids, vChannels)
	require.NoError(t, err)
	deleteRoutes := make([]string, 0, len(hashValues))
	for _, key := range hashValues {
		deleteRoutes = append(deleteRoutes, vChannels[key])
	}
	require.Equal(t, deleteRoutes, rowChannelsByPK(ids, vChannels))
}

func TestInsertTaskReassignAutoIDForStableIdempotencyErrors(t *testing.T) {
	primary := &schemapb.FieldSchema{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, AutoID: true}
	task := insertTask{
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				NumRows: 2,
				RowIDs:  []int64{1, 2},
			},
		},
	}

	err := task.reassignAutoIDForStableIdempotency(primary, []string{"ch0", "ch1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "idempotency key is required")

	task.idempotencyKey = "stable-key"
	err = task.reassignAutoIDForStableIdempotency(primary, []string{"ch0", "ch1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "id allocator is required")
}

func nonMatchingRowIDsForOffsets(t *testing.T, channels []string, rows int, start int64) []int64 {
	t.Helper()
	rowIDs := make([]int64, rows)
	for offset := range rowIDs {
		rowIDs[offset] = firstInt64IDNotRoutingToOffsetBucket(t, channels, offset, start+int64(offset*10000))
	}
	return rowIDs
}

func requireOffsetRoutesToModuloChannels(t *testing.T, ids *schemapb.IDs, channels []string) {
	t.Helper()
	actualChannels := rowChannelsByPK(ids, channels)
	require.Len(t, actualChannels, len(idsByOffsetsForTest(ids)))
	for offset, channel := range actualChannels {
		require.Equal(t, channels[offset%len(channels)], channel)
	}
}

func firstInt64IDNotRoutingToOffsetBucket(t *testing.T, channels []string, offset int, start int64) int64 {
	t.Helper()
	require.Greater(t, len(channels), 1)
	targetChannel := channels[(offset+1)%len(channels)]
	return firstInt64IDRoutingToChannel(t, channels, targetChannel, start)
}

func firstInt64IDRoutingToChannel(t *testing.T, channels []string, targetChannel string, start int64) int64 {
	t.Helper()
	for id := start; id < start+100000; id++ {
		ids := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{id}}},
		}
		actual := rowChannelsByPK(ids, channels)
		if len(actual) == 1 && actual[0] == targetChannel {
			return id
		}
	}
	t.Fatalf("failed to find an id that routes to %s", targetChannel)
	return 0
}
