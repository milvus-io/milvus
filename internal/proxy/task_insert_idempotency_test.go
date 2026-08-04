package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestInsertTaskIdempotencyBehavior(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)
	// Retired settings, even when present in an old deployment, cannot gate IK.
	require.NoError(t, Params.Save("streaming.idempotency.enabled", "false"))
	t.Cleanup(func() { _ = Params.Reset("streaming.idempotency.enabled") })
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyMaxKeyLength.Key, "8"))
	schema, err := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	})
	require.NoError(t, err)
	for _, property := range []string{"", "false", "true"} {
		for _, key := range []string{"", "user-key", "too-long-key"} {
			t.Run("property="+property+"/key="+key, func(t *testing.T) {
				var properties []*commonpb.KeyValuePair
				if property != "" {
					properties = []*commonpb.KeyValuePair{{Key: "collection.insert.idempotency.enabled", Value: property}}
				}
				cache := newInsertTaskIdempotencyMockCache(t, schema, properties)
				idAllocator := newInsertTaskIdempotencyIDAllocator(t, context.Background())
				task := newInsertTaskForIdempotencyTest(cache, idAllocator, key)
				err := task.PreExecute(context.Background())
				if len(key) > 8 {
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
					return
				}
				require.NoError(t, err)
				require.Equal(t, key != "", task.idempotencyEnabled)
				require.Equal(t, key, task.idempotencyKey)
				if key == "" {
					require.Nil(t, task.idempotentInsertDecoration())
				} else {
					require.NotNil(t, task.idempotentInsertDecoration())
				}
			})
		}
	}
}

func TestInsertTaskKeylessAutoIDIsNewWrite(t *testing.T) {
	paramtable.Init()
	schema, err := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	})
	require.NoError(t, err)
	cache := newInsertTaskIdempotencyMockCache(t, schema, nil)
	idAllocator := newInsertTaskIdempotencyIDAllocator(t, context.Background())
	var previousIDs []int64
	for i := 0; i < 2; i++ {
		// No channel manager: a keyless request must not enter stable IK routing.
		task := newInsertTaskForIdempotencyAutoIDTest(cache, idAllocator, nil)
		task.idempotencyKey = ""
		require.NoError(t, task.PreExecute(context.Background()))
		require.False(t, task.idempotencyEnabled)
		require.Empty(t, task.idempotencyKey)
		require.Nil(t, task.idempotentInsertDecoration())
		ids := task.result.GetIDs().GetIntId().GetData()
		require.NotEmpty(t, ids)
		require.NotEqual(t, previousIDs, ids)
		previousIDs = ids
	}
}

func TestPrepareIdempotencyKeyEncryptedCollection(t *testing.T) {
	paramtable.Init()
	patch := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
	defer patch.UnPatch()
	properties := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}}
	keyless := &insertTask{}
	require.NoError(t, keyless.prepareIdempotencyKey(properties))
	require.False(t, keyless.idempotencyEnabled)
	keyed := &insertTask{idempotencyKey: "key"}
	require.ErrorIs(t, keyed.prepareIdempotencyKey(properties), merr.ErrParameterInvalid)
}

func TestInsertTaskIdempotencyAutoIDStableShardAssignment(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)

	ctx := context.Background()

	schema, err := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	})
	require.NoError(t, err)
	cache := newInsertTaskIdempotencyMockCache(t, schema, nil)
	idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)
	channels := []string{"ch0", "ch1", "ch2"}
	chMgr := channelmgr.NewChannelsMgr(func(int64) (channelmgr.ChannelInfo, error) {
		return channelmgr.ChannelInfo{VChans: channels, PChans: channels}, nil
	})

	task := newInsertTaskForIdempotencyAutoIDTest(cache, idAllocator, chMgr)
	require.NoError(t, task.PreExecute(ctx))
	require.NotEmpty(t, task.idempotencyKey)
	require.Equal(t, channels, task.vChannels)
	require.Equal(t, task.insertMsg.GetRowIDs(), task.result.GetIDs().GetIntId().GetData())

	actualChannels := rowChannelsByPK(task.result.GetIDs(), channels)
	for offset, channel := range actualChannels {
		require.Equal(t, channels[offset%len(channels)], channel)
	}
}

func TestReassignAutoIDByOffsetChannelsUsesAssignChannelsByPK(t *testing.T) {
	rowIDs1 := []int64{1000, 1001, 1002, 1003, 1004, 1005}
	rowIDs2 := []int64{9000, 9001, 9002, 9003, 9004, 9005}
	channels := []string{"ch0", "ch1", "ch2", "ch3"}

	nextID := int64(100000)
	alloc := func(count uint32) (int64, int64, error) {
		begin := nextID
		nextID += int64(count)
		return begin, nextID, nil
	}
	require.NoError(t, reassignAutoIDByOffsetChannels(rowIDs1, schemapb.DataType_Int64, channels, 0, alloc))
	require.NoError(t, reassignAutoIDByOffsetChannels(rowIDs2, schemapb.DataType_Int64, channels, 0, alloc))

	actualChannels1 := rowChannelsByPK(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: rowIDs1}},
	}, channels)
	actualChannels2 := rowChannelsByPK(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: rowIDs2}},
	}, channels)
	for offset := range actualChannels1 {
		require.Equal(t, channels[offset%len(channels)], actualChannels1[offset])
		require.Equal(t, channels[offset%len(channels)], actualChannels2[offset])
	}
}

func TestBuildInsertWriteUnitIdempotentInsertResult(t *testing.T) {
	extra, err := buildInsertWriteUnitIdempotentInsertResult(
		&schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 11, 12}}},
		},
		[]int{2, 0},
	)
	require.NoError(t, err)
	require.Equal(t, []uint32{2, 0}, extra.GetRowOffsets())
	require.Equal(t, []int64{12, 10}, extra.GetIds().GetIntId().GetData())

	_, err = buildInsertWriteUnitIdempotentInsertResult(&schemapb.IDs{}, []int{0})
	require.Error(t, err)
}

func TestMergeDuplicateInsertResults(t *testing.T) {
	extra, err := anypb.New(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{2, 0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{102, 100}}},
		},
	})
	require.NoError(t, err)

	result := &milvuspb.MutationResult{
		IDs: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{200, 201, 202}}},
		},
	}
	err = mergeDuplicateInsertResults(result, types.AppendResponses{
		Responses: []types.AppendResponse{
			{AppendResult: &types.AppendResult{Extra: extra}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{100, 201, 102}, result.GetIDs().GetIntId().GetData())
}

func TestMergeDuplicateInsertResultsAcrossVChannels(t *testing.T) {
	duplicateExtra1, err := anypb.New(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{4, 1},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{104, 101}}},
		},
	})
	require.NoError(t, err)
	duplicateExtra2, err := anypb.New(&messagespb.IdempotentInsertResult{
		RowOffsets: []uint32{2, 5},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{102, 105}}},
		},
	})
	require.NoError(t, err)

	result := &milvuspb.MutationResult{
		IDs: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{200, 201, 202, 203, 204, 205}}},
		},
	}
	err = mergeDuplicateInsertResults(result, types.AppendResponses{
		Responses: []types.AppendResponse{
			{AppendResult: &types.AppendResult{Extra: duplicateExtra1}},
			{AppendResult: &types.AppendResult{}},
			{AppendResult: &types.AppendResult{Extra: duplicateExtra2}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{200, 101, 102, 203, 104, 105}, result.GetIDs().GetIntId().GetData())
}

func newInsertTaskIdempotencyMockCache(t *testing.T, schema *schemaInfo, properties []*commonpb.KeyValuePair) *MetaCache {
	t.Helper()
	cache := &MetaCache{}
	id := mockey.Mock((*MetaCache).GetCollectionID).Return(UniqueID(100), nil).Build()
	info := mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{
		CollID: 100, DBName: "db", Schema: schema, Properties: properties,
	}, nil).Build()
	t.Cleanup(func() { id.UnPatch() })
	t.Cleanup(func() { info.UnPatch() })
	return cache
}

func resetProxyIdempotencyParams(t *testing.T) {
	t.Helper()
	keys := []string{
		Params.StreamingCfg.IdempotencyMaxKeyLength.Key,
	}
	for _, key := range keys {
		key := key
		require.NoError(t, Params.Reset(key))
		t.Cleanup(func() {
			_ = Params.Reset(key)
		})
	}
}

func newInsertTaskIdempotencyIDAllocator(t *testing.T, _ context.Context) *allocator.IDAllocator {
	t.Helper()
	nextID := int64(1000)
	patch := mockey.Mock((*allocator.IDAllocator).Alloc).To(func(_ *allocator.IDAllocator, count uint32) (int64, int64, error) {
		begin := nextID
		nextID += int64(count)
		return begin, nextID, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return &allocator.IDAllocator{}
}

func newInsertTaskForIdempotencyTest(cache Cache, idAllocator *allocator.IDAllocator, key string) insertTask {
	return insertTask{
		baseTask: baseTask{MetaCache: cache},
		ctx:      context.Background(),
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				DbName:         "db",
				CollectionName: "coll",
				PartitionName:  "_default",
				Version:        msgpb.InsertDataVersion_ColumnBased,
				NumRows:        2,
				FieldsData: []*schemapb.FieldData{
					int64FieldData("pk", 1, []int64{10, 11}),
					int64FieldData("value", 2, []int64{100, 101}),
				},
			},
		},
		idAllocator:     idAllocator,
		idempotencyKey:  key,
		schemaTimestamp: 0,
	}
}

func newInsertTaskForIdempotencyAutoIDTest(cache Cache, idAllocator *allocator.IDAllocator, chMgr channelmgr.ChannelsMgr) insertTask {
	return insertTask{
		baseTask: baseTask{MetaCache: cache},
		ctx:      context.Background(),
		insertMsg: &BaseInsertTask{
			InsertRequest: &msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				DbName:         "db",
				CollectionName: "coll",
				PartitionName:  "_default",
				Version:        msgpb.InsertDataVersion_ColumnBased,
				NumRows:        6,
				FieldsData: []*schemapb.FieldData{
					int64FieldData("value", 2, []int64{100, 101, 102, 103, 104, 105}),
				},
			},
		},
		idAllocator:     idAllocator,
		idempotencyKey:  "autoid-request",
		chMgr:           chMgr,
		schemaTimestamp: 0,
	}
}

func rowChannelsByPK(ids *schemapb.IDs, channels []string) []string {
	offsetsByChannel, _ := assignChannelsByPK(ids, channels, &BaseInsertTask{
		InsertRequest: &msgpb.InsertRequest{},
	})
	rowChannels := make([]string, len(idsByOffsetsForTest(ids)))
	for channel, offsets := range offsetsByChannel {
		for _, offset := range offsets {
			rowChannels[offset] = channel
		}
	}
	return rowChannels
}

func idsByOffsetsForTest(ids *schemapb.IDs) []int64 {
	if ids.GetIntId() != nil {
		return ids.GetIntId().GetData()
	}
	return make([]int64, len(ids.GetStrId().GetData()))
}

func int64FieldData(name string, fieldID int64, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: name,
		FieldId:   fieldID,
		Type:      schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: values},
				},
			},
		},
	}
}
