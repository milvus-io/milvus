package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCollectionInsertIdempotencyEnabled(t *testing.T) {
	require.False(t, collectionInsertIdempotencyEnabled(nil))
	require.False(t, collectionInsertIdempotencyEnabled([]*commonpb.KeyValuePair{
		{Key: common.CollectionInsertIdempotencyEnabledKey, Value: "false"},
	}))
	require.True(t, collectionInsertIdempotencyEnabled([]*commonpb.KeyValuePair{
		{Key: common.CollectionInsertIdempotencyEnabledKey, Value: "true"},
	}))
}

func TestInsertTaskIdempotencyBehavior(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "true"))
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyMaxKeyLength.Key, "1024"))

	ctx := context.Background()
	oldCache := globalMetaCache
	defer func() { globalMetaCache = oldCache }()

	schema := &schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	}
	schemaInfo := newSchemaInfo(schema)

	tests := []struct {
		name        string
		enabled     bool
		explicitKey string
		wantErr     bool
	}{
		{
			name:        "enabled explicit key preserves user key",
			enabled:     true,
			explicitKey: "user-key",
		},
		{
			name:    "enabled without key generates auto key",
			enabled: true,
		},
		{
			name: "disabled without key clears idempotency properties",
		},
		{
			name:        "disabled explicit key rejects",
			explicitKey: "user-key",
			wantErr:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			globalMetaCache = newInsertTaskIdempotencyMockCache(t, schemaInfo, test.enabled)
			idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)

			task := newInsertTaskForIdempotencyTest(idAllocator, test.explicitKey)
			err := task.PreExecute(ctx)
			if test.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.enabled, task.idempotencyEnabled)

			if !test.enabled {
				require.Empty(t, task.idempotencyKey)
				return
			}

			if test.explicitKey == "" {
				require.NotEqual(t, test.explicitKey, task.idempotencyKey)
			} else {
				require.Equal(t, test.explicitKey, task.idempotencyKey)
			}
		})
	}
}

func TestPrepareAutoIdempotencyKeyUsesFieldsBeforeMutation(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "true"))

	schema := &schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
			{Name: "generated", FieldID: 3, DataType: schemapb.DataType_Int64},
		},
	}
	task := newInsertTaskForIdempotencyTest(nil, "")
	task.schema = schema
	properties := []*commonpb.KeyValuePair{
		{Key: common.CollectionInsertIdempotencyEnabledKey, Value: "true"},
	}

	expectedKey, err := canonicalInsertPayloadKey(task.insertMsg.GetNumRows(), task.insertMsg.GetFieldsData(), schema, false)
	require.NoError(t, err)
	require.NoError(t, task.prepareAutoIdempotencyKeyIfEnabled(context.Background(), properties, false))
	require.Equal(t, expectedKey, task.idempotencyKey)

	task.insertMsg.FieldsData = append(task.insertMsg.FieldsData, int64FieldData("generated", 3, []int64{200, 201}))
	mutatedKey, err := canonicalInsertPayloadKey(task.insertMsg.GetNumRows(), task.insertMsg.GetFieldsData(), schema, false)
	require.NoError(t, err)
	require.NotEqual(t, mutatedKey, task.idempotencyKey)
}

func TestPrepareAutoIdempotencyKeyValidatesGeneratedKeyLength(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "true"))
	// A limit below the 64-char SHA256 hex auto key: the client supplied no key, so
	// the proxy must reject its own over-limit auto key here rather than letting it
	// reach the streaming node and fail there with a confusing error.
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyMaxKeyLength.Key, "16"))

	schema := &schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	}
	task := newInsertTaskForIdempotencyTest(nil, "")
	task.schema = schema
	properties := []*commonpb.KeyValuePair{
		{Key: common.CollectionInsertIdempotencyEnabledKey, Value: "true"},
	}

	err := task.prepareAutoIdempotencyKeyIfEnabled(context.Background(), properties, false)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestInsertTaskIdempotencyAutoIDStableShardAssignment(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)
	require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "true"))

	ctx := context.Background()
	oldCache := globalMetaCache
	defer func() { globalMetaCache = oldCache }()

	schema := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	})
	globalMetaCache = newInsertTaskIdempotencyMockCache(t, schema, true)
	idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)
	channels := []string{"ch0", "ch1", "ch2"}
	chMgr := NewMockChannelsMgr(t)
	chMgr.EXPECT().getVChannels(UniqueID(100)).Return(channels, nil)

	task := newInsertTaskForIdempotencyAutoIDTest(idAllocator, chMgr)
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

func TestInsertTaskIdempotencyGlobalConfig(t *testing.T) {
	paramtable.Init()
	resetProxyIdempotencyParams(t)

	ctx := context.Background()
	oldCache := globalMetaCache
	defer func() { globalMetaCache = oldCache }()

	schema := newSchemaInfo(&schemapb.CollectionSchema{
		Name: "coll",
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	})

	t.Run("global disabled without key clears idempotency fields", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "false"))
		globalMetaCache = newInsertTaskIdempotencyMockCache(t, schema, true)
		idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)

		task := newInsertTaskForIdempotencyTest(idAllocator, "")
		require.NoError(t, task.PreExecute(ctx))
		require.False(t, task.idempotencyEnabled)
		require.Empty(t, task.idempotencyKey)
	})

	t.Run("global disabled explicit key rejects", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "false"))
		globalMetaCache = newInsertTaskIdempotencyMockCache(t, schema, true)
		idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)

		task := newInsertTaskForIdempotencyTest(idAllocator, "user-key")
		err := task.PreExecute(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("explicit key length limit", func(t *testing.T) {
		require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyEnabled.Key, "true"))
		require.NoError(t, Params.Save(Params.StreamingCfg.IdempotencyMaxKeyLength.Key, "4"))
		globalMetaCache = newInsertTaskIdempotencyMockCache(t, schema, true)
		idAllocator := newInsertTaskIdempotencyIDAllocator(t, ctx)

		task := newInsertTaskForIdempotencyTest(idAllocator, "too-long")
		err := task.PreExecute(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
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

func TestCanonicalInsertPayloadKey(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "pk", FieldID: 1, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
			{Name: "value", FieldID: 2, DataType: schemapb.DataType_Int64},
		},
	}

	fields1 := []*schemapb.FieldData{
		int64FieldData("value", 2, []int64{10, 20}),
		int64FieldData("pk", 1, []int64{100, 101}),
	}
	fields2 := []*schemapb.FieldData{
		int64FieldData("pk", 1, []int64{200, 201}),
		int64FieldData("value", 2, []int64{10, 20}),
	}

	hash1, err := canonicalInsertPayloadKey(2, fields1, schema, true)
	require.NoError(t, err)
	hash2, err := canonicalInsertPayloadKey(2, fields2, schema, true)
	require.NoError(t, err)
	require.Equal(t, hash1, hash2)
	require.Len(t, hash1, 64)

	fields2[1] = int64FieldData("value", 2, []int64{10, 21})
	hash3, err := canonicalInsertPayloadKey(2, fields2, schema, true)
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash3)

	_, err = canonicalInsertPayloadKey(2, []*schemapb.FieldData{
		int64FieldData("value", 2, []int64{10, 20}),
		int64FieldData("other", 2, []int64{30, 40}),
	}, schema, true)
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate field id 2")

	_, err = canonicalInsertPayloadKey(2, []*schemapb.FieldData{
		int64FieldData("value", 2, []int64{10, 20}),
		int64FieldData("value", 3, []int64{30, 40}),
	}, schema, true)
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate field name")
}

func newInsertTaskIdempotencyMockCache(t *testing.T, schema *schemaInfo, enabled bool) *MockCache {
	t.Helper()

	properties := []*commonpb.KeyValuePair(nil)
	if enabled {
		properties = []*commonpb.KeyValuePair{
			{Key: common.CollectionInsertIdempotencyEnabledKey, Value: "true"},
		}
	}

	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(UniqueID(100), nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{
		collID:     100,
		dbName:     "db",
		schema:     schema,
		properties: properties,
	}, nil)
	cache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schema, nil)
	return cache
}

func resetProxyIdempotencyParams(t *testing.T) {
	t.Helper()
	keys := []string{
		Params.StreamingCfg.IdempotencyEnabled.Key,
		Params.StreamingCfg.IdempotencyWindowTTL.Key,
		Params.StreamingCfg.IdempotencyMinEntriesPerWindow.Key,
		Params.StreamingCfg.IdempotencyMaxEntriesPerWindow.Key,
		Params.StreamingCfg.IdempotencySnapshotInterval.Key,
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

func newInsertTaskIdempotencyIDAllocator(t *testing.T, ctx context.Context) *allocator.IDAllocator {
	t.Helper()

	rc := mocks.NewMockRootCoordClient(t)
	rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
		Status: merr.Success(),
		ID:     1000,
		Count:  100,
	}, nil).Maybe()

	idAllocator, err := allocator.NewIDAllocator(ctx, rc, 0)
	require.NoError(t, err)
	require.NoError(t, idAllocator.Start())
	t.Cleanup(idAllocator.Close)
	return idAllocator
}

func newInsertTaskForIdempotencyTest(idAllocator *allocator.IDAllocator, key string) insertTask {
	return insertTask{
		ctx: context.Background(),
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

func newInsertTaskForIdempotencyAutoIDTest(idAllocator *allocator.IDAllocator, chMgr channelsMgr) insertTask {
	return insertTask{
		ctx: context.Background(),
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
