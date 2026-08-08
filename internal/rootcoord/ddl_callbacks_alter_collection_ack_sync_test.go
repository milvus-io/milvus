package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func requireAlterCollectionAckSyncUp(t *testing.T, msg message.BroadcastMutableMessage) {
	t.Helper()
	require.Equal(t, message.MessageTypeAlterCollection, msg.MessageType())
	require.True(t, msg.BroadcastHeader().AckSyncUp)
}

func requireAlterCollectionNoAckSyncUp(t *testing.T, msg message.BroadcastMutableMessage) {
	t.Helper()
	require.Equal(t, message.MessageTypeAlterCollection, msg.MessageType())
	require.False(t, msg.BroadcastHeader().AckSyncUp)
}

func expectAlterCollectionBroadcast(t *testing.T, requireBroadcast func(*testing.T, message.BroadcastMutableMessage)) *mock_broadcaster.MockBroadcastAPI {
	t.Helper()
	bc := mock_broadcaster.NewMockBroadcastAPI(t)
	bc.EXPECT().Close().Maybe()
	bc.EXPECT().Broadcast(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msg message.BroadcastMutableMessage) {
			requireBroadcast(t, msg)
		}).
		Return(&types.BroadcastAppendResult{}, nil).
		Once()
	return bc
}

func expectAlterCollectionAckSyncUpBroadcast(t *testing.T) *mock_broadcaster.MockBroadcastAPI {
	t.Helper()
	return expectAlterCollectionBroadcast(t, requireAlterCollectionAckSyncUp)
}

func expectAlterCollectionNoAckSyncUpBroadcast(t *testing.T) *mock_broadcaster.MockBroadcastAPI {
	t.Helper()
	return expectAlterCollectionBroadcast(t, requireAlterCollectionNoAckSyncUp)
}

func TestAlterCollectionPropertyBroadcastSkipsAckSyncUp(t *testing.T) {
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(nil)

	coll := new(DDLCallbacksCollectionFunctionTestSuite).createTestCollection()
	core := newTestCore(withValidMixCoord())
	core.meta = &mockMetaTable{
		GetCollectionByNameFunc: func(ctx context.Context, collectionName string, ts typeutil.Timestamp, allowUnavailable bool) (*model.Collection, error) {
			return coll, nil
		},
		ListAliasesFunc: func(ctx context.Context, dbName string, collectionName string, ts typeutil.Timestamp) ([]string, error) {
			return nil, nil
		},
	}

	bc := expectAlterCollectionNoAckSyncUpBroadcast(t)
	lockMocker := mockey.Mock((*Core).startBroadcastWithAliasOrCollectionLock).Return(bc, nil).Build()
	defer lockMocker.UnPatch()

	err := core.broadcastAlterCollectionForAlterCollection(context.Background(), &milvuspb.AlterCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionDescription, Value: "new description"},
		},
	})
	require.NoError(t, err)
}

func TestAlterCollectionTTLFieldBroadcastMarksSchemaChange(t *testing.T) {
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(nil)

	coll := new(DDLCallbacksCollectionFunctionTestSuite).createTestCollection()
	core := newTestCore(withValidMixCoord())
	core.meta = &mockMetaTable{
		GetCollectionByNameFunc: func(ctx context.Context, collectionName string, ts typeutil.Timestamp, allowUnavailable bool) (*model.Collection, error) {
			return coll, nil
		},
		ListAliasesFunc: func(ctx context.Context, dbName string, collectionName string, ts typeutil.Timestamp) ([]string, error) {
			return nil, nil
		},
	}

	bc := mock_broadcaster.NewMockBroadcastAPI(t)
	bc.EXPECT().Close().Maybe()
	bc.EXPECT().Broadcast(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msg message.BroadcastMutableMessage) {
			requireAlterCollectionAckSyncUp(t, msg)
			alterMsg := message.MustAsBroadcastAlterCollectionMessageV2(msg)
			paths := alterMsg.Header().GetUpdateMask().GetPaths()
			require.True(t, funcutil.SliceContain(paths, message.FieldMaskCollectionProperties))
			require.True(t, funcutil.SliceContain(paths, message.FieldMaskCollectionSchema))

			schema := alterMsg.MustBody().Updates.GetSchema()
			require.NotNil(t, schema)
			require.Equal(t, "text_field", common.CloneKeyValuePairs(schema.GetProperties()).ToMap()[common.CollectionTTLFieldKey])
		}).
		Return(&types.BroadcastAppendResult{}, nil).
		Once()

	lockMocker := mockey.Mock((*Core).startBroadcastWithAliasOrCollectionLock).Return(bc, nil).Build()
	defer lockMocker.UnPatch()

	err := core.broadcastAlterCollectionForAlterCollection(context.Background(), &milvuspb.AlterCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionTTLFieldKey, Value: "text_field"},
		},
	})
	require.NoError(t, err)
}

func TestAlterCollectionSchemaBroadcastUsesAckSyncUp(t *testing.T) {
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(nil)

	ctx := context.Background()
	dbName := "test_db"
	collectionName := "test_collection"
	coll := new(DDLCallbacksCollectionFunctionTestSuite).createTestCollection()

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetCollectionByName(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp, mock.Anything).Return(coll, nil).Maybe()
	meta.EXPECT().ListAliases(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return(nil, nil).Maybe()
	core := newTestCore(withMeta(meta), withValidMixCoord())

	bc := expectAlterCollectionAckSyncUpBroadcast(t)
	lockMocker := mockey.Mock((*Core).startBroadcastWithAliasOrCollectionLock).Return(bc, nil).Build()
	defer lockMocker.UnPatch()

	err := core.broadcastAlterCollectionForAddField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         getFieldSchema("field2"),
	})
	require.NoError(t, err)
}

func TestRenameCollectionBroadcastSkipsAckSyncUp(t *testing.T) {
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(nil)

	ctx := context.Background()
	dbName := "test_db"
	collectionName := "test_collection"
	coll := new(DDLCallbacksCollectionFunctionTestSuite).createTestCollection()
	coll.Name = collectionName

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().CheckIfCollectionRenamable(mock.Anything, dbName, collectionName, dbName, "new_"+collectionName).Return(nil).Once()
	meta.EXPECT().GetDatabaseByName(mock.Anything, dbName, typeutil.MaxTimestamp).Return(&model.Database{ID: coll.DBID, Name: dbName}, nil).Once()
	meta.EXPECT().GetCollectionByName(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp, false).Return(coll, nil).Maybe()
	meta.EXPECT().ListAliases(mock.Anything, dbName, collectionName, typeutil.MaxTimestamp).Return(nil, nil).Maybe()
	core := newTestCore(withMeta(meta), withValidMixCoord())

	bc := expectAlterCollectionNoAckSyncUpBroadcast(t)
	lockMocker := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).
		To(func(context.Context, ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return bc, nil
		}).
		Build()
	defer lockMocker.UnPatch()

	err := core.broadcastAlterCollectionForRenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:  dbName,
		OldName: collectionName,
		NewName: "new_" + collectionName,
	})
	require.NoError(t, err)
}

func TestAlterCollectionFunctionBroadcastUsesAckSyncUp(t *testing.T) {
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(nil)

	suite := new(DDLCallbacksCollectionFunctionTestSuite)
	oldColl := suite.createTestCollection()
	newColl := oldColl.Clone()
	newColl.Description = "changed"

	core := newTestCore(withMeta(&mockMetaTable{
		GetCollectionByNameFunc: func(ctx context.Context, collectionName string, ts typeutil.Timestamp, allowUnavailable bool) (*model.Collection, error) {
			return oldColl, nil
		},
		ListAliasesFunc: func(ctx context.Context, dbName string, collectionName string, ts typeutil.Timestamp) ([]string, error) {
			return nil, nil
		},
	}))

	bc := expectAlterCollectionAckSyncUpBroadcast(t)
	err := callAlterCollection(context.Background(), core, bc, oldColl, newColl, "test_db", "test_collection")
	require.NoError(t, err)
}

func TestAlterCollectionSchemaAckCallbackWaitsForChannelCheckpoint(t *testing.T) {
	mixCoord := imocks.NewMixCoord(t)
	mixCoord.EXPECT().WaitForChannelCheckpoint(mock.Anything, map[string]uint64{
		"v1": 10,
		"v2": 20,
	}).Return(nil).Once()

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil).Once()

	core := newTestCore(withMeta(meta), withMixCoord(mixCoord), withValidProxyManager())
	core.broker = &mockBroker{
		BroadcastAlteredCollectionFunc: func(ctx context.Context, collectionID UniqueID) error {
			require.Equal(t, UniqueID(100), collectionID)
			return nil
		},
	}
	cb := &DDLCallback{Core: core}
	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId:     100,
			UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
			CacheExpirations: &message.CacheExpirations{},
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: &schemapb.CollectionSchema{Name: "test_collection"},
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test"), "v1", "v2"}).
		MustBuildBroadcast()

	err := cb.alterCollectionV2AckCallback(context.Background(), message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsBroadcastAlterCollectionMessageV2(raw),
		Results: map[string]*message.AppendResult{
			funcutil.GetControlChannel("test"): {TimeTick: 5},
			"v1":                               {TimeTick: 10},
			"v2":                               {TimeTick: 20},
		},
	})
	require.NoError(t, err)
}

func TestAlterCollectionNonSchemaAckCallbackSkipsChannelCheckpoint(t *testing.T) {
	mixCoord := imocks.NewMixCoord(t)

	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(nil).Once()

	core := newTestCore(withMeta(meta), withMixCoord(mixCoord), withValidProxyManager())
	core.broker = &mockBroker{
		BroadcastAlteredCollectionFunc: func(ctx context.Context, collectionID UniqueID) error {
			require.Equal(t, UniqueID(100), collectionID)
			return nil
		},
	}
	cb := &DDLCallback{Core: core}
	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId:     100,
			UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionDescription}},
			CacheExpirations: &message.CacheExpirations{},
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Description: "new description",
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test"), "v1"}).
		MustBuildBroadcast()

	err := cb.alterCollectionV2AckCallback(context.Background(), message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsBroadcastAlterCollectionMessageV2(raw),
		Results: map[string]*message.AppendResult{
			funcutil.GetControlChannel("test"): {TimeTick: 5},
			"v1":                               {TimeTick: 10},
		},
	})
	require.NoError(t, err)
	mixCoord.AssertNotCalled(t, "WaitForChannelCheckpoint", mock.Anything, mock.Anything)
}

func TestAlterCollectionSchemaAckCallbackPropagatesChannelCheckpointError(t *testing.T) {
	mixCoord := imocks.NewMixCoord(t)
	waitErr := merr.WrapErrServiceInternal("wait checkpoint failed")
	mixCoord.EXPECT().WaitForChannelCheckpoint(mock.Anything, mock.Anything).Return(waitErr).Once()

	meta := mockrootcoord.NewIMetaTable(t)

	core := newTestCore(withMeta(meta), withMixCoord(mixCoord), withValidProxyManager())
	core.broker = newValidMockBroker()
	cb := &DDLCallback{Core: core}
	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			CollectionId:     100,
			UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
			CacheExpirations: &message.CacheExpirations{},
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{
			Updates: &messagespb.AlterCollectionMessageUpdates{
				Schema: &schemapb.CollectionSchema{Name: "test_collection"},
			},
		}).
		WithBroadcast([]string{funcutil.GetControlChannel("test"), "v1"}).
		MustBuildBroadcast()

	err := cb.alterCollectionV2AckCallback(context.Background(), message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsBroadcastAlterCollectionMessageV2(raw),
		Results: map[string]*message.AppendResult{
			funcutil.GetControlChannel("test"): {TimeTick: 5},
			"v1":                               {TimeTick: 10},
		},
	})
	require.ErrorIs(t, err, waitErr)
	meta.AssertNotCalled(t, "AlterCollection", mock.Anything, mock.Anything)
}
