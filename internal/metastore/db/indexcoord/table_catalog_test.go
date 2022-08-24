package indexcoord

//import (
//	"context"
//	"errors"
//	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
//	"os"
//	"testing"
//
//	"github.com/milvus-io/milvus/internal/metastore"
//	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel/mocks"
//	"github.com/milvus-io/milvus/internal/metastore/model"
//	"github.com/milvus-io/milvus/internal/proto/commonpb"
//	"github.com/milvus-io/milvus/internal/util/contextutil"
//	"github.com/milvus-io/milvus/internal/util/typeutil"
//	"github.com/stretchr/testify/mock"
//	"github.com/stretchr/testify/require"
//)
//
//const (
//	tenantID      = "test_tenant"
//	noTs          = typeutil.Timestamp(0)
//	ts            = typeutil.Timestamp(10)
//	collID1       = typeutil.UniqueID(101)
//	partitionID1  = typeutil.UniqueID(500)
//	fieldID1      = typeutil.UniqueID(1000)
//	indexID1      = typeutil.UniqueID(1500)
//	segmentID1    = typeutil.UniqueID(2000)
//	indexBuildID1 = typeutil.UniqueID(3000)
//
//	collName1  = "test_collection_name_1"
//	collAlias1 = "test_collection_alias_1"
//	collAlias2 = "test_collection_alias_2"
//
//	username = "test_username_1"
//	password = "test_xxx"
//)
//
//var (
//	ctx               context.Context
//	metaDomainMock    *mocks.IMetaDomain
//	collDbMock        *mocks.ICollectionDb
//	fieldDbMock       *mocks.IFieldDb
//	partitionDbMock   *mocks.IPartitionDb
//	collChannelDbMock *mocks.ICollChannelDb
//	indexDbMock       *mocks.IIndexDb
//	aliasDbMock       *mocks.ICollAliasDb
//	segIndexDbMock    *mocks.ISegmentIndexDb
//	userDbMock        *mocks.IUserDb
//
//	mockCatalog *Catalog
//)
//
//// TestMain is the first function executed in current package, we will do some initial here
//func TestMain(m *testing.M) {
//	ctx = contextutil.WithTenantID(context.Background(), tenantID)
//
//	collDbMock = &mocks.ICollectionDb{}
//	fieldDbMock = &mocks.IFieldDb{}
//	partitionDbMock = &mocks.IPartitionDb{}
//	collChannelDbMock = &mocks.ICollChannelDb{}
//	indexDbMock = &mocks.IIndexDb{}
//	aliasDbMock = &mocks.ICollAliasDb{}
//	segIndexDbMock = &mocks.ISegmentIndexDb{}
//	userDbMock = &mocks.IUserDb{}
//
//	metaDomainMock = &mocks.IMetaDomain{}
//	metaDomainMock.On("CollectionDb", ctx).Return(collDbMock)
//	metaDomainMock.On("FieldDb", ctx).Return(fieldDbMock)
//	metaDomainMock.On("PartitionDb", ctx).Return(partitionDbMock)
//	metaDomainMock.On("CollChannelDb", ctx).Return(collChannelDbMock)
//	metaDomainMock.On("IndexDb", ctx).Return(indexDbMock)
//	metaDomainMock.On("CollAliasDb", ctx).Return(aliasDbMock)
//	metaDomainMock.On("SegmentIndexDb", ctx).Return(segIndexDbMock)
//	metaDomainMock.On("UserDb", ctx).Return(userDbMock)
//
//	mockCatalog = mockMetaCatalog(metaDomainMock)
//
//	// m.Run entry for executing tests
//	os.Exit(m.Run())
//}
//
//type NoopTransaction struct{}
//
//func (*NoopTransaction) Transaction(ctx context.Context, fn func(txctx context.Context) error) error {
//	return fn(ctx)
//}
//
//func mockMetaCatalog(petDomain dbmodel.IMetaDomain) *Catalog {
//	return NewTableCatalog(&NoopTransaction{}, petDomain)
//}
//
//func TestTableCatalog_CreateIndex(t *testing.T) {
//	index := &model.Index{
//		CollectionID: collID1,
//		FieldID:      fieldID1,
//		IndexID:      indexID1,
//		IndexName:    "testColl_index_110",
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "test_index_params_k1",
//				Value: "test_index_params_v1",
//			},
//		},
//		SegmentIndexes: map[int64]model.SegmentIndex{
//			segmentID1: {
//				Segment: model.Segment{
//					SegmentID:   segmentID1,
//					PartitionID: partitionID1,
//				},
//				BuildID:    indexBuildID1,
//				CreateTime: 0,
//			},
//		},
//	}
//
//	// expectation
//	indexDbMock.On("Insert", mock.Anything).Return(nil).Once()
//	segIndexDbMock.On("Insert", mock.Anything).Return(nil).Once()
//
//	// actual
//	gotErr := mockCatalog.CreateIndex(ctx, index)
//	require.NoError(t, gotErr)
//}
//
//func TestTableCatalog_CreateIndex_InsertIndexError(t *testing.T) {
//	index := &model.Index{
//		CollectionID: collID1,
//		FieldID:      fieldID1,
//		IndexID:      indexID1,
//		IndexName:    "testColl_index_110",
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "test_index_params_k1",
//				Value: "test_index_params_v1",
//			},
//		},
//	}
//
//	// expectation
//	errTest := errors.New("test error")
//	indexDbMock.On("Insert", mock.Anything).Return(errTest).Once()
//
//	// actual
//	gotErr := mockCatalog.CreateIndex(ctx, index)
//	require.NoError(t, gotErr)
//}
//
//func TestTableCatalog_CreateIndex_InsertSegmentIndexError(t *testing.T) {
//	index := &model.Index{
//		CollectionID: collID1,
//		FieldID:      fieldID1,
//		IndexID:      indexID1,
//		IndexName:    "testColl_index_110",
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "test_index_params_k1",
//				Value: "test_index_params_v1",
//			},
//		},
//		SegmentIndexes: map[int64]model.SegmentIndex{
//			segmentID1: {
//				Segment: model.Segment{
//					SegmentID:   segmentID1,
//					PartitionID: partitionID1,
//				},
//				BuildID:        indexBuildID1,
//				CreateTime:     0,
//				IndexFilePaths: []string{"a\xc5z"},
//			},
//		},
//	}
//
//	// expectation
//	errTest := errors.New("test error")
//	indexDbMock.On("Insert", mock.Anything).Return(nil).Once()
//	segIndexDbMock.On("Insert", mock.Anything).Return(errTest).Once()
//
//	// actual
//	gotErr := mockCatalog.CreateIndex(ctx, index)
//	require.NoError(t, gotErr)
//}
//
//func TestTableCatalog_AlterIndex_AddSegmentIndex(t *testing.T) {
//	oldIndex := &model.Index{
//		CollectionID: collID1,
//		FieldID:      fieldID1,
//		IndexID:      indexID1,
//		IndexName:    "testColl_index_110",
//		CreateTime:   uint64(0),
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "test_index_params_k1",
//				Value: "test_index_params_v1",
//			},
//		},
//		SegmentIndexes: map[int64]model.SegmentIndex{
//			segmentID1: {
//				Segment: model.Segment{
//					SegmentID:   segmentID1,
//					PartitionID: partitionID1,
//				},
//				CreateTime: uint64(0),
//			},
//		},
//	}
//
//	newIndex := &model.Index{
//		CollectionID: collID1,
//		FieldID:      fieldID1,
//		IndexID:      indexID1,
//		IndexName:    "testColl_index_110",
//		CreateTime:   uint64(1011),
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "test_index_params_k1",
//				Value: "test_index_params_v1",
//			},
//		},
//		SegmentIndexes: map[int64]model.SegmentIndex{
//			segmentID1: {
//				Segment: model.Segment{
//					SegmentID:   segmentID1,
//					PartitionID: partitionID1,
//				},
//				BuildID:    indexBuildID1,
//				CreateTime: uint64(1011),
//			},
//		},
//	}
//
//	// expectation
//	segIndexDbMock.On("Upsert", mock.Anything).Return(nil).Once()
//	indexDbMock.On("Update", mock.Anything).Return(nil).Once()
//
//	// actual
//	gotErr := mockCatalog.AlterIndex(ctx, oldIndex, newIndex, metastore.ADD)
//	require.NoError(t, gotErr)
//}
//
//func TestTableCatalog_AlterIndex_AddSegmentIndex_UpsertSegmentIndexError(t *testing.T) {
//	oldIndex := &model.Index{
//		CollectionID: collID1,
//		FieldID:      fieldID1,
//		IndexID:      indexID1,
//		IndexName:    "testColl_index_110",
//		CreateTime:   uint64(0),
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "test_index_params_k1",
//				Value: "test_index_params_v1",
//			},
//		},
//		SegmentIndexes: map[int64]model.SegmentIndex{
//			segmentID1: {
//				Segment: model.Segment{
//					SegmentID:   segmentID1,
//					PartitionID: partitionID1,
//				},
//				CreateTime: uint64(0),
//			},
//		},
//	}
//
//	newIndex := &model.Index{
//		CollectionID: collID1,
//		FieldID:      fieldID1,
//		IndexID:      indexID1,
//		IndexName:    "testColl_index_110",
//		CreateTime:   uint64(1011),
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "test_index_params_k1",
//				Value: "test_index_params_v1",
//			},
//		},
//		SegmentIndexes: map[int64]model.SegmentIndex{
//			segmentID1: {
//				Segment: model.Segment{
//					SegmentID:   segmentID1,
//					PartitionID: partitionID1,
//				},
//				BuildID:    indexBuildID1,
//				CreateTime: uint64(1011),
//			},
//		},
//	}
//
//	// expectation
//	errTest := errors.New("test error")
//	segIndexDbMock.On("Upsert", mock.Anything).Return(errTest).Once()
//
//	// actual
//	gotErr := mockCatalog.AlterIndex(ctx, oldIndex, newIndex, metastore.ADD)
//	require.NoError(t, gotErr)
//}
//
//func TestTableCatalog_DropIndex(t *testing.T) {
//	// expectation
//	indexDbMock.On("MarkDeletedByIndexID", tenantID, indexID1).Return(nil).Once()
//	segIndexDbMock.On("MarkDeletedByIndexID", tenantID, indexID1).Return(nil).Once()
//
//	// actual
//	gotErr := mockCatalog.DropIndex(ctx, 0, indexID1)
//	require.NoError(t, gotErr)
//}
//
//func TestTableCatalog_DropIndex_IndexMarkDeletedError(t *testing.T) {
//	// expectation
//	errTest := errors.New("test error")
//	indexDbMock.On("MarkDeletedByIndexID", tenantID, indexID1).Return(errTest).Once()
//
//	// actual
//	gotErr := mockCatalog.DropIndex(ctx, 0, indexID1)
//	require.NoError(t, gotErr)
//}
//
//func TestTableCatalog_DropIndex_SegmentIndexMarkDeletedError(t *testing.T) {
//	// expectation
//	errTest := errors.New("test error")
//	indexDbMock.On("MarkDeletedByIndexID", tenantID, indexID1).Return(nil).Once()
//	segIndexDbMock.On("MarkDeletedByIndexID", tenantID, indexID1).Return(errTest).Once()
//
//	// actual
//	gotErr := mockCatalog.DropIndex(ctx, 0, indexID1)
//	require.NoError(t, gotErr)
//}
//
////func TestTableCatalog_ListIndexes(t *testing.T) {
////	indexResult := []*dbmodel.IndexResult{
////		{
////			FieldID:        fieldID1,
////			CollectionID:   collID1,
////			IndexID:        indexID1,
////			IndexName:      "test_index_name_1",
////			IndexParams:    "[{\"Key\":\"test_index_params_k1\",\"Value\":\"test_index_params_v1\"}]",
////			SegmentID:      segmentID1,
////			PartitionID:    partitionID1,
////			EnableIndex:    false,
////			IndexBuildID:   indexBuildID1,
////			IndexSize:      0,
////			IndexFilePaths: "[\"test_index_file_path_1\"]",
////		},
////	}
////	out := []*model.Index{
////		{
////			CollectionID: collID1,
////			FieldID:      fieldID1,
////			IndexID:      indexID1,
////			IndexName:    "test_index_name_1",
////			IsDeleted:    false,
////			CreateTime:   0,
////			IndexParams: []*commonpb.KeyValuePair{
////				{
////					Key:   "test_index_params_k1",
////					Value: "test_index_params_v1",
////				},
////			},
////			SegmentIndexes: map[int64]model.SegmentIndex{
////				segmentID1: {
////					Segment: model.Segment{
////						SegmentID:   segmentID1,
////						PartitionID: partitionID1,
////					},
////					BuildID: indexBuildID1,
////					//EnableIndex:    false,
////					CreateTime:     0,
////					IndexFilePaths: []string{"test_index_file_path_1"},
////				},
////			},
////			Extra: nil,
////		},
////	}
////
////	// expectation
////	indexDbMock.On("List", tenantID).Return(indexResult, nil).Once()
////
////	// actual
////	res, gotErr := mockCatalog.ListIndexes(ctx)
////	require.NoError(t, gotErr)
////	require.Equal(t, out, res)
////}
//
//func TestTableCatalog_ListIndexes_SelectIndexError(t *testing.T) {
//	// expectation
//	errTest := errors.New("test error")
//	indexDbMock.On("List", tenantID).Return(nil, errTest).Once()
//
//	// actual
//	res, gotErr := mockCatalog.ListIndexes(ctx)
//	require.Nil(t, res)
//	require.Error(t, gotErr)
//}
