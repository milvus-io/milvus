package db

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel/mock"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/require"
)

const (
	noTs          = typeutil.Timestamp(0)
	collID_1      = typeutil.UniqueID(101)
	partitionID_1 = typeutil.UniqueID(500)
	fieldID_1     = typeutil.UniqueID(1000)
	indexID_1     = typeutil.UniqueID(1500)
)

type NoopTransaction struct{}

func (*NoopTransaction) Transaction(ctx context.Context, fn func(txctx context.Context) error) error {
	return fn(ctx)
}

func mockMetaCatalog(petDomain dbmodel.IMetaDomain) *Catalog {
	return NewTableCatalog(&NoopTransaction{}, petDomain)
}

func Test_CreateCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metaDomain := mock.NewMockIMetaDomain(ctrl)
	collectionDb := mock.NewMockICollectionDb(ctrl)
	fieldDb := mock.NewMockIFieldDb(ctrl)
	partitionDb := mock.NewMockIPartitionDb(ctrl)
	channelDb := mock.NewMockICollChannelDb(ctrl)

	metaDomain.EXPECT().CollectionDb(gomock.Any()).Return(collectionDb).AnyTimes()
	metaDomain.EXPECT().FieldDb(gomock.Any()).Return(fieldDb).AnyTimes().AnyTimes()
	metaDomain.EXPECT().PartitionDb(gomock.Any()).Return(partitionDb).AnyTimes()
	metaDomain.EXPECT().CollChannelDb(gomock.Any()).Return(channelDb).AnyTimes()

	// Expectation
	collectionDb.EXPECT().Insert(gomock.Any()).Return(nil)
	fieldDb.EXPECT().Insert(gomock.Any()).Return(nil)
	partitionDb.EXPECT().Insert(gomock.Any()).Return(nil)
	channelDb.EXPECT().Insert(gomock.Any()).Return(nil)

	var collection = &model.Collection{
		CollectionID:     collID_1,
		Name:             "test_collection_name_1",
		AutoID:           false,
		ShardsNum:        2,
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
		Partitions: []*model.Partition{
			{
				PartitionID:   partitionID_1,
				PartitionName: "test_partition_name_1",
			},
		},
		Fields: []*model.Field{
			{
				FieldID:      fieldID_1,
				Name:         "test_field_name_1",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_FloatVector,
				TypeParams:   nil,
				IndexParams:  nil,
				AutoID:       false,
			},
		},
		FieldIDToIndexID: []common.Int64Tuple{
			{
				fieldID_1, indexID_1,
			},
		},
		VirtualChannelNames:  []string{"test_virtual_channel_1"},
		PhysicalChannelNames: []string{"test_physical_channel_1"},
	}

	// actual
	err := mockMetaCatalog(metaDomain).CreateCollection(context.Background(), collection, noTs)
	require.NoError(t, err)
}

func Test_CreateCollection_Rollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metaDomain := mock.NewMockIMetaDomain(ctrl)
	collectionDb := mock.NewMockICollectionDb(ctrl)
	fieldDb := mock.NewMockIFieldDb(ctrl)
	partitionDb := mock.NewMockIPartitionDb(ctrl)
	channelDb := mock.NewMockICollChannelDb(ctrl)

	metaDomain.EXPECT().CollectionDb(gomock.Any()).Return(collectionDb).AnyTimes()
	metaDomain.EXPECT().FieldDb(gomock.Any()).Return(fieldDb).AnyTimes().AnyTimes()
	metaDomain.EXPECT().PartitionDb(gomock.Any()).Return(partitionDb).AnyTimes()
	metaDomain.EXPECT().CollChannelDb(gomock.Any()).Return(channelDb).AnyTimes()

	// Expectation
	collectionDb.EXPECT().Insert(gomock.Any()).Return(errors.New("insert collection error"))

	var collection = &model.Collection{
		CollectionID:     collID_1,
		Name:             "test_collection_name_1",
		AutoID:           false,
		ShardsNum:        2,
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
		Partitions: []*model.Partition{
			{
				PartitionID:   partitionID_1,
				PartitionName: "test_partition_name_1",
			},
		},
		Fields: []*model.Field{
			{
				FieldID:      fieldID_1,
				Name:         "test_field_name_1",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_FloatVector,
				TypeParams:   nil,
				IndexParams:  nil,
				AutoID:       false,
			},
		},
		FieldIDToIndexID: []common.Int64Tuple{
			{
				fieldID_1, indexID_1,
			},
		},
		VirtualChannelNames:  []string{"test_virtual_channel_1"},
		PhysicalChannelNames: []string{"test_physical_channel_1"},
	}

	// actual
	err := mockMetaCatalog(metaDomain).CreateCollection(context.Background(), collection, noTs)
	require.Error(t, err)
}

func Test_GetCollectionByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metaDomain := mock.NewMockIMetaDomain(ctrl)
	collectionDb := mock.NewMockICollectionDb(ctrl)
	fieldDb := mock.NewMockIFieldDb(ctrl)
	partitionDb := mock.NewMockIPartitionDb(ctrl)
	channelDb := mock.NewMockICollChannelDb(ctrl)
	indexDb := mock.NewMockIIndexDb(ctrl)

	metaDomain.EXPECT().CollectionDb(gomock.Any()).Return(collectionDb).AnyTimes()
	metaDomain.EXPECT().FieldDb(gomock.Any()).Return(fieldDb).AnyTimes().AnyTimes()
	metaDomain.EXPECT().PartitionDb(gomock.Any()).Return(partitionDb).AnyTimes()
	metaDomain.EXPECT().CollChannelDb(gomock.Any()).Return(channelDb).AnyTimes()
	metaDomain.EXPECT().IndexDb(gomock.Any()).Return(indexDb).AnyTimes()

	outColl := &dbmodel.Collection{
		CollectionID:   collID_1,
		CollectionName: "test_collection_name_1",
		AutoID:         false,
		StartPosition:  "",
		ShardsNum:      2,
	}
	outField := []*dbmodel.Field{
		{
			FieldID:      fieldID_1,
			FieldName:    "test_field_name_1",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_FloatVector,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       false,
			CollectionID: collID_1,
		},
	}
	outPartition := []*dbmodel.Partition{
		{
			PartitionID:   partitionID_1,
			PartitionName: "test_partition_name_1",
			CollectionID:  collID_1,
		},
	}
	outChannel := []*dbmodel.CollectionChannel{
		{
			CollectionID:    collID_1,
			VirtualChannel:  "test_virtual_channel_1",
			PhysicalChannel: "test_physical_channel_1",
		},
	}
	outIndex := []*dbmodel.Index{
		{
			FieldID:      fieldID_1,
			CollectionID: collID_1,
			IndexID:      indexID_1,
			IndexName:    "test_index_name_1",
			IndexParams:  "",
		},
	}

	collectionDb.EXPECT().GetCidTs("", collID_1, noTs).Return(outColl, nil)
	collectionDb.EXPECT().Get("", collID_1, noTs).Return(outColl, nil)
	fieldDb.EXPECT().GetByCollID("", collID_1, noTs).Return(outField, nil)
	partitionDb.EXPECT().GetByCollID("", collID_1, noTs).Return(outPartition, nil)
	channelDb.EXPECT().GetByCollID("", collID_1, noTs).Return(outChannel, nil)
	indexDb.EXPECT().Get("", collID_1).Return(outIndex, nil)

	out := &model.Collection{
		CollectionID:     collID_1,
		Name:             "test_collection_name_1",
		AutoID:           false,
		ShardsNum:        2,
		ConsistencyLevel: 0,
		Partitions: []*model.Partition{
			{
				PartitionID:   partitionID_1,
				PartitionName: "test_partition_name_1",
			},
		},
		Fields: []*model.Field{
			{
				FieldID:      fieldID_1,
				Name:         "test_field_name_1",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_FloatVector,
				TypeParams:   nil,
				IndexParams:  nil,
				AutoID:       false,
			},
		},
		FieldIDToIndexID: []common.Int64Tuple{
			{
				fieldID_1, indexID_1,
			},
		},
		VirtualChannelNames:  []string{"test_virtual_channel_1"},
		PhysicalChannelNames: []string{"test_physical_channel_1"},
	}

	r, err := mockMetaCatalog(metaDomain).GetCollectionByID(context.Background(), collID_1, noTs)
	require.NoError(t, err)
	require.EqualValues(t, out, r)
}
