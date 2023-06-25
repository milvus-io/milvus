package rootcoord

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	tenantID      = "test_tenant"
	noTs          = typeutil.Timestamp(0)
	ts            = typeutil.Timestamp(10)
	collID1       = typeutil.UniqueID(101)
	partitionID1  = typeutil.UniqueID(500)
	fieldID1      = typeutil.UniqueID(1000)
	indexID1      = typeutil.UniqueID(1500)
	segmentID1    = typeutil.UniqueID(2000)
	indexBuildID1 = typeutil.UniqueID(3000)

	testDb = int64(1000)

	collName1  = "test_collection_name_1"
	collAlias1 = "test_collection_alias_1"
	collAlias2 = "test_collection_alias_2"

	username = "test_username_1"
	password = "test_xxx"
)

var (
	ctx               context.Context
	metaDomainMock    *mocks.IMetaDomain
	collDbMock        *mocks.ICollectionDb
	fieldDbMock       *mocks.IFieldDb
	partitionDbMock   *mocks.IPartitionDb
	collChannelDbMock *mocks.ICollChannelDb
	indexDbMock       *mocks.IIndexDb
	aliasDbMock       *mocks.ICollAliasDb
	segIndexDbMock    *mocks.ISegmentIndexDb
	userDbMock        *mocks.IUserDb
	roleDbMock        *mocks.IRoleDb
	userRoleDbMock    *mocks.IUserRoleDb
	grantDbMock       *mocks.IGrantDb
	grantIDDbMock     *mocks.IGrantIDDb

	mockCatalog *Catalog
)

// TestMain is the first function executed in current package, we will do some initial here
func TestMain(m *testing.M) {
	ctx = contextutil.WithTenantID(context.Background(), tenantID)

	collDbMock = &mocks.ICollectionDb{}
	fieldDbMock = &mocks.IFieldDb{}
	partitionDbMock = &mocks.IPartitionDb{}
	collChannelDbMock = &mocks.ICollChannelDb{}
	indexDbMock = &mocks.IIndexDb{}
	aliasDbMock = &mocks.ICollAliasDb{}
	segIndexDbMock = &mocks.ISegmentIndexDb{}
	userDbMock = &mocks.IUserDb{}
	roleDbMock = &mocks.IRoleDb{}
	userRoleDbMock = &mocks.IUserRoleDb{}
	grantDbMock = &mocks.IGrantDb{}
	grantIDDbMock = &mocks.IGrantIDDb{}

	metaDomainMock = &mocks.IMetaDomain{}
	metaDomainMock.On("CollectionDb", ctx).Return(collDbMock)
	metaDomainMock.On("FieldDb", ctx).Return(fieldDbMock)
	metaDomainMock.On("PartitionDb", ctx).Return(partitionDbMock)
	metaDomainMock.On("CollChannelDb", ctx).Return(collChannelDbMock)
	metaDomainMock.On("IndexDb", ctx).Return(indexDbMock)
	metaDomainMock.On("CollAliasDb", ctx).Return(aliasDbMock)
	metaDomainMock.On("SegmentIndexDb", ctx).Return(segIndexDbMock)
	metaDomainMock.On("UserDb", ctx).Return(userDbMock)
	metaDomainMock.On("RoleDb", ctx).Return(roleDbMock)
	metaDomainMock.On("UserRoleDb", ctx).Return(userRoleDbMock)
	metaDomainMock.On("GrantDb", ctx).Return(grantDbMock)
	metaDomainMock.On("GrantIDDb", ctx).Return(grantIDDbMock)

	mockCatalog = mockMetaCatalog(metaDomainMock)

	// m.Run entry for executing tests
	os.Exit(m.Run())
}

type NoopTransaction struct{}

func (*NoopTransaction) Transaction(ctx context.Context, fn func(txctx context.Context) error) error {
	return fn(ctx)
}

func mockMetaCatalog(petDomain dbmodel.IMetaDomain) *Catalog {
	return NewTableCatalog(&NoopTransaction{}, petDomain)
}

func TestTableCatalog_CreateCollection(t *testing.T) {
	coll := &model.Collection{
		CollectionID: collID1,
		Name:         collName1,
		AutoID:       false,
		Fields: []*model.Field{
			{
				FieldID:      fieldID1,
				Name:         "test_field_name_1",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "test_type_params_k1",
						Value: "test_type_params_v1",
					},
					{
						Key:   "test_type_params_k2",
						Value: "test_type_params_v2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "test_index_params_k1",
						Value: "test_index_params_v1",
					},
					{
						Key:   "test_index_params_k2",
						Value: "test_index_params_v2",
					},
				},
			},
		},
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "test_start_position_key1",
				Data: []byte("test_start_position_data1"),
			},
		},
		CreateTime: 0,
		Partitions: []*model.Partition{
			{
				PartitionID:               partitionID1,
				PartitionName:             "test_partition_name_1",
				PartitionCreatedTimestamp: 0,
			},
		},
		VirtualChannelNames: []string{
			fmt.Sprintf("dmChannel_%dv%d", collID1, 0),
			fmt.Sprintf("dmChannel_%dv%d", collID1, 1),
		},
		PhysicalChannelNames: []string{
			funcutil.ToPhysicalChannel(fmt.Sprintf("dmChannel_%dv%d", collID1, 0)),
			funcutil.ToPhysicalChannel(fmt.Sprintf("dmChannel_%dv%d", collID1, 1)),
		},
	}

	// expectation
	collDbMock.On("Insert", mock.Anything).Return(nil).Once()
	fieldDbMock.On("Insert", mock.Anything).Return(nil).Once()
	partitionDbMock.On("Insert", mock.Anything).Return(nil).Once()
	collChannelDbMock.On("Insert", mock.Anything).Return(nil).Once()

	// actual
	gotErr := mockCatalog.CreateCollection(ctx, coll, ts)
	require.Equal(t, nil, gotErr)
}

func TestTableCatalog_CreateCollection_InsertCollError(t *testing.T) {
	coll := &model.Collection{
		CollectionID: collID1,
		Name:         collName1,
		AutoID:       false,
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.CreateCollection(ctx, coll, ts)
	require.Equal(t, errTest, gotErr)
}

func TestTableCatalog_CreateCollection_MarshalStartPositionsError(t *testing.T) {
	coll := &model.Collection{
		CollectionID: collID1,
		Name:         collName1,
		AutoID:       false,
		StartPositions: []*commonpb.KeyDataPair{
			{
				Key:  "test_start_position_key1",
				Data: []byte("\\u002"),
			},
		},
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.CreateCollection(ctx, coll, ts)
	require.Equal(t, errTest, gotErr)
}

func TestTableCatalog_GetCollectionByID(t *testing.T) {
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		AutoID:         true,
		StartPosition:  "",
		Ts:             ts,
	}
	fields := []*dbmodel.Field{
		{
			FieldID:      fieldID1,
			FieldName:    "test_field_name_1",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams:   "",
			IndexParams:  "",
		},
	}
	partitions := []*dbmodel.Partition{
		{
			PartitionID:               partitionID1,
			PartitionName:             "test_partition_name_1",
			PartitionCreatedTimestamp: 0,
		},
	}
	collChannels := []*dbmodel.CollectionChannel{
		{
			TenantID:            tenantID,
			CollectionID:        collID1,
			VirtualChannelName:  "test_virtual_channel_name_1",
			PhysicalChannelName: "test_physical_channel_name_1",
		},
	}
	indexes := []*dbmodel.Index{
		{
			TenantID:     tenantID,
			FieldID:      fieldID1,
			CollectionID: collID1,
			IndexID:      indexID1,
			IndexName:    "test_index_name_1",
			IndexParams:  "",
		},
	}

	// expectation
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: ts}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(coll, nil).Once()
	fieldDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(fields, nil).Once()
	partitionDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(partitions, nil).Once()
	collChannelDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(collChannels, nil).Once()
	indexDbMock.On("Get", tenantID, collID1).Return(indexes, nil).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByID(ctx, util.NonDBID, ts, collID1)
	// collection basic info
	require.Equal(t, nil, gotErr)
	require.Equal(t, coll.TenantID, res.TenantID)
	require.Equal(t, coll.CollectionID, res.CollectionID)
	require.Equal(t, coll.CollectionName, res.Name)
	require.Equal(t, coll.AutoID, res.AutoID)
	require.Equal(t, coll.Ts, res.CreateTime)
	require.Empty(t, res.StartPositions)
	// partitions/fields/channels
	require.NotEmpty(t, res.Partitions)
	require.NotEmpty(t, res.Fields)
	require.NotEmpty(t, res.VirtualChannelNames)
	require.NotEmpty(t, res.PhysicalChannelNames)
}

func TestTableCatalog_GetCollectionByID_UnmarshalStartPositionsError(t *testing.T) {
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		AutoID:         false,
		StartPosition:  "\"Key\":  \"test_start_position_key1\",\"Data\": \"test_start_position_data1\",",
		CreatedAt:      time.UnixMilli(10000),
	}

	// expectation
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: ts}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(coll, nil).Once()
	fieldDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, nil).Once()
	partitionDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, nil).Once()
	collChannelDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, nil).Once()
	indexDbMock.On("Get", tenantID, collID1).Return(nil, nil).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByID(ctx, util.NonDBID, ts, collID1)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_GetCollectionByID_SelectCollError(t *testing.T) {
	// expectation
	errTest := errors.New("select collection error")
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: ts}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByID(ctx, util.NonDBID, ts, collID1)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_GetCollectionByID_SelectFieldError(t *testing.T) {
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		AutoID:         false,
		StartPosition:  "",
		CreatedAt:      time.UnixMilli(10000),
	}

	// expectation
	errTest := errors.New("select fields error")
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: ts}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(coll, nil).Once()
	fieldDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByID(ctx, util.NonDBID, ts, collID1)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_GetCollectionByID_SelectPartitionError(t *testing.T) {
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		AutoID:         false,
		StartPosition:  "",
		CreatedAt:      time.UnixMilli(10000),
	}

	// expectation
	errTest := errors.New("select fields error")
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: ts}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(coll, nil).Once()
	fieldDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, nil).Once()
	partitionDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByID(ctx, util.NonDBID, ts, collID1)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_GetCollectionByID_SelectChannelError(t *testing.T) {
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		AutoID:         false,
		StartPosition:  "",
		CreatedAt:      time.UnixMilli(10000),
	}

	// expectation
	errTest := errors.New("select fields error")
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: ts}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(coll, nil).Once()
	fieldDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, nil).Once()
	partitionDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, nil).Once()
	collChannelDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByID(ctx, util.NonDBID, ts, collID1)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_GetCollectionByName(t *testing.T) {
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		AutoID:         true,
		StartPosition:  "",
		Ts:             ts,
	}
	fields := []*dbmodel.Field{
		{
			FieldID:      fieldID1,
			FieldName:    "test_field_name_1",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams:   "",
			IndexParams:  "",
		},
	}
	partitions := []*dbmodel.Partition{
		{
			PartitionID:               partitionID1,
			PartitionName:             "test_partition_name_1",
			PartitionCreatedTimestamp: 0,
		},
	}
	collChannels := []*dbmodel.CollectionChannel{
		{
			TenantID:            tenantID,
			CollectionID:        collID1,
			VirtualChannelName:  "test_virtual_channel_name_1",
			PhysicalChannelName: "test_physical_channel_name_1",
		},
	}
	indexes := []*dbmodel.Index{
		{
			TenantID:     tenantID,
			FieldID:      fieldID1,
			CollectionID: collID1,
			IndexID:      indexID1,
			IndexName:    "test_index_name_1",
			IndexParams:  "",
		},
	}

	// expectation
	collDbMock.On("GetCollectionIDByName", tenantID, collName1, ts).Return(collID1, nil).Once()
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: ts}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(coll, nil).Once()
	fieldDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(fields, nil).Once()
	partitionDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(partitions, nil).Once()
	collChannelDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(collChannels, nil).Once()
	indexDbMock.On("Get", tenantID, collID1).Return(indexes, nil).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByName(ctx, util.NonDBID, collName1, ts)
	// collection basic info
	require.Equal(t, nil, gotErr)
	require.Equal(t, coll.TenantID, res.TenantID)
	require.Equal(t, coll.CollectionID, res.CollectionID)
	require.Equal(t, coll.CollectionName, res.Name)
	require.Equal(t, coll.AutoID, res.AutoID)
	require.Equal(t, coll.Ts, res.CreateTime)
	require.Empty(t, res.StartPositions)
	// partitions/fields/channels
	require.NotEmpty(t, res.Partitions)
	require.NotEmpty(t, res.Fields)
	require.NotEmpty(t, res.VirtualChannelNames)
	require.NotEmpty(t, res.PhysicalChannelNames)
}

func TestTableCatalog_GetCollectionByName_SelectCollIDError(t *testing.T) {
	// expectation
	errTest := errors.New("select fields error")
	collDbMock.On("GetCollectionIDByName", tenantID, collName1, ts).Return(typeutil.UniqueID(0), errTest).Once()

	// actual
	res, gotErr := mockCatalog.GetCollectionByName(ctx, util.NonDBID, collName1, ts)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_ListCollections(t *testing.T) {
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		AutoID:         true,
		StartPosition:  "",
		Ts:             ts,
	}
	fields := []*dbmodel.Field{
		{
			FieldID:      fieldID1,
			FieldName:    "test_field_name_1",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams:   "",
			IndexParams:  "",
		},
	}
	partitions := []*dbmodel.Partition{
		{
			PartitionID:               partitionID1,
			PartitionName:             "test_partition_name_1",
			PartitionCreatedTimestamp: 0,
		},
	}
	collChannels := []*dbmodel.CollectionChannel{
		{
			TenantID:            tenantID,
			CollectionID:        collID1,
			VirtualChannelName:  "test_virtual_channel_name_1",
			PhysicalChannelName: "test_physical_channel_name_1",
		},
	}
	indexes := []*dbmodel.Index{
		{
			TenantID:     tenantID,
			FieldID:      fieldID1,
			CollectionID: collID1,
			IndexID:      indexID1,
			IndexName:    "test_index_name_1",
			IndexParams:  "",
		},
	}

	// expectation
	collDbMock.On("ListCollectionIDTs", tenantID, ts).Return([]*dbmodel.Collection{{CollectionID: collID1, Ts: ts}}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, ts).Return(coll, nil).Once()
	fieldDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(fields, nil).Once()
	partitionDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(partitions, nil).Once()
	collChannelDbMock.On("GetByCollectionID", tenantID, collID1, ts).Return(collChannels, nil).Once()
	indexDbMock.On("Get", tenantID, collID1).Return(indexes, nil).Once()

	// actual
	res, gotErr := mockCatalog.ListCollections(ctx, util.NonDBID, ts)
	// collection basic info
	require.Equal(t, nil, gotErr)
	require.Equal(t, 1, len(res))
	require.Equal(t, coll.TenantID, res[0].TenantID)
	require.Equal(t, coll.CollectionID, res[0].CollectionID)
	require.Equal(t, coll.CollectionName, res[0].Name)
	require.Equal(t, coll.AutoID, res[0].AutoID)
	require.Equal(t, coll.Ts, res[0].CreateTime)
	require.Empty(t, res[0].StartPositions)
	// partitions/fields/channels
	require.NotEmpty(t, res[0].Partitions)
	require.NotEmpty(t, res[0].Fields)
	require.NotEmpty(t, res[0].VirtualChannelNames)
	require.NotEmpty(t, res[0].PhysicalChannelNames)
}

func TestTableCatalog_CollectionExists(t *testing.T) {
	resultTs := typeutil.Timestamp(5)
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
	}

	// expectation
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: resultTs}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, resultTs).Return(coll, nil).Once()

	// actual
	res := mockCatalog.CollectionExists(ctx, util.NonDBID, collID1, ts)
	require.True(t, res)
}

func TestTableCatalog_CollectionExists_IsDeletedTrue(t *testing.T) {
	resultTs := typeutil.Timestamp(5)
	coll := &dbmodel.Collection{
		TenantID:       tenantID,
		CollectionID:   collID1,
		CollectionName: collName1,
		IsDeleted:      true,
	}

	// expectation
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: resultTs}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, resultTs).Return(coll, nil).Once()

	// actual
	res := mockCatalog.CollectionExists(ctx, util.NonDBID, collID1, ts)
	require.False(t, res)
}

func TestTableCatalog_CollectionExists_CollNotExists(t *testing.T) {
	resultTs := typeutil.Timestamp(5)

	// expectation
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(&dbmodel.Collection{CollectionID: collID1, Ts: resultTs}, nil).Once()
	collDbMock.On("Get", tenantID, collID1, resultTs).Return(nil, nil).Once()

	// actual
	res := mockCatalog.CollectionExists(ctx, util.NonDBID, collID1, ts)
	require.False(t, res)
}

func TestTableCatalog_CollectionExists_GetCidTsError(t *testing.T) {
	// expectation
	errTest := errors.New("select error")
	collDbMock.On("GetCollectionIDTs", tenantID, collID1, ts).Return(nil, errTest).Once()

	// actual
	res := mockCatalog.CollectionExists(ctx, util.NonDBID, collID1, ts)
	require.False(t, res)
}

func TestTableCatalog_DropCollection_TsNot0(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		Aliases:      []string{collAlias1, collAlias2},
	}
	inColl := &dbmodel.Collection{
		TenantID:     tenantID,
		CollectionID: coll.CollectionID,
		Ts:           ts,
		IsDeleted:    true,
	}
	inAliases := []*dbmodel.CollectionAlias{
		{
			TenantID:        tenantID,
			CollectionID:    coll.CollectionID,
			CollectionAlias: coll.Aliases[0],
			Ts:              ts,
			IsDeleted:       true,
		},
		{
			TenantID:        tenantID,
			CollectionID:    coll.CollectionID,
			CollectionAlias: coll.Aliases[1],
			Ts:              ts,
			IsDeleted:       true,
		},
	}
	inChannels := []*dbmodel.CollectionChannel{
		{
			TenantID:     tenantID,
			CollectionID: coll.CollectionID,
			Ts:           ts,
			IsDeleted:    true,
		},
	}
	inFields := []*dbmodel.Field{
		{
			TenantID:     tenantID,
			CollectionID: coll.CollectionID,
			Ts:           ts,
			IsDeleted:    true,
		},
	}
	inPartitions := []*dbmodel.Partition{
		{
			TenantID:     tenantID,
			CollectionID: coll.CollectionID,
			Ts:           ts,
			IsDeleted:    true,
		},
	}

	// expectation
	collDbMock.On("Insert", inColl).Return(nil).Once()
	aliasDbMock.On("Insert", inAliases).Return(nil).Once()
	collChannelDbMock.On("Insert", inChannels).Return(nil).Once()
	fieldDbMock.On("Insert", inFields).Return(nil).Once()
	partitionDbMock.On("Insert", inPartitions).Return(nil).Once()
	indexDbMock.On("MarkDeletedByCollectionID", tenantID, coll.CollectionID).Return(nil).Once()
	segIndexDbMock.On("MarkDeletedByCollectionID", tenantID, coll.CollectionID).Return(nil).Once()

	// actual
	gotErr := mockCatalog.DropCollection(ctx, coll, ts)
	require.NoError(t, gotErr)
}

func TestTableCatalog_DropCollection_TsNot0_CollInsertError(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		Aliases:      []string{collAlias1, collAlias2},
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropCollection(ctx, coll, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropCollection_TsNot0_AliasInsertError(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		Aliases:      []string{collAlias1, collAlias2},
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Insert", mock.Anything).Return(nil).Once()
	aliasDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropCollection(ctx, coll, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropCollection_TsNot0_ChannelInsertError(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		Aliases:      []string{collAlias1, collAlias2},
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Insert", mock.Anything).Return(nil).Once()
	aliasDbMock.On("Insert", mock.Anything).Return(nil).Once()
	collChannelDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropCollection(ctx, coll, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropCollection_TsNot0_FieldInsertError(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		Aliases:      []string{collAlias1, collAlias2},
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Insert", mock.Anything).Return(nil).Once()
	aliasDbMock.On("Insert", mock.Anything).Return(nil).Once()
	collChannelDbMock.On("Insert", mock.Anything).Return(nil).Once()
	fieldDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropCollection(ctx, coll, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropCollection_TsNot0_PartitionInsertError(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		Aliases:      []string{collAlias1, collAlias2},
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Insert", mock.Anything).Return(nil).Once()
	aliasDbMock.On("Insert", mock.Anything).Return(nil).Once()
	collChannelDbMock.On("Insert", mock.Anything).Return(nil).Once()
	fieldDbMock.On("Insert", mock.Anything).Return(nil).Once()
	partitionDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropCollection(ctx, coll, ts)
	require.Error(t, gotErr)
}

func TestCatalog_AlterCollection(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		State:        pb.CollectionState_CollectionCreated,
		Aliases:      []string{collAlias1, collAlias2},
	}
	newColl := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		State:        pb.CollectionState_CollectionDropping,
		Aliases:      []string{collAlias1, collAlias2},
	}

	collDbMock.On("Update", mock.Anything).Return(nil).Once()

	gotErr := mockCatalog.AlterCollection(ctx, coll, newColl, metastore.MODIFY, ts)
	require.NoError(t, gotErr)
}

func TestTableCatalog_AlterCollection_TsNot0_AlterTypeError(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		State:        pb.CollectionState_CollectionCreated,
		Aliases:      []string{collAlias1, collAlias2},
	}

	gotErr := mockCatalog.AlterCollection(ctx, coll, coll, metastore.ADD, ts)
	require.Error(t, gotErr)

	gotErr = mockCatalog.AlterCollection(ctx, coll, coll, metastore.DELETE, ts)
	require.Error(t, gotErr)
}

func TestCatalog_AlterCollection_TsNot0_CollInsertError(t *testing.T) {
	coll := &model.Collection{
		TenantID:     tenantID,
		CollectionID: collID1,
		Name:         collName1,
		State:        pb.CollectionState_CollectionCreated,
		Aliases:      []string{collAlias1, collAlias2},
	}

	// expectation
	errTest := errors.New("test error")
	collDbMock.On("Update", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.AlterCollection(ctx, coll, coll, metastore.MODIFY, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_CreatePartition(t *testing.T) {
	partition := &model.Partition{
		PartitionID:               partitionID1,
		PartitionName:             "test_partition_name_1",
		PartitionCreatedTimestamp: 0,
		CollectionID:              collID1,
	}

	// expectation
	partitionDbMock.On("Insert", mock.Anything).Return(nil).Once()

	// actual
	gotErr := mockCatalog.CreatePartition(ctx, util.NonDBID, partition, ts)
	require.Equal(t, nil, gotErr)
}

func TestTableCatalog_CreatePartition_InsertPartitionError(t *testing.T) {
	partition := &model.Partition{
		PartitionID:               partitionID1,
		PartitionName:             "test_partition_name_1",
		PartitionCreatedTimestamp: 0,
		CollectionID:              collID1,
	}

	// expectation
	errTest := errors.New("test error")
	partitionDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.CreatePartition(ctx, util.NonDBID, partition, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropPartition_TsNot0(t *testing.T) {
	// expectation
	partitionDbMock.On("Insert", mock.Anything).Return(nil).Once()

	// actual
	gotErr := mockCatalog.DropPartition(ctx, util.NonDBID, collID1, partitionID1, ts)
	require.NoError(t, gotErr)
}

func TestTableCatalog_DropPartition_TsNot0_PartitionInsertError(t *testing.T) {
	// expectation
	errTest := errors.New("test error")
	partitionDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropPartition(ctx, util.NonDBID, collID1, partitionID1, ts)
	require.Error(t, gotErr)
}

func TestCatalog_AlterPartition(t *testing.T) {
	partition := &model.Partition{
		PartitionID:               partitionID1,
		PartitionName:             "test_partition_name_1",
		PartitionCreatedTimestamp: 1,
		CollectionID:              collID1,
		State:                     pb.PartitionState_PartitionCreated,
	}
	newPartition := &model.Partition{
		PartitionID:               partitionID1,
		PartitionName:             "test_partition_name_1",
		PartitionCreatedTimestamp: 1,
		CollectionID:              collID1,
		State:                     pb.PartitionState_PartitionDropping,
	}

	partitionDbMock.On("Update", mock.Anything).Return(nil).Once()

	gotErr := mockCatalog.AlterPartition(ctx, util.NonDBID, partition, newPartition, metastore.MODIFY, ts)
	require.NoError(t, gotErr)
}

func TestCatalog_AlterPartition_TsNot0_AlterTypeError(t *testing.T) {
	partition := &model.Partition{
		PartitionID:               partitionID1,
		PartitionName:             "test_partition_name_1",
		PartitionCreatedTimestamp: 1,
		CollectionID:              collID1,
		State:                     pb.PartitionState_PartitionCreated,
	}

	gotErr := mockCatalog.AlterPartition(ctx, util.NonDBID, partition, partition, metastore.ADD, ts)
	require.Error(t, gotErr)

	gotErr = mockCatalog.AlterPartition(ctx, util.NonDBID, partition, partition, metastore.DELETE, ts)
	require.Error(t, gotErr)
}

func TestCatalog_AlterPartition_TsNot0_PartitionInsertError(t *testing.T) {
	partition := &model.Partition{
		PartitionID:               partitionID1,
		PartitionName:             "test_partition_name_1",
		PartitionCreatedTimestamp: 1,
		CollectionID:              collID1,
		State:                     pb.PartitionState_PartitionCreated,
	}

	// expectation
	errTest := errors.New("test error")
	partitionDbMock.On("Update", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.AlterPartition(ctx, util.NonDBID, partition, partition, metastore.MODIFY, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_CreateAlias(t *testing.T) {
	alias := &model.Alias{
		CollectionID: collID1,
		Name:         collAlias1,
	}

	// expectation
	aliasDbMock.On("Insert", mock.Anything).Return(nil).Once()

	// actual
	gotErr := mockCatalog.CreateAlias(ctx, alias, ts)
	require.NoError(t, gotErr)
}

func TestTableCatalog_CreateAlias_InsertAliasError(t *testing.T) {
	alias := &model.Alias{
		CollectionID: collID1,
		Name:         collAlias1,
	}

	// expectation
	errTest := errors.New("test error")
	aliasDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.CreateAlias(ctx, alias, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropAlias_TsNot0(t *testing.T) {
	// expectation
	aliasDbMock.On("GetCollectionIDByAlias", tenantID, collAlias1, ts).Return(collID1, nil).Once()
	aliasDbMock.On("Insert", mock.Anything).Return(nil).Once()

	// actual
	gotErr := mockCatalog.DropAlias(ctx, testDb, collAlias1, ts)
	require.NoError(t, gotErr)
}

func TestTableCatalog_DropAlias_TsNot0_SelectCollectionIDByAliasError(t *testing.T) {
	// expectation
	errTest := errors.New("test error")
	aliasDbMock.On("GetCollectionIDByAlias", tenantID, collAlias1, ts).Return(typeutil.UniqueID(0), errTest).Once()

	// actual
	gotErr := mockCatalog.DropAlias(ctx, testDb, collAlias1, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropAlias_TsNot0_InsertIndexError(t *testing.T) {
	// expectation
	errTest := errors.New("test error")
	aliasDbMock.On("GetCollectionIDByAlias", tenantID, collAlias1, ts).Return(collID1, nil).Once()
	aliasDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropAlias(ctx, testDb, collAlias1, ts)
	require.Error(t, gotErr)
}

func TestTableCatalog_AlterAlias_TsNot0(t *testing.T) {
	alias := &model.Alias{
		CollectionID: collID1,
		Name:         collAlias1,
	}

	// expectation
	aliasDbMock.On("Insert", mock.Anything).Return(nil).Once()

	// actual
	gotErr := mockCatalog.AlterAlias(ctx, alias, ts)
	require.NoError(t, gotErr)
}

func TestTableCatalog_ListAliases(t *testing.T) {
	out := []*model.Alias{
		{
			CollectionID: collID1,
			Name:         collAlias1,
		},
	}
	collAliases := []*dbmodel.CollectionAlias{
		{
			CollectionID:    collID1,
			CollectionAlias: collAlias1,
		},
	}

	// expectation
	cidTsPairs := []*dbmodel.CollectionAlias{{CollectionID: collID1, Ts: ts}}
	aliasDbMock.On("ListCollectionIDTs", tenantID, ts).Return(cidTsPairs, nil).Once()
	aliasDbMock.On("List", tenantID, cidTsPairs).Return(collAliases, nil).Once()

	// actual
	res, gotErr := mockCatalog.ListAliases(ctx, testDb, ts)
	require.Equal(t, nil, gotErr)
	require.Equal(t, out, res)
}

func TestTableCatalog_ListAliases_NoResult(t *testing.T) {
	// expectation
	aliasDbMock.On("ListCollectionIDTs", tenantID, ts).Return(nil, nil).Once()

	// actual
	res, gotErr := mockCatalog.ListAliases(ctx, testDb, ts)
	require.Equal(t, nil, gotErr)
	require.Empty(t, res)
}

func TestTableCatalog_ListAliases_ListCidTsError(t *testing.T) {
	// expectation
	errTest := errors.New("test error")
	aliasDbMock.On("ListCollectionIDTs", tenantID, ts).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.ListAliases(ctx, testDb, ts)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_ListAliases_SelectAliasError(t *testing.T) {
	// expectation
	cidTsPairs := []*dbmodel.CollectionAlias{{CollectionID: collID1, Ts: ts}}
	errTest := errors.New("test error")
	aliasDbMock.On("ListCollectionIDTs", tenantID, ts).Return(cidTsPairs, nil).Once()
	aliasDbMock.On("List", tenantID, mock.Anything).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.ListAliases(ctx, testDb, ts)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_GetCredential(t *testing.T) {
	out := &model.Credential{
		Username:          username,
		EncryptedPassword: password,
	}
	user := &dbmodel.User{
		Username:          username,
		EncryptedPassword: password,
	}

	// expectation
	userDbMock.On("GetByUsername", tenantID, username).Return(user, nil).Once()

	// actual
	res, gotErr := mockCatalog.GetCredential(ctx, username)
	require.NoError(t, gotErr)
	require.Equal(t, out, res)
}

func TestTableCatalog_GetCredential_SelectUserError(t *testing.T) {
	// expectation
	errTest := errors.New("test error")
	userDbMock.On("GetByUsername", tenantID, username).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.GetCredential(ctx, username)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_CreateCredential(t *testing.T) {
	in := &model.Credential{
		Username:          username,
		EncryptedPassword: password,
	}

	// expectation
	userDbMock.On("Insert", mock.Anything).Return(nil).Once()

	// actual
	gotErr := mockCatalog.CreateCredential(ctx, in)
	require.NoError(t, gotErr)
}

func TestTableCatalog_CreateCredential_InsertUserError(t *testing.T) {
	in := &model.Credential{
		Username:          username,
		EncryptedPassword: password,
	}

	// expectation
	errTest := errors.New("test error")
	userDbMock.On("Insert", mock.Anything).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.CreateCredential(ctx, in)
	require.Error(t, gotErr)
}

func TestTableCatalog_AlterCredential(t *testing.T) {
	in := &model.Credential{
		Username:          username,
		EncryptedPassword: password,
	}

	// expectation
	userDbMock.On("UpdatePassword", tenantID, username, password).Return(nil).Once()

	// actual
	gotErr := mockCatalog.AlterCredential(ctx, in)
	require.NoError(t, gotErr)
}

func TestTableCatalog_AlterCredential_Error(t *testing.T) {
	in := &model.Credential{
		Username:          username,
		EncryptedPassword: password,
	}

	// expectation
	errTest := errors.New("test error")
	userDbMock.On("UpdatePassword", tenantID, username, password).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.AlterCredential(ctx, in)
	require.Error(t, gotErr)
}

func TestTableCatalog_DropCredential(t *testing.T) {
	// expectation
	userDbMock.On("MarkDeletedByUsername", tenantID, username).Return(nil).Once()

	// actual
	gotErr := mockCatalog.DropCredential(ctx, username)
	require.NoError(t, gotErr)
}

func TestTableCatalog_DropCredential_MarkUserDeletedError(t *testing.T) {
	// expectation
	errTest := errors.New("test error")
	userDbMock.On("MarkDeletedByUsername", tenantID, username).Return(errTest).Once()

	// actual
	gotErr := mockCatalog.DropCredential(ctx, username)
	require.Error(t, gotErr)
}

func TestTableCatalog_ListCredentials(t *testing.T) {
	user := &dbmodel.User{
		Username:          username,
		EncryptedPassword: password,
	}

	// expectation
	userDbMock.On("ListUser", tenantID).Return([]*dbmodel.User{user}, nil).Once()

	// actual
	res, gotErr := mockCatalog.ListCredentials(ctx)
	require.NoError(t, gotErr)
	require.Equal(t, []string{username}, res)
}

func TestTableCatalog_ListCredentials_SelectUsernamesError(t *testing.T) {
	// expectation
	errTest := errors.New("test error")
	userDbMock.On("ListUser", tenantID).Return(nil, errTest).Once()

	// actual
	res, gotErr := mockCatalog.ListCredentials(ctx)
	require.Nil(t, res)
	require.Error(t, gotErr)
}

func TestTableCatalog_CreateRole(t *testing.T) {
	var (
		roleName = "foo"
		err      error
	)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{}, nil).Once()
	roleDbMock.On("Insert", mock.Anything).Return(nil).Once()
	err = mockCatalog.CreateRole(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName})
	require.NoError(t, err)
}

func TestTableCatalog_CreateRole_Error(t *testing.T) {
	var (
		roleName = "foo"
		err      error
	)
	roleDbMock.On("GetRoles", tenantID, roleName).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.CreateRole(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName})
	require.Error(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: 1}}}, nil).Once()
	err = mockCatalog.CreateRole(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName})
	require.Equal(t, true, common.IsIgnorableError(err))

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{}, nil).Once()
	roleDbMock.On("Insert", mock.Anything).Return(errors.New("test error")).Once()
	err = mockCatalog.CreateRole(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName})
	require.Error(t, err)
}

func TestTableCatalog_DropRole(t *testing.T) {
	var (
		roleName = "foo"
		err      error
	)

	roleDbMock.On("Delete", tenantID, roleName).Return(nil).Once()
	err = mockCatalog.DropRole(ctx, tenantID, roleName)
	require.NoError(t, err)
}

func TestTableCatalog_DropRole_Error(t *testing.T) {
	var (
		roleName = "foo"
		err      error
	)

	roleDbMock.On("Delete", tenantID, roleName).Return(errors.New("test error")).Once()
	err = mockCatalog.DropRole(ctx, tenantID, roleName)
	require.Error(t, err)
}

func TestTableCatalog_AlterUserRole(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		roleID   = 10
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{}, nil).Once()
	userRoleDbMock.On("Insert", mock.Anything).Return(nil).Once()

	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_AddUserToRole)
	require.NoError(t, err)
}

func TestTableCatalog_AlterUserRole_GetUserIDError(t *testing.T) {
	var (
		username = "foo"
		roleName = "fo"
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_AddUserToRole)
	require.Error(t, err)
}

func TestTableCatalog_AlterUserRole_GetRoleIDError(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_AddUserToRole)
	require.Error(t, err)
}

func TestTableCatalog_AlterUserRole_GetUserRoleError(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		roleID   = 10
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{}, errors.New("test error")).Once()
	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_AddUserToRole)
	require.Error(t, err)
}

func TestTableCatalog_AlterUserRole_RepeatUserRole(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		roleID   = 10
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{{}}, nil).Once()
	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_AddUserToRole)
	require.Error(t, err)
	require.Equal(t, true, common.IsIgnorableError(err))
}

func TestTableCatalog_AlterUserRole_InsertError(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		roleID   = 10
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{}, nil).Once()
	userRoleDbMock.On("Insert", mock.Anything).Return(errors.New("test error")).Once()

	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_AddUserToRole)
	require.Error(t, err)
}

func TestTableCatalog_AlterUserRole_Delete(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		roleID   = 10
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{{}}, nil).Once()
	userRoleDbMock.On("Delete", tenantID, int64(userID), int64(roleID)).Return(nil).Once()

	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_RemoveUserFromRole)
	require.NoError(t, err)
}

func TestTableCatalog_AlterUserRole_DeleteError(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		roleID   = 10
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{}, nil).Once()

	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_RemoveUserFromRole)
	require.Error(t, err)
	require.True(t, common.IsIgnorableError(err))

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{{}}, nil).Once()
	userRoleDbMock.On("Delete", tenantID, int64(userID), int64(roleID)).Return(errors.New("test error")).Once()

	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, milvuspb.OperateUserRoleType_RemoveUserFromRole)
	require.Error(t, err)
}

func TestTableCatalog_AlterUserRole_InvalidType(t *testing.T) {
	var (
		username = "foo"
		userID   = 100
		roleName = "fo"
		roleID   = 10
		err      error
	)

	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: int64(userID)}, nil).Once()
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: int64(roleID)}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(userID), int64(roleID)).Return([]*dbmodel.UserRole{{}}, nil).Once()

	err = mockCatalog.AlterUserRole(ctx, tenantID, &milvuspb.UserEntity{Name: username}, &milvuspb.RoleEntity{Name: roleName}, 100)
	require.Error(t, err)
}

func TestTableCatalog_ListRole_AllRole(t *testing.T) {
	var (
		roleName1 = "foo1"
		roleName2 = "foo2"
		result    []*milvuspb.RoleResult
		err       error
	)

	roleDbMock.On("GetRoles", tenantID, "").Return([]*dbmodel.Role{{Name: roleName1}, {Name: roleName2}}, nil).Once()

	result, err = mockCatalog.ListRole(ctx, tenantID, nil, false)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, roleName1, result[0].Role.Name)
}

func TestTableCatalog_ListRole_AllRole_IncludeUserInfo(t *testing.T) {
	var (
		roleName1 = "foo1"
		username1 = "fo1"
		username2 = "fo2"
		roleName2 = "foo2"
		result    []*milvuspb.RoleResult
		err       error
	)

	roleDbMock.On("GetRoles", tenantID, "").Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: 1}, Name: roleName1}, {Base: dbmodel.Base{ID: 10}, Name: roleName2}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(0), int64(1)).Return([]*dbmodel.UserRole{{User: dbmodel.User{Username: username1}}, {User: dbmodel.User{Username: username2}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(0), int64(10)).Return([]*dbmodel.UserRole{}, nil).Once()

	result, err = mockCatalog.ListRole(ctx, tenantID, nil, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, roleName1, result[0].Role.Name)
	require.Equal(t, 2, len(result[0].Users))
	require.Equal(t, username1, result[0].Users[0].Name)
	require.Equal(t, username2, result[0].Users[1].Name)
	require.Equal(t, roleName2, result[1].Role.Name)
}

func TestTableCatalog_ListRole_AllRole_Empty(t *testing.T) {
	var (
		result []*milvuspb.RoleResult
		err    error
	)

	roleDbMock.On("GetRoles", tenantID, "").Return([]*dbmodel.Role{}, nil).Once()

	result, err = mockCatalog.ListRole(ctx, tenantID, nil, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(result))
}

func TestTableCatalog_ListRole_OneRole(t *testing.T) {
	var (
		roleName1 = "foo1"
		result    []*milvuspb.RoleResult
		err       error
	)

	roleDbMock.On("GetRoles", tenantID, roleName1).Return([]*dbmodel.Role{{Name: roleName1}}, nil).Once()

	result, err = mockCatalog.ListRole(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName1}, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, roleName1, result[0].Role.Name)
}

func TestTableCatalog_ListRole_OneRole_IncludeUserInfo(t *testing.T) {
	var (
		roleName1 = "foo1"
		username1 = "fo1"
		username2 = "fo2"
		result    []*milvuspb.RoleResult
		err       error
	)

	roleDbMock.On("GetRoles", tenantID, roleName1).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: 1}, Name: roleName1}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(0), int64(1)).Return([]*dbmodel.UserRole{{User: dbmodel.User{Username: username1}}, {User: dbmodel.User{Username: username2}}}, nil).Once()

	result, err = mockCatalog.ListRole(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName1}, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, roleName1, result[0].Role.Name)
	require.Equal(t, 2, len(result[0].Users))
	require.Equal(t, username1, result[0].Users[0].Name)
	require.Equal(t, username2, result[0].Users[1].Name)
}

func TestTableCatalog_ListRole_OneRole_Empty(t *testing.T) {
	var (
		roleName = "foo"
		err      error
	)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{}, nil).Once()

	_, err = mockCatalog.ListRole(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName}, false)
	require.Error(t, err)
}

func TestTableCatalog_ListRole_GetRolesError(t *testing.T) {
	roleDbMock.On("GetRoles", tenantID, "").Return(nil, errors.New("test error")).Once()

	_, err := mockCatalog.ListRole(ctx, tenantID, nil, false)
	require.Error(t, err)
}

func TestTableCatalog_ListRole_GetUserRolesError(t *testing.T) {
	roleDbMock.On("GetRoles", tenantID, "").Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: 1}, Name: "foo"}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(0), int64(1)).Return(nil, errors.New("test error")).Once()
	_, err := mockCatalog.ListRole(ctx, tenantID, nil, true)
	require.Error(t, err)
}

func TestTableCatalog_ListUser_AllUser(t *testing.T) {
	var (
		username1 = "foo1"
		username2 = "foo2"
		result    []*milvuspb.UserResult
		err       error
	)

	userDbMock.On("ListUser", tenantID).Return([]*dbmodel.User{{Username: username1}, {Username: username2}}, nil).Once()

	result, err = mockCatalog.ListUser(ctx, tenantID, nil, false)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, username1, result[0].User.Name)
}

func TestTableCatalog_ListUser_AllUser_IncludeRoleInfo(t *testing.T) {
	var (
		roleName1 = "foo1"
		username1 = "fo1"
		username2 = "fo2"
		roleName2 = "foo2"
		result    []*milvuspb.UserResult
		err       error
	)

	userDbMock.On("ListUser", tenantID).Return([]*dbmodel.User{{ID: 1, Username: username1}, {ID: 10, Username: username2}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(1), int64(0)).Return([]*dbmodel.UserRole{{Role: dbmodel.Role{Name: roleName1}}, {Role: dbmodel.Role{Name: roleName2}}}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(10), int64(0)).Return([]*dbmodel.UserRole{}, nil).Once()

	result, err = mockCatalog.ListUser(ctx, tenantID, nil, true)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.Equal(t, username1, result[0].User.Name)
	require.Equal(t, 2, len(result[0].Roles))
	require.Equal(t, roleName1, result[0].Roles[0].Name)
	require.Equal(t, roleName2, result[0].Roles[1].Name)
	require.Equal(t, username2, result[1].User.Name)
}

func TestTableCatalog_ListUser_AllUser_Empty(t *testing.T) {
	var (
		result []*milvuspb.UserResult
		err    error
	)

	userDbMock.On("ListUser", tenantID).Return([]*dbmodel.User{}, nil).Once()

	result, err = mockCatalog.ListUser(ctx, tenantID, nil, false)
	require.NoError(t, err)
	require.Equal(t, 0, len(result))
}

func TestTableCatalog_ListUser_OneUser(t *testing.T) {
	var (
		username1 = "foo1"
		result    []*milvuspb.UserResult
		err       error
	)

	userDbMock.On("GetByUsername", tenantID, username1).Return(&dbmodel.User{Username: username1}, nil).Once()

	result, err = mockCatalog.ListUser(ctx, tenantID, &milvuspb.UserEntity{Name: username1}, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, username1, result[0].User.Name)
}

func TestTableCatalog_ListUser_OneUser_IncludeRoleInfo(t *testing.T) {
	var (
		roleName1 = "foo1"
		roleName2 = "foo1"
		username1 = "fo1"
		result    []*milvuspb.UserResult
		err       error
	)

	userDbMock.On("GetByUsername", tenantID, username1).Return(&dbmodel.User{ID: 1, Username: username1}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(1), int64(0)).
		Return([]*dbmodel.UserRole{{Role: dbmodel.Role{Name: roleName1}}, {Role: dbmodel.Role{Name: roleName2}}}, nil).Once()

	result, err = mockCatalog.ListUser(ctx, tenantID, &milvuspb.UserEntity{Name: username1}, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, username1, result[0].User.Name)
	require.Equal(t, 2, len(result[0].Roles))
	require.Equal(t, roleName1, result[0].Roles[0].Name)
	require.Equal(t, roleName2, result[0].Roles[1].Name)
}

func TestTableCatalog_ListUser_ListUserError(t *testing.T) {
	userDbMock.On("ListUser", tenantID).Return(nil, errors.New("test error")).Once()

	_, err := mockCatalog.ListUser(ctx, tenantID, nil, false)
	require.Error(t, err)
}

func TestTableCatalog_ListUser_GetByUsernameError(t *testing.T) {
	var (
		username1 = "foo"
		err       error
	)

	userDbMock.On("GetByUsername", tenantID, username1).Return(nil, errors.New("test error")).Once()

	_, err = mockCatalog.ListUser(ctx, tenantID, &milvuspb.UserEntity{Name: username1}, false)
	require.Error(t, err)
}

func TestTableCatalog_ListUser_GetUserRolesError(t *testing.T) {
	var (
		username1 = "foo"
		err       error
	)

	userDbMock.On("GetByUsername", tenantID, username1).Return(&dbmodel.User{ID: 1, Username: username1}, nil).Once()
	userRoleDbMock.On("GetUserRoles", tenantID, int64(1), int64(0)).Return(nil, errors.New("test error")).Once()

	_, err = mockCatalog.ListUser(ctx, tenantID, &milvuspb.UserEntity{Name: username1}, true)
	require.Error(t, err)
}

func TestTableCatalog_AlterGrant_Revoke(t *testing.T) {
	var (
		roleName         = "foo"
		roleID     int64 = 1
		object           = "Collection"
		objectName       = "col1"
		grantID    int64 = 10
		username         = "fo"
		privilege        = "PrivilegeLoad"
		grant      *milvuspb.GrantEntity
		err        error
	)
	grant = &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: roleName},
		Object:     &milvuspb.ObjectEntity{Name: object},
		ObjectName: objectName,
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: username},
			Privilege: &milvuspb.PrivilegeEntity{Name: privilege},
		},
	}

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return([]*dbmodel.GrantID{{}}, nil).Once()
	grantIDDbMock.On("Delete", tenantID, grantID, privilege).Return(nil).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Revoke)
	require.NoError(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Revoke)
	require.Error(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return(nil, nil).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Revoke)
	require.Error(t, err)
	require.True(t, common.IsIgnorableError(err))

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Revoke)
	require.Error(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return(nil, nil).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Revoke)
	require.Error(t, err)
	require.True(t, common.IsIgnorableError(err))

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return([]*dbmodel.GrantID{{}}, nil).Once()
	grantIDDbMock.On("Delete", tenantID, grantID, privilege).Return(errors.New("test error")).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Revoke)
	require.Error(t, err)
}

func TestTableCatalog_AlterGrant_Grant(t *testing.T) {
	var (
		roleName         = "foo"
		roleID     int64 = 1
		object           = "Collection"
		objectName       = "col1"
		grantID    int64 = 10
		username         = "fo"
		userID     int64 = 100
		privilege        = "PrivilegeLoad"
		grant      *milvuspb.GrantEntity
		err        error
	)
	grant = &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: roleName},
		Object:     &milvuspb.ObjectEntity{Name: object},
		ObjectName: objectName,
		Grantor: &milvuspb.GrantorEntity{
			User:      &milvuspb.UserEntity{Name: username},
			Privilege: &milvuspb.PrivilegeEntity{Name: privilege},
		},
	}
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return([]*dbmodel.GrantID{}, nil).Once()
	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: userID}, nil).Once()
	grantIDDbMock.On("Insert", mock.Anything).Return(nil).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Grant)
	require.NoError(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return(nil, nil).Once()
	grantDbMock.On("Insert", mock.Anything).Return(nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, mock.Anything, privilege, mock.Anything, mock.Anything).Return([]*dbmodel.GrantID{}, nil).Once()
	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: userID}, nil).Once()
	grantIDDbMock.On("Insert", mock.Anything).Return(nil).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Grant)
	require.NoError(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Grant)
	require.Error(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return([]*dbmodel.GrantID{{}}, nil).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Grant)
	require.Error(t, err)
	require.True(t, common.IsIgnorableError(err))

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return([]*dbmodel.GrantID{}, nil).Once()
	userDbMock.On("GetByUsername", tenantID, username).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Grant)
	require.Error(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, object, objectName).Return([]*dbmodel.Grant{{Base: dbmodel.Base{ID: grantID}}}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, privilege, mock.Anything, mock.Anything).Return([]*dbmodel.GrantID{}, nil).Once()
	userDbMock.On("GetByUsername", tenantID, username).Return(&dbmodel.User{ID: userID}, nil).Once()
	grantIDDbMock.On("Insert", mock.Anything).Return(errors.New("test error")).Once()
	err = mockCatalog.AlterGrant(ctx, tenantID, grant, milvuspb.OperatePrivilegeType_Grant)
	require.Error(t, err)
}

func TestTableCatalog_AlterGrant_InvalidType(t *testing.T) {
	var (
		roleName = "foo"
		err      error
	)

	err = mockCatalog.AlterGrant(ctx, tenantID, &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: roleName}}, 100)
	require.Error(t, err)
}

func TestTableCatalog_ListGrant(t *testing.T) {
	var (
		roleID  int64 = 1
		grantID int64 = 10
		grant   *milvuspb.GrantEntity
		grants  []*dbmodel.Grant
		entites []*milvuspb.GrantEntity
		err     error
	)

	grant = &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "foo"},
		Object:     &milvuspb.ObjectEntity{Name: "Collection"},
		ObjectName: "col1",
	}
	grants = []*dbmodel.Grant{
		{
			Base:       dbmodel.Base{ID: grantID},
			Role:       dbmodel.Role{Name: "foo"},
			Object:     "Collection",
			ObjectName: "col1",
		},
	}
	roleDbMock.On("GetRoles", tenantID, grant.Role.Name).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, grant.Object.Name, grant.ObjectName).Return(grants, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, "", false, true).Return([]*dbmodel.GrantID{
		{
			Privilege: "PrivilegeLoad",
			Grantor:   dbmodel.User{Username: "root"},
		},
		{
			Privilege: "*",
			Grantor:   dbmodel.User{Username: "root"},
		},
	}, nil).Once()

	entites, err = mockCatalog.ListGrant(ctx, tenantID, grant)
	require.NoError(t, err)
	require.Equal(t, 2, len(entites))
	require.Equal(t, "foo", entites[0].Role.Name)
	require.Equal(t, "Collection", entites[1].Object.Name)
	require.Equal(t, "col1", entites[1].ObjectName)
	require.Equal(t, "root", entites[1].Grantor.User.Name)
	require.Equal(t, "*", entites[1].Grantor.Privilege.Name)
}

func TestTableCatalog_ListGrant_GetRolesError(t *testing.T) {
	var (
		grant *milvuspb.GrantEntity
		err   error
	)
	grant = &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "foo"},
		Object:     &milvuspb.ObjectEntity{Name: "Collection"},
		ObjectName: "col1",
	}
	roleDbMock.On("GetRoles", tenantID, grant.Role.Name).Return(nil, errors.New("test error")).Once()
	_, err = mockCatalog.ListGrant(ctx, tenantID, grant)
	require.Error(t, err)
}

func TestTableCatalog_ListGrant_GetGrantError(t *testing.T) {
	var (
		grant *milvuspb.GrantEntity
		err   error
	)
	grant = &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "foo"},
		Object:     &milvuspb.ObjectEntity{Name: "Collection"},
		ObjectName: "col1",
	}
	roleDbMock.On("GetRoles", tenantID, grant.Role.Name).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: 1}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, int64(1), grant.Object.Name, grant.ObjectName).Return(nil, errors.New("test error")).Once()
	_, err = mockCatalog.ListGrant(ctx, tenantID, grant)
	require.Error(t, err)
}

func TestTableCatalog_ListGrant_GetGrantIDError(t *testing.T) {
	var (
		roleID  int64 = 1
		grantID int64 = 10
		grant   *milvuspb.GrantEntity
		grants  []*dbmodel.Grant
		err     error
	)

	grant = &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "foo"},
		Object:     &milvuspb.ObjectEntity{Name: "Collection"},
		ObjectName: "col1",
	}
	grants = []*dbmodel.Grant{
		{
			Base:       dbmodel.Base{ID: grantID},
			Role:       dbmodel.Role{Name: "foo"},
			Object:     "Collection",
			ObjectName: "col1",
		},
	}
	roleDbMock.On("GetRoles", tenantID, grant.Role.Name).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, roleID, grant.Object.Name, grant.ObjectName).Return(grants, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID, "", false, true).Return(nil, errors.New("test error")).Once()

	_, err = mockCatalog.ListGrant(ctx, tenantID, grant)
	require.Error(t, err)
}

func TestTableCatalog_ListGrant_NotExistError(t *testing.T) {
	var (
		grant *milvuspb.GrantEntity
		err   error
	)
	grant = &milvuspb.GrantEntity{
		Role:       &milvuspb.RoleEntity{Name: "foo"},
		Object:     &milvuspb.ObjectEntity{Name: "Collection"},
		ObjectName: "col1",
	}
	roleDbMock.On("GetRoles", tenantID, grant.Role.Name).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: 1}}}, nil).Once()
	grantDbMock.On("GetGrants", tenantID, int64(1), grant.Object.Name, grant.ObjectName).Return(nil, nil).Once()

	_, err = mockCatalog.ListGrant(ctx, tenantID, grant)
	require.Error(t, err)
	require.True(t, common.IsKeyNotExistError(err))
}

func TestTableCatalog_DropGrant(t *testing.T) {
	var (
		roleName       = "foo"
		roleID   int64 = 10
		err      error
	)
	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("Delete", tenantID, roleID, "", "").Return(nil).Once()
	err = mockCatalog.DeleteGrant(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName})
	require.NoError(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return(nil, errors.New("test error")).Once()
	err = mockCatalog.DeleteGrant(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName})
	require.Error(t, err)

	roleDbMock.On("GetRoles", tenantID, roleName).Return([]*dbmodel.Role{{Base: dbmodel.Base{ID: roleID}}}, nil).Once()
	grantDbMock.On("Delete", tenantID, roleID, "", "").Return(errors.New("test error")).Once()
	err = mockCatalog.DeleteGrant(ctx, tenantID, &milvuspb.RoleEntity{Name: roleName})
	require.Error(t, err)
}

func TestTableCatalog_ListPolicy(t *testing.T) {
	var (
		roleName1         = "foo1"
		roleName2         = "foo1"
		grantID1    int64 = 10
		grantID2    int64 = 100
		object1           = "obj1"
		object2           = "obj2"
		objectName1       = "col1"
		objectName2       = "col2"
		privilege1        = "PrivilegeInsert"
		privilege2        = "PrivilegeQuery"
		grants      []*dbmodel.Grant
		policies    []string
		err         error
	)

	grants = []*dbmodel.Grant{
		{
			Base:       dbmodel.Base{ID: grantID1},
			Role:       dbmodel.Role{Name: roleName1},
			Object:     object1,
			ObjectName: objectName1,
		},
		{
			Base:       dbmodel.Base{ID: grantID2},
			Role:       dbmodel.Role{Name: roleName2},
			Object:     object2,
			ObjectName: objectName2,
		},
	}
	grantDbMock.On("GetGrants", tenantID, int64(0), "", "").Return(grants, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID1, "", false, false).Return([]*dbmodel.GrantID{
		{Privilege: privilege1},
		{Privilege: privilege2},
	}, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID2, "", false, false).Return([]*dbmodel.GrantID{
		{Privilege: privilege1},
	}, nil).Once()

	policies, err = mockCatalog.ListPolicy(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, 3, len(policies))
	require.Equal(t, funcutil.PolicyForPrivilege(roleName1, object1, objectName1, privilege1, util.DefaultDBName), policies[0])

	grantDbMock.On("GetGrants", tenantID, int64(0), "", "").Return(nil, errors.New("test error")).Once()
	_, err = mockCatalog.ListPolicy(ctx, tenantID)
	require.Error(t, err)

	grantDbMock.On("GetGrants", tenantID, int64(0), "", "").Return(grants, nil).Once()
	grantIDDbMock.On("GetGrantIDs", tenantID, grantID1, "", false, false).Return(nil, errors.New("test error")).Once()
	_, err = mockCatalog.ListPolicy(ctx, tenantID)
	require.Error(t, err)
}

func TestTableCatalog_ListUserRole(t *testing.T) {
	var (
		username1 = "foo1"
		username2 = "foo2"
		roleName1 = "fo1"
		roleName2 = "fo2"
		userRoles []string
		err       error
	)

	userRoleDbMock.On("GetUserRoles", tenantID, int64(0), int64(0)).Return([]*dbmodel.UserRole{
		{User: dbmodel.User{Username: username1}, Role: dbmodel.Role{Name: roleName1}},
		{User: dbmodel.User{Username: username2}, Role: dbmodel.Role{Name: roleName2}},
	}, nil).Once()

	userRoles, err = mockCatalog.ListUserRole(ctx, tenantID)
	require.NoError(t, err)
	require.Equal(t, 2, len(userRoles))
	require.Equal(t, funcutil.EncodeUserRoleCache(username1, roleName1), userRoles[0])
}

func TestTableCatalog_ListUserRole_Error(t *testing.T) {
	userRoleDbMock.On("GetUserRoles", tenantID, int64(0), int64(0)).Return(nil, errors.New("test error")).Once()
	_, err := mockCatalog.ListUserRole(ctx, tenantID)
	require.Error(t, err)
}
