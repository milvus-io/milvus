package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestNewInterceptor(t *testing.T) {
	mixc := &mocks.MockMixCoordClient{}
	mixc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: false}, nil)
	node := &Proxy{
		mixCoord: mixc,
		session:  &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
	}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.On("DescribeCollection", mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
	var err error
	globalMetaCache, err = NewMetaCache(mixCoord)
	assert.NoError(t, err)
	interceptor, err := NewInterceptor[*milvuspb.DescribeCollectionRequest, *milvuspb.DescribeCollectionResponse](node, "DescribeCollection")
	assert.NoError(t, err)
	resp, err := interceptor.Call(context.Background(), &milvuspb.DescribeCollectionRequest{
		DbName:         "test",
		CollectionName: "test",
	})
	assert.NoError(t, err)
	assert.Equal(t, "can't find collection[database=test][collection=test]", resp.Status.Reason)
}

func TestCachedProxyServiceProvider_DescribeCollection_IgnoresLegacyDoPhysicalBackfill(t *testing.T) {
	ctx := context.Background()

	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	dbName := "test_db"
	collectionName := "test_collection"
	collectionID := int64(1000)
	schema := &schemapb.CollectionSchema{
		Name:               collectionName,
		DoPhysicalBackfill: true,
		Fields: []*schemapb.FieldSchema{{
			FieldID:      common.StartOfUserFieldID,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}},
	}

	mockCache := &MockCache{}
	mockCache.EXPECT().GetCollectionID(mock.Anything, dbName, collectionName).Return(collectionID, nil)
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, dbName, collectionName, collectionID).Return(&collectionInfo{
		collID:    collectionID,
		schema:    newSchemaInfo(schema),
		shardsNum: common.DefaultShardsNum,
	}, nil)
	globalMetaCache = mockCache

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	resp, err := provider.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.False(t, resp.GetSchema().GetDoPhysicalBackfill())
}

func TestCachedProxyServiceProvider_DescribeCollection_ByIDFillsNameAndUsesRequestID(t *testing.T) {
	ctx := context.Background()

	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	dbName := "db1"
	dbID := int64(3)
	collectionName := "coll1"
	collectionID := int64(449574)
	schema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{{
			FieldID:      common.StartOfUserFieldID,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}},
	}

	// Query by collection id only: no collection name, no db name (e.g. the HTTP
	// management API). The provider must fill the top-level collection_name from
	// the cache instead of echoing the empty request.CollectionName, and must
	// keep the caller-provided id: GetCollectionID is intentionally not mocked,
	// so resolving the id back from the derived name would panic the test.
	mockCache := &MockCache{}
	mockCache.EXPECT().GetCollectionName(mock.Anything, "", collectionID).Return(collectionName, nil)
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, "", collectionName, collectionID).Return(&collectionInfo{
		collID:    collectionID,
		dbName:    dbName,
		dbID:      dbID,
		schema:    newSchemaInfo(schema),
		shardsNum: common.DefaultShardsNum,
	}, nil)
	globalMetaCache = mockCache

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	request := &milvuspb.DescribeCollectionRequest{
		CollectionID: collectionID,
	}
	resp, err := provider.DescribeCollection(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Equal(t, collectionName, resp.GetCollectionName())
	assert.Equal(t, collectionID, resp.GetCollectionID())
	assert.Equal(t, dbName, resp.GetDbName())
	assert.Equal(t, dbID, resp.GetDbId())
	// the handler must not rewrite the request in place: access logs and metric
	// labels serialize it after the call and must see what the client sent
	assert.Empty(t, request.GetCollectionName())
	assert.Equal(t, collectionID, request.GetCollectionID())
}

func TestCachedProxyServiceProvider_DescribeCollection_FilterNamespaceField(t *testing.T) {
	ctx := context.Background()

	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	dbName := "test_db"
	collectionName := "test_collection"
	collectionID := int64(1000)

	schema := &schemapb.CollectionSchema{
		Name:               collectionName,
		EnableDynamicField: true,
		EnableNamespace:    true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  common.StartOfUserFieldID + 1,
				Name:     common.NamespaceFieldName,
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:   common.StartOfUserFieldID + 2,
				Name:      common.MetaFieldName,
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
		},
	}

	mockCache := &MockCache{}
	mockCache.EXPECT().GetCollectionID(mock.Anything, dbName, collectionName).Return(collectionID, nil)
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, dbName, collectionName, collectionID).Return(&collectionInfo{
		collID:    collectionID,
		schema:    newSchemaInfo(schema),
		shardsNum: common.DefaultShardsNum,
	}, nil)
	globalMetaCache = mockCache

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	resp, err := provider.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.True(t, resp.GetSchema().GetEnableNamespace())

	fieldNames := make(map[string]struct{})
	for _, f := range resp.GetSchema().GetFields() {
		fieldNames[f.GetName()] = struct{}{}
	}
	_, hasNamespace := fieldNames[common.NamespaceFieldName]
	assert.False(t, hasNamespace)
	_, hasMeta := fieldNames[common.MetaFieldName]
	assert.False(t, hasMeta)
	_, hasID := fieldNames["id"]
	assert.True(t, hasID)
}

func TestCachedProxyServiceProvider_DescribeCollection_ByIDReturnsActualDbName(t *testing.T) {
	ctx := context.Background()

	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	collectionID := int64(2000)
	schema := &schemapb.CollectionSchema{
		Name: "coll1",
		Fields: []*schemapb.FieldSchema{{
			FieldID:      common.StartOfUserFieldID,
			Name:         "id",
			IsPrimaryKey: true,
			DataType:     schemapb.DataType_Int64,
		}},
	}

	mockCache := &MockCache{}
	mockCache.EXPECT().GetCollectionName(mock.Anything, "", collectionID).Return("coll1", nil)
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, "", "coll1", collectionID).Return(&collectionInfo{
		collID: collectionID,
		// resolved by the coordinator and carried in the cache entry
		dbID:      7,
		dbName:    "db1",
		schema:    newSchemaInfo(schema),
		shardsNum: common.DefaultShardsNum,
	}, nil)
	globalMetaCache = mockCache

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	// the monitoring http path passes only the collection id, both db name and
	// collection name are empty
	resp, err := provider.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionID: collectionID})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Equal(t, "db1", resp.GetDbName())
	assert.Equal(t, int64(7), resp.GetDbId())
}
