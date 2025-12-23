package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
