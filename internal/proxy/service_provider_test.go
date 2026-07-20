package proxy

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func proxyHistogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric := &dto.Metric{}
	assert.NoError(t, observer.(prometheus.Metric).Write(metric))
	return metric.GetHistogram().GetSampleCount()
}

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
		schema:    mustNewSchemaInfo(schema),
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
	// keep the caller-provided id. Neither GetCollectionName nor GetCollectionID
	// is mocked, so any redundant identifier round-trip would fail the test.
	mockCache := &MockCache{}
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, "", "", collectionID).Return(&collectionInfo{
		collID:    collectionID,
		dbName:    dbName,
		dbID:      dbID,
		schema:    mustNewSchemaInfo(schema),
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
		schema:    mustNewSchemaInfo(schema),
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
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, "", "", collectionID).Return(&collectionInfo{
		collID: collectionID,
		// resolved by the coordinator and carried in the cache entry
		dbID:      7,
		dbName:    "db1",
		schema:    mustNewSchemaInfo(schema),
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

// TestCachedProxyServiceProvider_DescribeCollection_ByIDNotFoundUsesCollectionNotExistsCode
// covers the direct id-only lookup failing with a not-found. Both the typed code
// and deprecated ErrorCode must carry the collection-not-found meaning.
func TestCachedProxyServiceProvider_DescribeCollection_ByIDNotFoundUsesCollectionNotExistsCode(t *testing.T) {
	ctx := context.Background()

	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	collectionID := int64(4242)
	mockCache := &MockCache{}
	mockCache.EXPECT().GetCollectionInfo(mock.Anything, "", "", collectionID).
		Return(nil, merr.WrapErrCollectionNotFound(collectionID))
	globalMetaCache = mockCache

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	resp, err := provider.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionID: collectionID})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_CollectionNotExists, resp.GetStatus().GetErrorCode())
	assert.Equal(t, merr.Code(merr.ErrCollectionNotFound), resp.GetStatus().GetCode())
	assert.Equal(t, "true", resp.GetStatus().GetExtraInfo()[merr.InputErrorFlagKey],
		"collection not found must be flagged as an input error")
	assert.False(t, resp.GetStatus().GetRetriable())
}

// TestCachedProxyServiceProvider_DescribeCollection_EmptyRequestFailsValidation:
// name=="" and id==0 does NOT enter the id-only branch (id>0 is false), so the
// empty name reaches validateCollectionName and must fail without touching the
// cache.
func TestCachedProxyServiceProvider_DescribeCollection_EmptyRequestFailsValidation(t *testing.T) {
	ctx := context.Background()

	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	// no expectations: any cache call is an unexpected-call failure
	globalMetaCache = &MockCache{}

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	resp, err := provider.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode(),
		"an empty collection name must fail validation")
}

// TestCachedProxyServiceProvider_DescribeCollection_NameOnlyDoesNotMutateRequest
// mirrors the id-only no-mutation guarantee for the name-only path: the handler
// resolves the id on a local copy and must not write it back into the request,
// which the access log and metric labels read after the call.
func TestCachedProxyServiceProvider_DescribeCollection_NameOnlyDoesNotMutateRequest(t *testing.T) {
	ctx := context.Background()

	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	dbName := "db"
	collectionName := "coll1"
	collectionID := int64(918)
	schema := &schemapb.CollectionSchema{
		Name: collectionName,
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
		schema:    mustNewSchemaInfo(schema),
		shardsNum: common.DefaultShardsNum,
	}, nil)
	globalMetaCache = mockCache

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	request := &milvuspb.DescribeCollectionRequest{DbName: dbName, CollectionName: collectionName}
	resp, err := provider.DescribeCollection(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	// the resolved id must not leak back into the request
	assert.Equal(t, int64(0), request.GetCollectionID())
	assert.Equal(t, collectionName, request.GetCollectionName())
}

func TestCachedProxyServiceProvider_DescribeCollection_IDOnlyOldRootCoordUsesOneDescribeAndKeepsUnknownDB(t *testing.T) {
	ctx := context.Background()
	origCache := globalMetaCache
	defer func() { globalMetaCache = origCache }()

	const collectionID = int64(101)
	var describeCount atomic.Int32
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			describeCount.Add(1)
			assert.Equal(t, collectionID, req.GetCollectionID())
			assert.Empty(t, req.GetDbName())
			assert.Empty(t, req.GetCollectionName())
			return &milvuspb.DescribeCollectionResponse{
				Status:       merr.Success(),
				CollectionID: collectionID,
				// Rolling-upgrade old RootCoord resolves the id but cannot report
				// the collection's real database.
				DbName: "",
				Schema: &schemapb.CollectionSchema{
					Name: "foo",
				},
			}, nil
		}).Once()
	cache, err := NewMetaCache(mixCoord)
	assert.NoError(t, err)
	defer cache.Close()
	globalMetaCache = cache

	provider := &CachedProxyServiceProvider{Proxy: &Proxy{}}
	request := &milvuspb.DescribeCollectionRequest{CollectionID: collectionID}
	interceptor := DatabaseInterceptor()
	result, err := interceptor(ctx, request, &grpc.UnaryServerInfo{}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return provider.DescribeCollection(ctx, req.(*milvuspb.DescribeCollectionRequest))
	})
	assert.NoError(t, err)
	resp := result.(*milvuspb.DescribeCollectionResponse)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	assert.Equal(t, collectionID, resp.GetCollectionID())
	assert.Equal(t, "foo", resp.GetCollectionName())
	assert.Empty(t, request.GetDbName(), "the interceptor must preserve an omitted db for id-only requests")
	assert.Empty(t, resp.GetDbName(), "an old RootCoord's unknown db must not be reported as default")
	assert.Equal(t, int32(1), describeCount.Load())
}

func TestProjectDescribeCollectionSchema_CopyOnWriteAndFinalization(t *testing.T) {
	timestamptzField := &schemapb.FieldSchema{
		FieldID:  common.StartOfUserFieldID + 1,
		Name:     "created_at",
		DataType: schemapb.DataType_Timestamptz,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_TimestamptzData{TimestamptzData: 1_700_000_000_000_000},
		},
	}
	ordinaryField := &schemapb.FieldSchema{
		FieldID:       common.StartOfUserFieldID,
		Name:          "id",
		DataType:      schemapb.DataType_Int64,
		IsPrimaryKey:  true,
		State:         schemapb.FieldState_FieldDropping,
		ExternalField: "external_id",
	}
	source := &schemapb.CollectionSchema{
		Name:         "collection",
		DbName:       "db",
		Version:      7,
		Properties:   []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "UTC"}},
		ExternalSpec: `{"format":"parquet","extfs":{"access_key_id":"AKIA","access_key_value":"SECRET"}}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			ordinaryField,
			timestamptzField,
			{FieldID: common.StartOfUserFieldID + 2, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
			{FieldID: common.StartOfUserFieldID + 3, Name: common.NamespaceFieldName, DataType: schemapb.DataType_VarChar},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: common.StartOfUserFieldID + 4,
			Name:    "profile",
			Fields: []*schemapb.FieldSchema{{
				FieldID:       common.StartOfUserFieldID + 5,
				Name:          "profile[age]",
				DataType:      schemapb.DataType_Int64,
				State:         schemapb.FieldState_FieldDropping,
				ExternalField: "external_age",
			}},
		}},
	}

	projected, err := projectDescribeCollectionSchema(source, true)
	assert.NoError(t, err)
	assert.Len(t, projected.GetFields(), 2)
	assert.Same(t, ordinaryField, projected.GetFields()[0], "ordinary immutable fields should not be cloned")
	assert.NotSame(t, timestamptzField, projected.GetFields()[1], "the field rewritten for the API must be cloned")
	assert.Equal(t, schemapb.FieldState_FieldDropping, projected.GetFields()[0].GetState())
	assert.Equal(t, "external_id", projected.GetFields()[0].GetExternalField())
	assert.Equal(t, "age", projected.GetStructArrayFields()[0].GetFields()[0].GetName())
	assert.Equal(t, schemapb.FieldState_FieldDropping, projected.GetStructArrayFields()[0].GetFields()[0].GetState())
	assert.Equal(t, "external_age", projected.GetStructArrayFields()[0].GetFields()[0].GetExternalField())

	resp := &milvuspb.DescribeCollectionResponse{Status: merr.Success(), Schema: projected}
	assert.NoError(t, finalizeDescribeCollectionResponse(resp))
	assert.IsType(t, &schemapb.ValueField_StringData{}, projected.GetFields()[1].GetDefaultValue().GetData())
	assert.IsType(t, &schemapb.ValueField_TimestamptzData{}, timestamptzField.GetDefaultValue().GetData(),
		"public response conversion must not mutate the cached canonical default")
	assert.NotContains(t, projected.GetExternalSpec(), "AKIA")
	assert.NotContains(t, projected.GetExternalSpec(), "SECRET")
	assert.Equal(t, int32(7), projected.GetVersion())
	assert.Equal(t, "db", projected.GetDbName())
}

func TestDescribeCollectionCachedAndRemoteProjectionEquivalent(t *testing.T) {
	ctx := context.Background()
	const (
		database       = "db"
		collectionName = "collection"
		collectionID   = int64(100)
	)
	raw := &milvuspb.DescribeCollectionResponse{
		Status:               merr.Success(),
		CollectionID:         collectionID,
		CollectionName:       collectionName,
		DbName:               database,
		DbId:                 10,
		CreatedTimestamp:     11,
		CreatedUtcTimestamp:  12,
		UpdateTimestamp:      13,
		ShardsNum:            2,
		NumPartitions:        3,
		ConsistencyLevel:     commonpb.ConsistencyLevel_Bounded,
		VirtualChannelNames:  []string{"v1"},
		PhysicalChannelNames: []string{"p1"},
		Aliases:              []string{"alias"},
		Properties:           []*commonpb.KeyValuePair{{Key: "collection_property", Value: "value"}},
		Schema: &schemapb.CollectionSchema{
			Name:               collectionName,
			Description:        "description",
			DbName:             database,
			Version:            9,
			EnableDynamicField: true,
			EnableNamespace:    true,
			Properties:         []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "UTC"}},
			ExternalSource:     "s3://bucket/path",
			ExternalSpec:       `{"format":"parquet","extfs":{"access_key_id":"AKIA","access_key_value":"SECRET"}}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
				{FieldID: common.StartOfUserFieldID, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, State: schemapb.FieldState_FieldDropping, ExternalField: "external_id"},
				{FieldID: common.StartOfUserFieldID + 1, Name: "created_at", DataType: schemapb.DataType_Timestamptz, DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_TimestamptzData{TimestamptzData: 1_700_000_000_000_000}}},
				{FieldID: common.StartOfUserFieldID + 2, Name: common.MetaFieldName, DataType: schemapb.DataType_JSON, IsDynamic: true},
				{FieldID: common.StartOfUserFieldID + 3, Name: common.NamespaceFieldName, DataType: schemapb.DataType_VarChar},
			},
			StructArrayFields: []*schemapb.StructArrayFieldSchema{{
				FieldID: common.StartOfUserFieldID + 4,
				Name:    "profile",
				Fields: []*schemapb.FieldSchema{{
					FieldID: common.StartOfUserFieldID + 5, Name: "profile[age]", DataType: schemapb.DataType_Int64,
					State: schemapb.FieldState_FieldDropping, ExternalField: "external_age",
				}},
			}},
		},
	}

	mix := NewMixCoordMock()
	mix.SetDescribeCollectionFunc(func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return proto.Clone(raw).(*milvuspb.DescribeCollectionResponse), nil
	})
	remoteTask := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			DbName: database, CollectionName: collectionName,
		},
		ctx: ctx, mixCoord: mix,
	}
	assert.NoError(t, remoteTask.PreExecute(ctx))
	assert.NoError(t, remoteTask.Execute(ctx))
	remoteResp := remoteTask.result
	assert.NoError(t, finalizeDescribeCollectionResponse(remoteResp))

	cacheSchema := proto.Clone(raw.GetSchema()).(*schemapb.CollectionSchema)
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, database, collectionName).Return(collectionID, nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, database, collectionName, collectionID).Return(&collectionInfo{
		collID:              collectionID,
		dbID:                raw.GetDbId(),
		dbName:              database,
		schema:              mustNewSchemaInfo(cacheSchema),
		createdTimestamp:    raw.GetCreatedTimestamp(),
		createdUtcTimestamp: raw.GetCreatedUtcTimestamp(),
		consistencyLevel:    raw.GetConsistencyLevel(),
		updateTimestamp:     raw.GetUpdateTimestamp(),
		vChannels:           raw.GetVirtualChannelNames(),
		pChannels:           raw.GetPhysicalChannelNames(),
		numPartitions:       raw.GetNumPartitions(),
		shardsNum:           raw.GetShardsNum(),
		aliases:             raw.GetAliases(),
		properties:          raw.GetProperties(),
	}, nil)
	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()

	cachedResp, err := (&CachedProxyServiceProvider{Proxy: &Proxy{}}).DescribeCollection(ctx,
		&milvuspb.DescribeCollectionRequest{DbName: database, CollectionName: collectionName})
	assert.NoError(t, err)
	assert.NoError(t, finalizeDescribeCollectionResponse(cachedResp))
	assert.True(t, proto.Equal(remoteResp, cachedResp), "cached and remote responses differ:\nremote=%s\ncached=%s", remoteResp, cachedResp)
	assert.IsType(t, &schemapb.ValueField_TimestamptzData{}, cacheSchema.GetFields()[2].GetDefaultValue().GetData(),
		"cached provider must preserve the canonical TIMESTAMPTZ default")
}

func TestDescribeCollectionCachedAndRemoteNotFoundStatusEquivalent(t *testing.T) {
	ctx := context.Background()
	const (
		database       = "db"
		collectionName = "missing"
	)
	notFound := merr.WrapErrCollectionNotFound(collectionName)
	mix := NewMixCoordMock()
	mix.SetDescribeCollectionFunc(func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{Status: merr.Status(notFound)}, nil
	})
	remoteTask := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			DbName: database, CollectionName: collectionName,
		},
		ctx: ctx, mixCoord: mix,
	}
	assert.NoError(t, remoteTask.PreExecute(ctx))
	assert.NoError(t, remoteTask.Execute(ctx))

	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, database, collectionName).Return(int64(0), notFound)
	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()
	cachedResp, err := (&CachedProxyServiceProvider{Proxy: &Proxy{}}).DescribeCollection(ctx,
		&milvuspb.DescribeCollectionRequest{DbName: database, CollectionName: collectionName})
	assert.NoError(t, err)

	assert.True(t, proto.Equal(remoteTask.result.GetStatus(), cachedResp.GetStatus()))
	assert.Equal(t, commonpb.ErrorCode_CollectionNotExists, cachedResp.GetStatus().GetErrorCode())
	assert.Equal(t, merr.Code(merr.ErrCollectionNotFound), cachedResp.GetStatus().GetCode())
	assert.Equal(t, "true", cachedResp.GetStatus().GetExtraInfo()[merr.InputErrorFlagKey])
	assert.False(t, cachedResp.GetStatus().GetRetriable())
}

func TestDescribeCollectionCachedAndRemoteDatabaseNotFoundStatusEquivalent(t *testing.T) {
	ctx := context.Background()
	const (
		database       = "missing_db"
		collectionName = "collection"
	)
	notFound := merr.WrapErrDatabaseNotFound(database)
	mix := NewMixCoordMock()
	mix.SetDescribeCollectionFunc(func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{Status: merr.Status(notFound)}, nil
	})
	remoteTask := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base:   &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			DbName: database, CollectionName: collectionName,
		},
		ctx: ctx, mixCoord: mix,
	}
	assert.NoError(t, remoteTask.PreExecute(ctx))
	assert.NoError(t, remoteTask.Execute(ctx))

	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, database, collectionName).Return(int64(0), notFound)
	oldCache := globalMetaCache
	globalMetaCache = cache
	defer func() { globalMetaCache = oldCache }()
	cachedResp, err := (&CachedProxyServiceProvider{Proxy: &Proxy{}}).DescribeCollection(ctx,
		&milvuspb.DescribeCollectionRequest{DbName: database, CollectionName: collectionName})
	assert.NoError(t, err)

	assert.True(t, proto.Equal(remoteTask.result.GetStatus(), cachedResp.GetStatus()))
	assert.ErrorIs(t, merr.Error(cachedResp.GetStatus()), merr.ErrDatabaseNotFound)
	// The deprecated ErrorCode enum cannot represent database-not-found; the
	// typed Code below is the authoritative wire value.
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, cachedResp.GetStatus().GetErrorCode())
	assert.Equal(t, merr.Code(merr.ErrDatabaseNotFound), cachedResp.GetStatus().GetCode())
	assert.Contains(t, cachedResp.GetStatus().GetReason(), "database not found")
	assert.Equal(t, "true", cachedResp.GetStatus().GetExtraInfo()[merr.InputErrorFlagKey])
	assert.False(t, cachedResp.GetStatus().GetRetriable())
}

func TestInterceptorImpl_RecordsOutcomeFromErrorAndResponseStatus(t *testing.T) {
	paramtable.Init()
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	request := &milvuspb.DescribeCollectionRequest{DbName: "db", CollectionName: "collection"}
	nodeID := paramtable.GetStringNodeID()

	tests := []struct {
		name         string
		onCall       func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
		expectStatus string
	}{
		{
			name: "response input error",
			onCall: func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrParameterInvalidMsg("bad request"))}, nil
			},
			expectStatus: metrics.FailInputLabel,
		},
		{
			name: "go system error",
			onCall: func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
				return nil, merr.WrapErrServiceInternalMsg("internal failure")
			},
			expectStatus: metrics.FailSystemLabel,
		},
		{
			name: "provider abandon",
			onCall: func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
				return nil, withServiceProviderMetric(
					merr.WrapErrServiceInternalMsg("enqueue failed"), metrics.AbandonLabel)
			},
			expectStatus: metrics.AbandonLabel,
		},
		{
			name: "success",
			onCall: func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
				return &milvuspb.DescribeCollectionResponse{Status: merr.Success()}, nil
			},
			expectStatus: metrics.SuccessLabel,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			method := "DescribeCollectionMetric_" + tc.name
			outcomeCounter := metrics.ProxyFunctionCall.WithLabelValues(
				nodeID, method, tc.expectStatus, request.GetDbName(), request.GetCollectionName())
			totalCounter := metrics.ProxyFunctionCall.WithLabelValues(
				nodeID, method, metrics.TotalLabel, request.GetDbName(), request.GetCollectionName())
			latency := metrics.ProxyReqLatency.WithLabelValues(nodeID, method)
			beforeOutcome := testutil.ToFloat64(outcomeCounter)
			beforeTotal := testutil.ToFloat64(totalCounter)
			beforeLatency := proxyHistogramSampleCount(t, latency)

			interceptor := &InterceptorImpl[*milvuspb.DescribeCollectionRequest, *milvuspb.DescribeCollectionResponse]{
				proxy:  node,
				method: method,
				onCall: tc.onCall,
				onError: func(err error) (*milvuspb.DescribeCollectionResponse, error) {
					return &milvuspb.DescribeCollectionResponse{Status: merr.Status(err)}, nil
				},
			}
			resp, err := interceptor.Call(context.Background(), request)
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			assert.Equal(t, beforeOutcome+1, testutil.ToFloat64(outcomeCounter))
			assert.Equal(t, beforeTotal+1, testutil.ToFloat64(totalCounter))
			assert.Equal(t, beforeLatency+1, proxyHistogramSampleCount(t, latency))
		})
	}
}
