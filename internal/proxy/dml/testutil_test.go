package dml

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/util/uniquegenerator"
)

const testVecDim = 128

// enableMultipleVectorFields indicates whether to enable multiple vector fields.
const (
	enableMultipleVectorFields = true
	testMaxVarCharLength       = 512
)

const (
	testInt64Field     = "int64"
	testVarCharField   = "varChar"
	testFloatVecField  = "fvec"
	int64Field         = "int64"
	floatVecField      = "fVec"
	testBoolField      = "bool"
	testInt32Field     = "int32"
	testFloatField     = "float"
	testDoubleField    = "double"
	testBinaryVecField = "bvec"
)

// mustNewSchemaInfo builds a schemaInfo for tests.
func mustNewSchemaInfo(schema *schemapb.CollectionSchema) *schemaInfo {
	si, err := metacache.NewSchemaInfo(schema)
	if err != nil {
		panic(err)
	}
	return si
}

// newSchemaInfo builds a schemaInfo for tests.
func newSchemaInfo(schema *schemapb.CollectionSchema) (*schemaInfo, error) {
	return metacache.NewSchemaInfo(schema)
}

// newScalarFieldData builds scalar field data for tests.
func newScalarFieldData(fieldSchema *schemapb.FieldSchema, fieldName string, numRows int) *schemapb.FieldData {
	return testutils.GenerateScalarFieldData(fieldSchema.GetDataType(), fieldName, numRows)
}

// newFloatVectorFieldData builds float-vector field data for tests.
func newFloatVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloatVectorFieldData(fieldName, numRows, dim)
}

// generateFieldData builds field data for a data type.
func generateFieldData(dataType schemapb.DataType, fieldName string, numRows int) *schemapb.FieldData {
	if dataType < 100 {
		return testutils.GenerateScalarFieldData(dataType, fieldName, numRows)
	}
	return testutils.GenerateVectorFieldData(dataType, fieldName, numRows, testVecDim)
}

// newTestCache returns an empty metacache whose methods are expected to be
// patched by mockey in each test.
func newTestCache() *metacache.MetaCache {
	cache, err := metacache.NewMetaCache(nil)
	if err != nil {
		panic(err)
	}
	return cache
}

// mockTsoAllocator is a simple taskmodel.TsoAllocator for tests.
type mockTsoAllocator struct {
	mu        sync.Mutex
	logicPart uint32
}

func (tso *mockTsoAllocator) AllocOne(ctx context.Context) (Timestamp, error) {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	tso.logicPart++
	physical := uint64(time.Now().UnixMilli())
	return (physical << 18) + uint64(tso.logicPart), nil
}

// mockIDAllocatorInterface is a stub allocator for tests.
type mockIDAllocatorInterface struct{}

func (m *mockIDAllocatorInterface) AllocOne() (UniqueID, error) {
	return UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()), nil
}

func (m *mockIDAllocatorInterface) Alloc(count uint32) (UniqueID, UniqueID, error) {
	return UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt()), UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt() + int(count)), nil
}

// limiterMock is a minimal types.Limiter stub for tests.
type limiterMock struct {
	limit             bool
	rate              float64
	quotaStates       []milvuspb.QuotaState
	quotaStateReasons []commonpb.ErrorCode
}

func (l *limiterMock) Check(dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error {
	if l.rate == 0 {
		return merr.ErrServiceQuotaExceeded
	}
	if l.limit {
		return merr.ErrServiceRateLimit
	}
	return nil
}

func (l *limiterMock) Alloc(ctx context.Context, dbID int64, collectionIDToPartIDs map[int64][]int64, rt internalpb.RateType, n int) error {
	return l.Check(dbID, collectionIDToPartIDs, rt, n)
}

func newTextSchemaForStorageV3Test(collectionName string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: testInt64Field, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
			{
				FieldID:  102,
				Name:     testFloatVecField,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: strconv.Itoa(testVecDim)},
				},
			},
		},
	}
}

// newBinaryVectorFieldData builds binary-vector field data for tests.
func newBinaryVectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBinaryVectorFieldData(fieldName, numRows, dim)
}

// newFloat16VectorFieldData builds float16-vector field data for tests.
func newFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewFloat16VectorFieldData(fieldName, numRows, dim)
}

// newBFloat16VectorFieldData builds bfloat16-vector field data for tests.
func newBFloat16VectorFieldData(fieldName string, numRows, dim int) *schemapb.FieldData {
	return testutils.NewBFloat16VectorFieldData(fieldName, numRows, dim)
}

// mockTaskNode is a taskmodel.TaskNode stub for tests whose methods are not
// invoked on the exercised paths.
type mockTaskNode struct{}

func (n *mockTaskNode) GetMetaCache() metacache.Cache        { return nil }
func (n *mockTaskNode) MixCoord() types.MixCoordClient       { return nil }
func (n *mockTaskNode) LBPolicy() shardclient.LBPolicy       { return nil }
func (n *mockTaskNode) ShardMgr() shardclient.ShardClientMgr { return nil }
func (n *mockTaskNode) ChMgr() channelmgr.ChannelsMgr        { return nil }
func (n *mockTaskNode) TsoAllocator() taskmodel.TsoAllocator { return nil }

// mockUpsertNode is a test double for the proxy composition root that the
// UpsertTask holds as its node. It exposes a writable tsoAllocator and a
// query method that tests can mockey-patch, mirroring the old *Proxy assertions.
type mockUpsertNode struct {
	mockTaskNode
	tsoAllocator taskmodel.TsoAllocator
	mixCoord     types.MixCoordClient
	lbPolicy     shardclient.LBPolicy
	chMgr        channelmgr.ChannelsMgr
}

func (n *mockUpsertNode) TsoAllocator() taskmodel.TsoAllocator { return n.tsoAllocator }
func (n *mockUpsertNode) MixCoord() types.MixCoordClient       { return n.mixCoord }
func (n *mockUpsertNode) LBPolicy() shardclient.LBPolicy       { return n.lbPolicy }
func (n *mockUpsertNode) ChMgr() channelmgr.ChannelsMgr        { return n.chMgr }
func (n *mockUpsertNode) query(ctx context.Context, qt *dql.QueryTask, sp trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	return nil, segcore.StorageCost{}, merr.WrapErrServiceInternalMsg("mock query not patched")
}

func (n *mockUpsertNode) ExecuteQuery(ctx context.Context, qt taskmodel.Task, sp trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	return n.query(ctx, qt.(*dql.QueryTask), sp)
}

func constructCollectionSchemaByDataType(collectionName string, fieldName2DataType map[string]schemapb.DataType, primaryFieldName string, autoID bool) *schemapb.CollectionSchema {
	fieldsSchema := make([]*schemapb.FieldSchema, 0)

	idx := int64(100)
	for fieldName, dataType := range fieldName2DataType {
		fieldSchema := &schemapb.FieldSchema{
			FieldID:  idx,
			Name:     fieldName,
			DataType: dataType,
		}
		idx++
		if typeutil.IsVectorType(dataType) {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(testVecDim),
				},
			}
		}
		if dataType == schemapb.DataType_VarChar {
			fieldSchema.TypeParams = []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: strconv.Itoa(testMaxVarCharLength),
				},
			}
		}
		if fieldName == primaryFieldName {
			fieldSchema.IsPrimaryKey = true
			fieldSchema.AutoID = autoID
		}

		fieldsSchema = append(fieldsSchema, fieldSchema)
	}

	return &schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: fieldsSchema,
	}
}

// mockTest registers a mockey patch that is automatically unpatched when the
// test finishes, so global mockey patches never leak between tests.
func mockTest(t *testing.T, target any, rets ...any) *mockey.Mocker {
	m := mockey.Mock(target).Return(rets...).Build()
	t.Cleanup(func() { m.UnPatch() })
	return m
}

// mockTestTo registers a mockey patch with a custom implementation that is
// automatically unpatched when the test finishes.
func mockTestTo(t *testing.T, target any, fn any) *mockey.Mocker {
	m := mockey.Mock(target).To(fn).Build()
	t.Cleanup(func() { m.UnPatch() })
	return m
}

// captureProxyLogs captures global logs for the duration of a test.
func captureProxyLogs(t *testing.T) *mlog.TestSink {
	t.Helper()

	return mlog.CaptureGlobalLogs(t, &mlog.Config{
		Level:             "debug",
		Format:            "text",
		DisableCaller:     true,
		DisableTimestamp:  true,
		DisableStacktrace: true,
	})
}
