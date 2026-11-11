package dml

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestComplexDeleteQueryViewBatch(t *testing.T) {
	for _, scenario := range []string{"int64", "varchar", "empty", "query_error", "incomplete", "rate_limit", "enqueue_error", "write_error"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			pkType := schemapb.DataType_Int64
			values := []any{int64(1), int64(2), int64(3)}
			if scenario == "varchar" {
				pkType = schemapb.DataType_VarChar
				values = []any{"a", "b", "c"}
			}
			schema := mustNewSchemaInfo(&schemapb.CollectionSchema{Version: 7, Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: pkType},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
			}})
			plan, err := planparserv2.CreateRetrievePlan(schema.SchemaHelper, "value < 3", nil)
			require.NoError(t, err)
			ids := func(values ...any) *schemapb.IDs {
				result := &schemapb.IDs{}
				for _, value := range values {
					typeutil.AppendID(result, value)
				}
				return result
			}
			response := &queryclient.LegacyQueryResult{Results: []*internalpb.RetrieveResults{
				{Ids: ids(values[0], values[1]), AllRetrieveCount: 7, ScannedRemoteBytes: 10, ScannedTotalBytes: 20},
				{Ids: ids(values[1], values[2]), AllRetrieveCount: 5, ScannedRemoteBytes: 30, ScannedTotalBytes: 40},
			}}
			if scenario == "empty" {
				response.Results = nil
			}
			if scenario == "incomplete" {
				response.Results[1].HasMoreResult = true
			}
			client := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, nil, nil, nil)
			var captured *internalpb.RetrieveRequest
			query := mockey.Mock(mockey.GetMethod(client.Legacy(), "Query")).To(func(_ context.Context, req *queryclient.LegacyQueryRequest) (*queryclient.LegacyQueryResult, error) {
				captured = proto.Clone(req.Req).(*internalpb.RetrieveRequest)
				if scenario == "query_error" {
					return nil, merr.ErrServiceUnavailable
				}
				return response, nil
			}).Build()
			defer query.UnPatch()
			// Transient query errors consult the authoritative load config.
			loadInfo := mockey.Mock((*mocks.MockMixCoordClient).GetQueryViewLoadInfo).Return(&querypb.GetQueryViewLoadInfoResponse{
				Status: merr.Success(), PartitionIDs: []int64{11},
			}, nil).Build()
			defer loadInfo.UnPatch()

			idPatch := mockey.Mock((*allocator.IDAllocator).AllocOne).Return(int64(42), nil).Build()
			defer idPatch.UnPatch()
			tsoPatch := mockey.Mock((*mockTsoAllocator).AllocOne).Return(uint64(100), nil).Build()
			defer tsoPatch.UnPatch()

			var limited *schemapb.IDs
			limitCalls := 0
			limit := mockey.Mock((*limiterMock).Alloc).To(func(_ *limiterMock, _ context.Context, dbID int64, partitions map[int64][]int64, rateType internalpb.RateType, n int) error {
				limitCalls++
				require.Equal(t, int64(9), dbID)
				require.Equal(t, map[int64][]int64{10: {11}}, partitions)
				require.Equal(t, internalpb.RateType_DMLDelete, rateType)
				limited = ids(values...)
				require.Equal(t, proto.Size(limited), n)
				if scenario == "rate_limit" {
					return merr.ErrServiceRateLimit
				}
				return nil
			}).Build()
			defer limit.UnPatch()
			var produced *schemapb.IDs
			produce := mockey.Mock((*DeleteRunner).produce).To(func(_ *DeleteRunner, ctx context.Context, primaryKeys *schemapb.IDs, partitionID int64) (*DeleteTask, error) {
				require.Equal(t, int64(11), partitionID)
				produced = proto.Clone(primaryKeys).(*schemapb.IDs)
				if scenario == "enqueue_error" {
					return nil, merr.ErrServiceUnavailable
				}
				task := &DeleteTask{Condition: NewTaskCondition(ctx), count: int64(typeutil.GetSizeOfIDs(primaryKeys)), sessionTS: 200}
				if scenario == "write_error" {
					task.Notify(merr.ErrServiceUnavailable)
				} else {
					task.Notify(nil)
				}
				return task, nil
			}).Build()
			defer produce.UnPatch()

			runner := &DeleteRunner{
				node:   &mockUpsertNode{mixCoord: &mocks.MockMixCoordClient{}},
				req:    &milvuspb.DeleteRequest{CollectionName: "batch_delete", ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
				result: &milvuspb.MutationResult{Status: merr.Success()},
				schema: schema, plan: plan, dbID: 9, collectionID: 10, partitionIDs: []int64{11},
				idAllocator: &allocator.IDAllocator{}, tsoAllocatorIns: &mockTsoAllocator{}, limiter: &limiterMock{},
				// No legacy LB: falling back to GetShardLeaders would fail this test.
			}
			runner.SetViewQueryClient(client)
			err = runner.Run(ctx)
			switch scenario {
			case "query_error", "enqueue_error", "write_error":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "incomplete":
				require.ErrorIs(t, err, merr.ErrServiceInternal)
			case "rate_limit":
				require.ErrorIs(t, err, merr.ErrServiceRateLimit)
			default:
				require.NoError(t, err)
			}
			if scenario == "query_error" {
				require.Equal(t, 1, loadInfo.Times())
			} else {
				require.Zero(t, loadInfo.Times())
			}
			require.NotNil(t, captured)
			require.Equal(t, int64(9), captured.GetDbID())
			require.Equal(t, int64(10), captured.GetCollectionID())
			require.EqualValues(t, 7, captured.GetSchemaVersion())
			require.Equal(t, []int64{11}, captured.GetPartitionIDs())
			require.Equal(t, commonpb.ConsistencyLevel_Strong, captured.GetConsistencyLevel())
			require.EqualValues(t, typeutil.Unlimited, captured.GetLimit())
			require.Equal(t, metrics.DeleteQueryLabel, captured.GetQueryLabel())
			require.Equal(t, []int64{100, common.TimeStampField}, captured.GetOutputFieldsId())
			parsed := &planpb.PlanNode{}
			require.NoError(t, proto.Unmarshal(captured.GetSerializedExprPlan(), parsed))
			require.EqualValues(t, typeutil.Unlimited, parsed.GetQuery().GetLimit())
			require.True(t, proto.Equal(plan.GetQuery().GetPredicates(), parsed.GetQuery().GetPredicates()))
			if scenario == "int64" || scenario == "varchar" {
				require.True(t, proto.Equal(ids(values...), produced), "overlapping worker results must produce each PK once")
				require.EqualValues(t, 3, runner.Result().GetDeleteCnt())
				require.EqualValues(t, 200, runner.Result().GetTimestamp())
				require.EqualValues(t, 12, runner.AllQueryCnt())
				require.EqualValues(t, 40, runner.ScannedRemoteBytes())
				require.EqualValues(t, 60, runner.ScannedTotalBytes())
			} else {
				require.Zero(t, runner.Result().GetDeleteCnt())
			}
			if scenario == "query_error" || scenario == "incomplete" || scenario == "empty" {
				require.Zero(t, limitCalls)
			}
			if scenario != "int64" && scenario != "varchar" && scenario != "enqueue_error" && scenario != "write_error" {
				require.Nil(t, produced, "failed, incomplete or empty reads must not enqueue a delete")
			}
		})
	}
}

// A failed read says nothing about the metadata it was bound to. The next attempt
// must resolve the request again and read at a new snapshot, and only the final
// attempt's rows are deleted. The request still counts once.
func TestComplexDeleteQueryViewReplansEachAttempt(t *testing.T) {
	ctx := context.Background()
	enabled := featureusage.Enabled()
	t.Cleanup(func() { featureusage.SetEnabled(enabled) })
	featureusage.SetEnabled(true)
	newSchema := func(version int32, valueID int64) *schemaInfo {
		return mustNewSchemaInfo(&schemapb.CollectionSchema{Version: version, Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: valueID, Name: "value", DataType: schemapb.DataType_Int64},
		}})
	}
	retired, current := newSchema(7, 101), newSchema(8, 102)

	cache := NewMockCache(t)
	cache.EXPECT().GetDatabaseInfo(mock.Anything, "db").Return(&databaseInfo{DBID: 9}, nil)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "tenant").Return(int64(10), nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "tenant", int64(10)).Return(&collectionInfo{
		CollID: 10, VChannels: []string{"v-retired"}, Schema: retired,
	}, nil).Once()
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "tenant", int64(10)).Return(&collectionInfo{
		CollID: 10, VChannels: []string{"v-current"}, Schema: current,
	}, nil).Once()

	client := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, nil, nil, nil)
	var reads []*queryclient.LegacyQueryRequest
	query := mockey.Mock(mockey.GetMethod(client.Legacy(), "Query")).To(func(_ context.Context, req *queryclient.LegacyQueryRequest) (*queryclient.LegacyQueryResult, error) {
		reads = append(reads, req)
		if len(reads) == 1 {
			return nil, merr.WrapErrServiceInternalMsg("read failed")
		}
		return &queryclient.LegacyQueryResult{Results: []*internalpb.RetrieveResults{
			{Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}}, AllRetrieveCount: 2},
		}}, nil
	}).Build()
	defer query.UnPatch()
	var produced []*schemapb.IDs
	produce := mockey.Mock((*DeleteRunner).produce).To(func(dr *DeleteRunner, ctx context.Context, primaryKeys *schemapb.IDs, _ int64) (*DeleteTask, error) {
		require.Equal(t, []vChan{"v-current"}, dr.vChannels, "the write follows the attempt that read the rows")
		produced = append(produced, proto.Clone(primaryKeys).(*schemapb.IDs))
		task := &DeleteTask{Condition: NewTaskCondition(ctx), count: int64(typeutil.GetSizeOfIDs(primaryKeys))}
		task.Notify(nil)
		return task, nil
	}).Build()
	defer produce.UnPatch()

	runner := &DeleteRunner{
		node: &mockUpsertNode{retryOnce: true}, metaCache: cache,
		req: &milvuspb.DeleteRequest{
			DbName: "db", CollectionName: "tenant", Expr: "value < {limit}",
			ExprTemplateValues: map[string]*schemapb.TemplateValue{"limit": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 3}}},
		},
		idAllocator: &mockIDAllocatorInterface{}, tsoAllocatorIns: &mockTsoAllocator{},
	}
	runner.SetViewQueryClient(client)

	before := featureCounts()
	require.NoError(t, runner.Init(ctx))
	// What the first attempt resolved must not survive into the replan.
	runner.partitionIDs = []int64{11}
	require.NoError(t, runner.Run(ctx))

	counted := featureDelta(before, featureCounts())
	require.Contains(t, counted, "delete_mode=filter")
	require.Contains(t, counted, featureusage.FeatureExprTemplateValues.Name())
	for name, count := range counted {
		require.EqualValues(t, 1, count, name)
	}
	require.Len(t, reads, 2)
	require.EqualValues(t, 7, reads[0].Req.GetSchemaVersion())
	require.EqualValues(t, 8, reads[1].Req.GetSchemaVersion())
	require.Equal(t, []int64{11}, reads[0].Req.GetPartitionIDs())
	require.Empty(t, reads[1].Req.GetPartitionIDs())
	require.Same(t, current.CollectionSchema, reads[1].Schema)
	require.Greater(t, reads[1].BarrierMVCC, reads[0].BarrierMVCC)
	require.Equal(t, reads[1].BarrierMVCC, reads[1].Req.GetGuaranteeTimestamp())
	parsed := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(reads[1].Req.GetSerializedExprPlan(), parsed))
	require.EqualValues(t, typeutil.Unlimited, parsed.GetQuery().GetLimit())
	require.EqualValues(t, 102, parsed.GetQuery().GetPredicates().GetUnaryRangeExpr().GetColumnInfo().GetFieldId())
	require.Len(t, produced, 1)
	require.Equal(t, []int64{1, 2}, produced[0].GetIntId().GetData())
	require.EqualValues(t, 2, runner.Result().GetDeleteCnt())
	require.EqualValues(t, 2, runner.AllQueryCnt())
}

func TestComplexDeleteQueryViewRequiresQueryRunner(t *testing.T) {
	runner := &DeleteRunner{node: &mockTaskNode{}}
	require.ErrorIs(t, runner.complexDeleteByQueryView(context.Background()), merr.ErrServiceInternal)
}

// A drop and recreate, or an alias reassignment, must not redirect the delete.
func TestComplexDeleteRetryRejectsReplacementCollection(t *testing.T) {
	cache := NewMockCache(t)
	cache.EXPECT().GetDatabaseInfo(mock.Anything, "db").Return(&databaseInfo{DBID: 9}, nil)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "tenant").Return(int64(11), nil)
	runner := &DeleteRunner{
		node: &mockUpsertNode{}, metaCache: cache,
		req:          &milvuspb.DeleteRequest{DbName: "db", CollectionName: "tenant", Expr: "value < 3"},
		collectionID: 10, readAttempted: true,
	}

	require.ErrorIs(t, runner.complexDeleteByQueryView(t.Context()), merr.ErrCollectionNotFound)
	require.EqualValues(t, 10, runner.collectionID)
}
