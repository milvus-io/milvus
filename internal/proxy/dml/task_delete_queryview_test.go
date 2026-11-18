package dml

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestComplexDeleteQueryViewBatch(t *testing.T) {
	for _, scenario := range []string{"int64", "varchar", "empty", "cold", "cold_disabled", "cold_load_error", "query_error", "incomplete", "missing_size", "rate_limit", "enqueue_error", "write_error"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			cold := strings.HasPrefix(scenario, "cold")
			if cold {
				params := paramtable.Get()
				key := params.ProxyCfg.EnableAutoLoad.Key
				require.NoError(t, params.Save(key, strconv.FormatBool(scenario != "cold_disabled")))
				t.Cleanup(func() { require.NoError(t, params.Reset(key)) })
			}
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
			if scenario != "missing_size" {
				response.Plans = []queryclient.ShardPlan{{CollectionSizeBytes: proto.Int64(12000)}}
			}
			if scenario == "incomplete" {
				response.Results[1].HasMoreResult = true
			}
			client := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, nil, nil, nil)
			var captured *internalpb.RetrieveRequest
			queryCalls := 0
			query := mockey.Mock(mockey.GetMethod(client.Legacy(), "Query")).To(func(_ context.Context, req *queryclient.LegacyQueryRequest) (*queryclient.LegacyQueryResult, error) {
				captured = proto.Clone(req.Req).(*internalpb.RetrieveRequest)
				queryCalls++
				if cold && queryCalls == 1 {
					return nil, merr.WrapErrCollectionNotLoaded(int64(10))
				}
				if scenario == "query_error" {
					return nil, merr.ErrServiceUnavailable
				}
				return response, nil
			}).Build()
			defer query.UnPatch()
			// Transient query errors consult the authoritative load config.
			coord := mocks.NewMockMixCoordClient(t)
			if scenario == "cold" || scenario == "cold_load_error" {
				status := merr.Success()
				if scenario == "cold_load_error" {
					status = merr.Status(merr.ErrServiceUnavailable)
				}
				coord.EXPECT().EnsureCollectionReady(mock.Anything, &querypb.EnsureCollectionReadyRequest{
					CollectionID: 10, ExpectedVchannels: []string{"batch_shard"},
				}).Return(status, nil).Once()
			}
			mixCoord := mockey.Mock((*mockTaskNode).MixCoord).Return(coord).Build()
			defer mixCoord.UnPatch()
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
				node: &mockTaskNode{}, vChannels: []string{"batch_shard"},
				req:    &milvuspb.DeleteRequest{CollectionName: "batch_delete", ConsistencyLevel: commonpb.ConsistencyLevel_Strong},
				result: &milvuspb.MutationResult{Status: merr.Success()},
				schema: schema, plan: plan, dbID: 9, collectionID: 10, partitionIDs: []int64{11},
				idAllocator: &allocator.IDAllocator{}, tsoAllocatorIns: &mockTsoAllocator{}, limiter: &limiterMock{},
				// No legacy LB: falling back to GetShardLeaders would fail this test.
			}
			runner.SetViewQueryClient(client)
			runner.MeasurePayload()
			err = runner.Run(ctx)
			switch scenario {
			case "query_error", "enqueue_error", "write_error", "cold_load_error":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "cold_disabled":
				require.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
			case "incomplete", "missing_size":
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
			if scenario == "cold" {
				require.Equal(t, 2, queryCalls)
			} else {
				require.Equal(t, 1, queryCalls)
			}
			wrote := scenario == "int64" || scenario == "varchar" || scenario == "cold"
			if wrote {
				require.Equal(t, 1, produce.Times())
				require.True(t, proto.Equal(ids(values...), produced), "overlapping worker results must produce each PK once")
				require.EqualValues(t, 3, runner.Result().GetDeleteCnt())
				require.EqualValues(t, 200, runner.Result().GetTimestamp())
				require.EqualValues(t, 12, runner.AllQueryCnt())
				require.EqualValues(t, 40, runner.ScannedRemoteBytes())
				require.EqualValues(t, 60, runner.ScannedTotalBytes())
				require.Equal(t, primaryKeyBytes(ids(values...)), runner.DeletedPKBytes())
			} else {
				require.Zero(t, runner.Result().GetDeleteCnt())
			}
			if err == nil {
				require.EqualValues(t, 12000, runner.CollectionSizeBytes())
			}
			if scenario == "query_error" || scenario == "incomplete" || scenario == "empty" {
				require.Zero(t, limitCalls)
			}
			if !wrote && scenario != "enqueue_error" && scenario != "write_error" {
				require.Nil(t, produced, "failed, incomplete or empty reads must not enqueue a delete")
			}
		})
	}
}
