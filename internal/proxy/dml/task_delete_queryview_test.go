package dml

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
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
