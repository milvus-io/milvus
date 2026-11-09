package dql

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/views/queryclient"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMaterializeQueryViewRetainsSnapshotAcrossFallback(t *testing.T) {
	paramtable.Init()
	for _, outcome := range []string{"success", "unsupported_request", "format_skew", "schema_skew", "malformed_body"} {
		t.Run(outcome, func(t *testing.T) {
			withMaterializeSchemas(t, materializeSchemaCacheSize)
			transport := queryclient.NewCompositeViewQueryServiceClient(nil, nil)
			client := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, nil, transport, nil)
			defer client.Close()
			plan := queryclient.ShardPlan{
				ShardID:    qviews.ShardID{ReplicaID: 2, VChannel: "p_100v0"},
				Version:    &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 3}, QueryVersion: 7},
				Mvcc:       &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
				WorkNodes:  []qviews.WorkNode{qviews.StreamingNode{PChannel: "p"}},
				Selections: []*viewpb.QueryPlanWorkNode{{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}, SegmentSelection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{42}}}},
			}
			task := &QueryTask{
				RetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 100, OutputFieldsId: []int64{101}, SchemaVersion: 4},
				schema:          mustNewSchemaInfo(mzSchema()), viewQueryClient: client, queryViewPlans: []queryclient.ShardPlan{plan},
				resultBuf: typeutil.NewConcurrentSet[*internalpb.RetrieveResults](),
			}
			if outcome == "schema_skew" {
				schema := mzSchema()
				schema.Fields[1].DataType = schemapb.DataType_Int64
				task.schema = mustNewSchemaInfo(schema)
			}
			var original *viewpb.QueryOnViewRequest
			materialized := mockey.Mock(mockey.GetMethod(transport, "MaterializeOnView")).To(func(_ context.Context, _ qviews.WorkNode, req *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
				original = proto.Clone(req.GetQueryByPk()).(*viewpb.QueryOnViewRequest)
				if outcome == "unsupported_request" {
					return nil, merr.ErrServiceUnimplemented
				}
				res := mzWorkerReply(t, []int64{1}, []int64{10}, []string{"newest"})
				res.Header.FormatVersion = materialize.FormatVersion
				switch outcome {
				case "format_skew":
					res.Header.FormatVersion++
				case "malformed_body":
					res.Body = [][]byte{[]byte("bad IPC")}
				}
				return res, nil
			}).Build()
			defer materialized.UnPatch()
			var queryCalls atomic.Int32
			query := mockey.Mock(mockey.GetMethod(transport, "QueryOnView")).To(func(_ context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
				queryCalls.Add(1)
				require.True(t, proto.Equal(original, req), "fallback must retain schema, view, MVCC and selection")
				return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{Status: merr.Success()}}, nil
			}).Build()
			defer query.UnPatch()
			path, err := task.executeMaterialize(t.Context())
			switch outcome {
			case "success":
				require.NoError(t, err)
				require.Equal(t, executePathMaterialize, path)
				result := task.resultBuf.Collect()
				require.Len(t, result, 1)
				require.Equal(t, []string{"newest"}, textCol(t, result[0]))
				require.EqualValues(t, 0, queryCalls.Load())
			case "malformed_body":
				require.Error(t, err)
				require.Zero(t, queryCalls.Load(), "corrupt IPC is a real error")
			default:
				require.NoError(t, err)
				require.Equal(t, executePathFallback, path)
				require.EqualValues(t, 1, queryCalls.Load())
			}
		})
	}
}

func TestMaterializeQueryViewSizeBudgetFallsBackBeforeOutputQuota(t *testing.T) {
	paramtable.Init()
	quota := &paramtable.Get().QuotaConfig.MaxOutputSize
	originalLimit := quota.GetValue()
	require.NoError(t, paramtable.Get().Save(quota.Key, "16"))
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(quota.Key, originalLimit)) })
	for _, duplicate := range []bool{true, false} {
		name := "duplicate_pk_fits"
		if !duplicate {
			name = "distinct_pks_exceed_quota"
		}
		t.Run(name, func(t *testing.T) {
			withMaterializeSchemas(t, materializeSchemaCacheSize)
			transport := queryclient.NewCompositeViewQueryServiceClient(nil, nil)
			client := queryclient.NewLegacyViewQueryClient(queryclient.ViewQueryClientConfig{}, nil, transport, nil)
			defer client.Close()
			plan := queryclient.ShardPlan{
				ShardID:   qviews.ShardID{ReplicaID: 2, VChannel: "p_100v0"},
				Version:   &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 3}, QueryVersion: 7},
				Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 101, TransformingTimetick: 99},
				WorkNodes: []qviews.WorkNode{qviews.StreamingNode{PChannel: "p"}, qviews.NewQueryNode(11)},
				Selections: []*viewpb.QueryPlanWorkNode{{
					Node:             &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 11}},
					SegmentSelection: &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Include, SegmentIds: []int64{42}},
				}},
			}
			task := &QueryTask{
				RetrieveRequest: &internalpb.RetrieveRequest{CollectionID: 100, OutputFieldsId: []int64{101}, SchemaVersion: 4, Limit: typeutil.Unlimited},
				schema:          mustNewSchemaInfo(mzSchema()), viewQueryClient: client, queryViewPlans: []queryclient.ShardPlan{plan},
				resultBuf: typeutil.NewConcurrentSet[*internalpb.RetrieveResults](),
			}
			row := func(node qviews.WorkNode) (int64, int64, string) {
				if node.NodeType() == qviews.NodeTypeStreamingNode {
					return 1, 20, "newvalue"
				}
				if duplicate {
					return 1, 10, "oldvalue"
				}
				return 2, 10, "oldvalue"
			}
			var mu sync.Mutex
			original := make(map[string]*viewpb.QueryOnViewRequest)
			var materializeCalls, queryCalls atomic.Int32
			materialized := mockey.Mock(mockey.GetMethod(transport, "MaterializeOnView")).To(func(_ context.Context, node qviews.WorkNode, req *viewpb.MaterializeOnViewRequest) (*internalpb.MaterializeResults, error) {
				mu.Lock()
				original[node.String()] = proto.Clone(req.GetQueryByPk()).(*viewpb.QueryOnViewRequest)
				mu.Unlock()
				materializeCalls.Add(1)
				pk, ts, value := row(node)
				res := mzWorkerReply(t, []int64{pk}, []int64{ts}, []string{value})
				res.Header.FormatVersion = materialize.FormatVersion
				res.Header.ResultSize = 16
				return res, nil
			}).Build()
			defer materialized.UnPatch()
			query := mockey.Mock(mockey.GetMethod(transport, "QueryOnView")).To(func(_ context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
				require.EqualValues(t, 2, materializeCalls.Load(), "all Materialize calls must finish before fallback")
				mu.Lock()
				require.True(t, proto.Equal(original[node.String()], req), "fallback must keep the original snapshot and selection")
				mu.Unlock()
				queryCalls.Add(1)
				pk, ts, value := row(node)
				return &viewpb.QueryOnViewResponse{LegacyResults: &internalpb.RetrieveResults{
					Status: merr.Success(), Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}}},
					FieldsData: []*schemapb.FieldData{
						{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{ts}}}}}},
						{FieldId: 101, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{value}}}}}},
					},
				}}, nil
			}).Build()
			defer query.UnPatch()
			path, err := task.executeMaterialize(t.Context())
			require.Equal(t, executePathFallback, path)
			require.EqualValues(t, 2, queryCalls.Load())
			if duplicate {
				require.NoError(t, err)
				results := task.resultBuf.Collect()
				require.Len(t, results, 1)
				require.Equal(t, []int64{1}, results[0].GetIds().GetIntId().GetData())
				require.Equal(t, []string{"newvalue"}, textCol(t, results[0]))
			} else {
				require.ErrorIs(t, err, merr.ErrParameterInvalid, "the reduced output still must obey the quota")
			}
		})
	}
}
