package dml

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type deleteConsistencyQueryClient struct{ types.QueryNodeClient }

func TestDeleteQuerySnapshotConsistency(t *testing.T) {
	paramtable.Init()
	for _, consistency := range []commonpb.ConsistencyLevel{
		commonpb.ConsistencyLevel_Strong,
		commonpb.ConsistencyLevel_Session,
		commonpb.ConsistencyLevel_Bounded,
		commonpb.ConsistencyLevel_Eventually,
		commonpb.ConsistencyLevel_Customized,
	} {
		t.Run(consistency.String(), func(t *testing.T) {
			var captured []*querypb.QueryRequest
			patch := mockey.Mock((*deleteConsistencyQueryClient).QueryStream).To(func(_ *deleteConsistencyQueryClient, _ context.Context, req *querypb.QueryRequest, _ ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
				// Copy while the callback owns the request; do not retain a
				// potentially stack-allocated pointer across the mocked call.
				captured = append(captured, proto.Clone(req).(*querypb.QueryRequest))
				return nil, context.Canceled
			}).Build()
			defer patch.UnPatch()
			runner := &DeleteRunner{
				req:          &milvuspb.DeleteRequest{ConsistencyLevel: consistency},
				ts:           tsoutil.ComposeTSByTime(time.Now()),
				msgID:        10,
				collectionID: 20,
				partitionIDs: []int64{30},
			}
			plan, fields := []byte{1, 2, 3}, []int64{100, 1}
			execute := runner.getStreamingQueryAndDelteFunc(plan, fields)
			// Fan-out and a retry must construct independent requests with
			// the same guarantee, and let Strong reads select their snapshot.
			channels := []string{"p0_20v0", "p1_20v1", "p0_20v0"}
			for _, channel := range channels {
				require.ErrorIs(t, execute(context.Background(), 40, &deleteConsistencyQueryClient{}, channel), context.Canceled)
			}
			require.Len(t, captured, 3)
			for i, request := range captured {
				inner := request.GetReq()
				require.Equal(t, consistency, inner.GetConsistencyLevel())
				require.Equal(t, dql.ParseGuaranteeTsFromConsistency(runner.ts, runner.ts, consistency), inner.GetGuaranteeTimestamp())
				if consistency == commonpb.ConsistencyLevel_Strong {
					require.Zero(t, inner.GetMvccTimestamp())
				} else {
					require.Equal(t, runner.ts, inner.GetMvccTimestamp(), "preserve non-Strong snapshot behavior")
				}
				require.Equal(t, runner.collectionID, inner.GetCollectionID())
				require.Equal(t, runner.partitionIDs, inner.GetPartitionIDs())
				require.Equal(t, plan, inner.GetSerializedExprPlan())
				require.Equal(t, fields, inner.GetOutputFieldsId())
				require.Equal(t, metrics.DeleteQueryLabel, inner.GetQueryLabel())
				require.False(t, inner.GetIsIterator())
				require.Equal(t, []string{channels[i]}, request.GetDmlChannels())
			}
		})
	}
}
