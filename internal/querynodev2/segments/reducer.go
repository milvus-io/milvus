package segments

import (
	"context"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"google.golang.org/protobuf/proto"
)

type internalReducer interface {
	Reduce(context.Context, []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error)
}

func CreateInternalReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) internalReducer {
	var plan planpb.PlanNode
	// TODO:ashkrisk unmarshall overhead may be high
	err := proto.Unmarshal(req.GetReq().GetSerializedExprPlan(), &plan)
	if err != nil {
		log.Error("couldn't unmarshall query plan", zap.Error(err))
	}
	queryPlan, ok := plan.GetNode().(*planpb.PlanNode_Query)
	if !ok {
		log.Error("plan in QueryRequest is not a QueryPlan")
	}

	if req.GetReq().GetIsCount() {
		return &cntReducer{}
	}
	if aggr := queryPlan.Query.GetAggregation(); aggr != nil {
		reducer, err := newAggrReducerInternal(aggr)
		if err != nil {
			// TODO:ashkrisk remove fatality
			log.Fatal("Couldn't create aggregation reducer", zap.Error(err))
		}
		return reducer
	}
	return newDefaultLimitReducer(req, schema)
}

type segCoreReducer interface {
	Reduce(context.Context, []*segcorepb.RetrieveResults, []Segment, *RetrievePlan) (*segcorepb.RetrieveResults, error)
}

func CreateSegCoreReducer(req *querypb.QueryRequest, schema *schemapb.CollectionSchema, manager *Manager) segCoreReducer {
	if req.GetReq().GetIsCount() {
		return &cntReducerSegCore{}
	}

	// TODO:ashkrisk there's no need to deserialize here since plan(C) is already available in caller
	var plan planpb.PlanNode
	err := proto.Unmarshal(req.GetReq().GetSerializedExprPlan(), &plan)
	if err != nil {
		log.Fatal("Couldn't deserialize plan in segcorereducer")
		return nil // just in case
	}
	if aggr := plan.GetQuery().GetAggregation(); aggr != nil {
		reducer, err := newAggrReducerSegcore(aggr)
		if err != nil {
			// TODO:ashkrisk this is the third time I'm repeating myself
			log.Fatal("Couldn't create aggregation reducer", zap.Error(err))
		}
		return reducer
	}

	return newDefaultLimitReducerSegcore(req, schema, manager)
}

type TimestampedRetrieveResult[T interface {
	typeutil.ResultWithID
	GetFieldsData() []*schemapb.FieldData
}] struct {
	Result     T
	Timestamps []int64
}

func (r *TimestampedRetrieveResult[T]) GetIds() *schemapb.IDs {
	return r.Result.GetIds()
}

func (r *TimestampedRetrieveResult[T]) GetHasMoreResult() bool {
	return r.Result.GetHasMoreResult()
}

func (r *TimestampedRetrieveResult[T]) GetTimestamps() []int64 {
	return r.Timestamps
}

func NewTimestampedRetrieveResult[T interface {
	typeutil.ResultWithID
	GetFieldsData() []*schemapb.FieldData
}](result T) (*TimestampedRetrieveResult[T], error) {
	tsField, has := lo.Find(result.GetFieldsData(), func(fd *schemapb.FieldData) bool {
		return fd.GetFieldId() == common.TimeStampField
	})
	if !has {
		return nil, merr.WrapErrServiceInternal("RetrieveResult does not have timestamp field")
	}
	timestamps := tsField.GetScalars().GetLongData().GetData()
	idSize := typeutil.GetSizeOfIDs(result.GetIds())

	if idSize != len(timestamps) {
		return nil, merr.WrapErrServiceInternal("id length is not equal to timestamp length")
	}

	return &TimestampedRetrieveResult[T]{
		Result:     result,
		Timestamps: timestamps,
	}, nil
}
