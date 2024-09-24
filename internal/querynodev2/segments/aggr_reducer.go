package segments

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/samber/lo"
)

type aggrReducerInternal struct {
	reducer funcutil.AggrReducer
}

func newAggrReducerInternal(aggr *planpb.Aggr) (*aggrReducerInternal, error) {
	types := lo.Map(aggr.Arguments, func(col *planpb.ColumnInfo, _ int) schemapb.DataType {
		return col.DataType
	})
	reducer, err := funcutil.NewAggrReducer(aggr.FnName, types)
	if err != nil {
		return nil, fmt.Errorf("couldn't create aggregation reducer: %w", err)
	}

	return &aggrReducerInternal {
		reducer: reducer,
	}, nil
}

func (r *aggrReducerInternal) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	inputs := lo.Map(results, func(result *internalpb.RetrieveResults, _ int) *internalpb.AggrData {
		return result.Aggr
	})
	output, err := r.reducer.Reduce(inputs)
	if err != nil {
		return nil, err
	}

	return &internalpb.RetrieveResults{
		Status: merr.Success(),
		Aggr: output,
	}, nil
}

type aggrReducerSegcore struct {
	reducer funcutil.AggrReducer
}

func newAggrReducerSegcore(aggr *planpb.Aggr) (*aggrReducerSegcore, error) {
	types := lo.Map(aggr.Arguments, func(col *planpb.ColumnInfo, _ int) schemapb.DataType {
		return col.DataType
	})
	reducer, err := funcutil.NewAggrReducer(aggr.FnName, types)
	if err != nil {
		return nil, fmt.Errorf("couldn't create aggregation reducer: %w", err)
	}

	return &aggrReducerSegcore{
		reducer: reducer,
	}, nil
}

func (r *aggrReducerSegcore) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults, segments []Segment, plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	inputs := lo.Map(results, func(result *segcorepb.RetrieveResults, _ int) *internalpb.AggrData {
		// TODO
		return result.GetAggr()
	})

	output, err := r.reducer.Reduce(inputs)
	if err != nil {
		return nil, err
	}

	return &segcorepb.RetrieveResults{
		Aggr: output,
	}, nil
}
