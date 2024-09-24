package proxy

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/samber/lo"
)

type aggrReducer struct {
	reducer funcutil.AggrReducer
	collectionName string
	outputFieldName string
}

func newAggrReducer(collectionName string, outputFieldName string, aggr *planpb.Aggr) (*aggrReducer, error) {
	types := lo.Map(aggr.Arguments, func(col *planpb.ColumnInfo, _ int) schemapb.DataType {
		return col.DataType
	})
	reducer, err := funcutil.NewAggrReducer(aggr.FnName, types)
	if err != nil {
		return nil, fmt.Errorf("couldn't create aggregation reducer: %w", err)
	}

	return &aggrReducer{
		reducer: reducer,
		collectionName: collectionName,
		outputFieldName: outputFieldName,
	}, nil
}

func (r *aggrReducer) Reduce(results []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	// panic("not implemented")
	inputs := lo.Map(results, func(result *internalpb.RetrieveResults, _ int) *internalpb.AggrData {
		return result.GetAggr()
	})

	output, err := r.reducer.ReduceFinal(inputs)
	if err != nil {
		return nil, err
	}
	output.FieldName = r.outputFieldName

	return &milvuspb.QueryResults{
		Status: merr.Success(),
		FieldsData: []*schemapb.FieldData{output},
		CollectionName: r.collectionName,
	}, nil
}
