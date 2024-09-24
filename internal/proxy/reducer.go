package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

type milvusReducer interface {
	Reduce([]*internalpb.RetrieveResults) (*milvuspb.QueryResults, error)
}

func createMilvusReducer(ctx context.Context, params *queryParams, req *internalpb.RetrieveRequest, schema *schemapb.CollectionSchema, plan *planpb.PlanNode, collectionName string) milvusReducer {
	if plan.GetQuery().GetIsCount() {
		return &cntReducer{
			collectionName: collectionName,
		}
	} else if plan.GetQuery().GetAggregation() != nil {
		outputFieldName := GenOutputFieldNameForAggr(schema, plan.GetQuery().GetAggregation())
		reducer, err := newAggrReducer(collectionName, outputFieldName, plan.GetQuery().GetAggregation())
		if err != nil {
			// TODO:ashkrisk surely there's a better way
			log.Fatal("could not create aggr reducer", zap.Error(err))
		}
		return reducer
	}
	return newDefaultLimitReducer(ctx, params, req, schema, collectionName)
}

func GenOutputFieldNameForAggr(schema *schemapb.CollectionSchema, aggr *planpb.Aggr) string {
	fallback := aggr.FnName + "(...)"
	name := aggr.FnName + "("

	if len(aggr.Arguments) > 0 {
		helper, err := typeutil.CreateSchemaHelper(schema)
		if err != nil {
			log.Error("Couldn't create schemaHelper from schema")
			return fallback
		}

		for i, arg := range aggr.Arguments {
			field, err := helper.GetFieldFromID(arg.GetFieldId())
			if err != nil {
				log.Error("Unable to generate field name from ID")
				return fallback
			}

			if i != 0 {
				name += ","
			}
			name += field.GetName()
		}
	}

	name += ")"
	return name
}
