package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
)

func Test_createMilvusReducer(t *testing.T) {
	n := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				IsCount: false,
			},
		},
	}
	var r milvusReducer

	ctx := context.TODO()
	r = createMilvusReducer(ctx, nil, nil, nil, n, "")
	_, ok := r.(*defaultLimitReducer)
	assert.True(t, ok)

	n.Node.(*planpb.PlanNode_Query).Query.IsCount = true
	r = createMilvusReducer(ctx, nil, nil, nil, n, "")
	_, ok = r.(*cntReducer)
	assert.True(t, ok)

	req := &internalpb.RetrieveRequest{
		IterationExtensionReduceRate: 100,
	}
	params := &queryParams{
		limit: 10,
	}
	r = createMilvusReducer(ctx, params, req, nil, nil, "")
	defaultReducer, typeOk := r.(*defaultLimitReducer)
	assert.True(t, typeOk)
	assert.Equal(t, int64(10*100), defaultReducer.params.limit)

	req = &internalpb.RetrieveRequest{
		IterationExtensionReduceRate: 1000,
	}
	params = &queryParams{
		limit: 100,
	}
	r = createMilvusReducer(ctx, params, req, nil, nil, "")
	defaultReducer, typeOk = r.(*defaultLimitReducer)
	assert.True(t, typeOk)
	assert.Equal(t, int64(16384), defaultReducer.params.limit)
}
