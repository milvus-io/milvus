package querynode

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

func Test_createInternalReducer(t *testing.T) {
	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			IsCount: false,
		},
	}

	var r internalReducer
	var ok bool

	r = createInternalReducer(context.TODO(), req, nil)
	_, ok = r.(*defaultLimitReducer)
	assert.True(t, ok)

	req.Req.IsCount = true

	r = createInternalReducer(context.TODO(), req, nil)
	_, ok = r.(*cntReducer)
	assert.True(t, ok)
}

func Test_createSegCoreReducer(t *testing.T) {

	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			IsCount: false,
		},
	}

	var r segCoreReducer
	var ok bool

	r = createSegCoreReducer(context.TODO(), req, nil)
	_, ok = r.(*defaultLimitReducerSegcore)
	assert.True(t, ok)

	req.Req.IsCount = true

	r = createSegCoreReducer(context.TODO(), req, nil)
	_, ok = r.(*cntReducerSegCore)
	assert.True(t, ok)
}
