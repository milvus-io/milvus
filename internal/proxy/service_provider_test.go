package proxy

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewInterceptor(t *testing.T) {
	mixc := &mocks.MockMixCoordClient{}
	mixc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: false}, nil)
	node := &Proxy{
		mixCoord: mixc,
		session:  &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
	}
	interceptor, err := NewInterceptor[*milvuspb.DescribeCollectionRequest, *milvuspb.DescribeCollectionResponse](node, "DescribeCollection")
	assert.NoError(t, err)
	_, err = interceptor.Call(context.Background(), &milvuspb.DescribeCollectionRequest{})
	assert.NoError(t, err)
}
