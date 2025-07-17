package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestNewInterceptor(t *testing.T) {
	mixc := &mocks.MockMixCoordClient{}
	mixc.EXPECT().CheckHealth(mock.Anything, mock.Anything).Return(&milvuspb.CheckHealthResponse{IsHealthy: false}, nil)
	node := &Proxy{
		mixCoord: mixc,
		session:  &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}},
	}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.On("DescribeCollection", mock.Anything, mock.Anything).Return(nil, merr.ErrCollectionNotFound).Maybe()
	var err error
	globalMetaCache, err = NewMetaCache(mixCoord, nil)
	assert.NoError(t, err)
	interceptor, err := NewInterceptor[*milvuspb.DescribeCollectionRequest, *milvuspb.DescribeCollectionResponse](node, "DescribeCollection")
	assert.NoError(t, err)
	resp, err := interceptor.Call(context.Background(), &milvuspb.DescribeCollectionRequest{
		DbName:         "test",
		CollectionName: "test",
	})
	assert.NoError(t, err)
	assert.Equal(t, "can't find collection[database=test][collection=test]", resp.Status.Reason)
}
