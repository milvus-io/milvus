package querynode

import (
	"context"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
)

type mockProxy struct {
}

func (m *mockProxy) Init() error {
	return nil
}

func (m *mockProxy) Start() error {
	return nil
}

func (m *mockProxy) Stop() error {
	return nil
}

func (m *mockProxy) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	return nil, nil
}

func (m *mockProxy) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	return nil, nil
}

func (m *mockProxy) Register() error {
	return nil
}

func (m *mockProxy) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *mockProxy) ReleaseDQLCache(ctx context.Context, in *proxypb.ReleaseDQLCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

func (m *mockProxy) SendSearchResult(ctx context.Context, req *internalpb.SearchResults) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func (m *mockProxy) SendRetrieveResult(ctx context.Context, req *internalpb.RetrieveResults) (*commonpb.Status, error) {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, nil
}

func newMockProxy() types.Proxy {
	return &mockProxy{}
}

func mockProxyCreator() proxyCreatorFunc {
	return func(ctx context.Context, addr string) (types.Proxy, error) {
		return newMockProxy(), nil
	}
}
