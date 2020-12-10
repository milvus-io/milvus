package indexbuilder

import (
	"context"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
)

const (
	reqTimeoutInterval = time.Second * 10
)

func (b *Builder) BuildIndex(ctx context.Context, request *indexbuilderpb.BuildIndexRequest) (*indexbuilderpb.BuildIndexResponse, error) {
	panic("implement me")
}

func (b *Builder) DescribeIndex(ctx context.Context, request *indexbuilderpb.DescribleIndexRequest) (*indexbuilderpb.DescribleIndexResponse, error) {
	panic("implement me")
}

func (b *Builder) GetIndexFilePaths(ctx context.Context, request *indexbuilderpb.GetIndexFilePathsRequest) (*indexbuilderpb.GetIndexFilePathsResponse, error) {
	panic("implement me")
}
