package mock

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"

	mpb "github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
)

type testMasterServer struct {
	mpb.UnimplementedMasterServer
}

func (s *testMasterServer) CreateCollection(ctx context.Context, in *internalpb.CreateCollectionRequest) (*commonpb.Status, error) {

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_SUCCESS,
	}, nil

}
