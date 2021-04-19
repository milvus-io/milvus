package components

import (
	"context"
	"log"

	grpcqueryservice "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type QueryService struct {
	ctx context.Context
	svr *grpcqueryservice.Server
}

func NewQueryService(ctx context.Context, factory msgstream.Factory) (*QueryService, error) {
	svr, err := grpcqueryservice.NewServer(ctx, factory)
	if err != nil {
		panic(err)
	}

	return &QueryService{
		ctx: ctx,
		svr: svr,
	}, nil
}

func (qs *QueryService) Run() error {
	if err := qs.svr.Run(); err != nil {
		panic(err)
	}
	log.Println("QueryService successfully started ...")
	return nil
}

func (qs *QueryService) Stop() error {
	if err := qs.svr.Stop(); err != nil {
		return err
	}
	return nil
}
