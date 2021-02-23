package components

import (
	"context"

	grpcdataserviceclient "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type DataService struct {
	ctx context.Context
	svr *grpcdataserviceclient.Server
}

func NewDataService(ctx context.Context, factory msgstream.Factory) (*DataService, error) {
	s, err := grpcdataserviceclient.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &DataService{
		ctx: ctx,
		svr: s,
	}, nil
}

func (s *DataService) Run() error {
	if err := s.svr.Run(); err != nil {
		return err
	}
	return nil
}

func (s *DataService) Stop() error {
	if err := s.svr.Stop(); err != nil {
		return err
	}
	return nil
}
