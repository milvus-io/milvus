package components

import (
	"context"

	grpcindexserver "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice"
)

type IndexService struct {
	svr *grpcindexserver.Server
}

func NewIndexService(ctx context.Context) (*IndexService, error) {
	s := &IndexService{}
	svr, err := grpcindexserver.NewServer(ctx)
	if err != nil {
		return nil, err
	}
	s.svr = svr
	return s, nil
}
func (s *IndexService) Run() error {
	if err := s.svr.Run(); err != nil {
		return err
	}
	return nil
}
func (s *IndexService) Stop() error {
	if err := s.svr.Stop(); err != nil {
		return err
	}
	return nil
}
