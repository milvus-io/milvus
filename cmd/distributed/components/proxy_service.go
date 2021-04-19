package components

import (
	"context"

	grpcproxyservice "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type ProxyService struct {
	svr *grpcproxyservice.Server
}

func NewProxyService(ctx context.Context, factory msgstream.Factory) (*ProxyService, error) {
	var err error
	service := &ProxyService{}
	svr, err := grpcproxyservice.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	service.svr = svr
	return service, nil
}

func (s *ProxyService) Run() error {
	if err := s.svr.Run(); err != nil {
		return err
	}
	return nil
}

func (s *ProxyService) Stop() error {
	if err := s.svr.Stop(); err != nil {
		return err
	}
	return nil
}
