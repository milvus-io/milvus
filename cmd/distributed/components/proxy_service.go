package components

import (
	"context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"

	grpcproxyservice "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice"
)

type ProxyService struct {
	svr *grpcproxyservice.Server
}

func NewProxyService(ctx context.Context, factory msgstream.Factory) (*ProxyService, error) {
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
