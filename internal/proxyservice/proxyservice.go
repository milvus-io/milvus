package proxyservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
)

type ProxyService struct {
	// implement Service

	//nodeClients [] .Interface
	// factory method

}

func (s ProxyService) Init() {
	panic("implement me")
}

func (s ProxyService) Start() {
	panic("implement me")
}

func (s ProxyService) Stop() {
	panic("implement me")
}

func (s ProxyService) GetServiceStates() (internalpb2.ServiceStates, error) {
	panic("implement me")
}

func (s ProxyService) GetTimeTickChannel() (string, error) {
	panic("implement me")
}

func (s ProxyService) GetStatisticsChannel() (string, error) {
	panic("implement me")
}

func (s ProxyService) RegisterLink() (proxypb.RegisterLinkResponse, error) {
	panic("implement me")
}

func (s ProxyService) RegisterNode(request proxypb.RegisterNodeRequest) (proxypb.RegisterNodeResponse, error) {
	panic("implement me")
}

func (s ProxyService) InvalidateCollectionMetaCache(request proxypb.InvalidateCollMetaCacheRequest) error {
	panic("implement me")
}

func NewProxyServiceImpl() Interface {
	return &ProxyService{}
}
