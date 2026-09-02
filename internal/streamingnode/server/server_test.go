//go:build test && dynamic

package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestRegisterGRPCServiceRegistersQueryViewServices(t *testing.T) {
	resource.InitForTest(t)

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	s := &Server{
		handlerService: service.NewHandlerService(nil),
		managerService: service.NewManagerService(nil),
	}
	s.registerGRPCService(grpcServer)

	_, ok := grpcServer.GetServiceInfo()[viewpb.ViewSyncService_ServiceDesc.ServiceName]
	require.True(t, ok)
	_, ok = grpcServer.GetServiceInfo()[viewpb.QueryPlanService_ServiceDesc.ServiceName]
	require.True(t, ok)
	_, ok = grpcServer.GetServiceInfo()[viewpb.ViewQueryService_ServiceDesc.ServiceName]
	require.True(t, ok)
}
