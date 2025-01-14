package coordclient

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// localClient is a client that can access local server directly
type localClient struct {
	queryCoordClient *syncutil.Future[types.QueryCoordClient]
	dataCoordClient  *syncutil.Future[types.DataCoordClient]
	rootCoordClient  *syncutil.Future[types.RootCoordClient]
}

var (
	enableLocal  *LocalClientRoleConfig // a global map to store all can be local accessible roles.
	glocalClient *localClient           // !!! WARNING: local client will ignore all interceptor of grpc client and server.
)

func init() {
	enableLocal = &LocalClientRoleConfig{}
	glocalClient = &localClient{
		queryCoordClient: syncutil.NewFuture[types.QueryCoordClient](),
		dataCoordClient:  syncutil.NewFuture[types.DataCoordClient](),
		rootCoordClient:  syncutil.NewFuture[types.RootCoordClient](),
	}
}

type LocalClientRoleConfig struct {
	ServerType       string
	EnableQueryCoord bool
	EnableDataCoord  bool
	EnableRootCoord  bool
}

// EnableLocalClientRole init localable roles
func EnableLocalClientRole(cfg *LocalClientRoleConfig) {
	if cfg.ServerType != typeutil.StandaloneRole && cfg.ServerType != typeutil.MixtureRole {
		return
	}
	enableLocal = cfg
}

// RegisterQueryCoordServer register query coord server
func RegisterQueryCoordServer(server querypb.QueryCoordServer) {
	if !enableLocal.EnableQueryCoord {
		return
	}
	newLocalClient := grpcclient.NewLocalGRPCClient(&querypb.QueryCoord_ServiceDesc, server, querypb.NewQueryCoordClient)
	glocalClient.queryCoordClient.Set(&nopCloseQueryCoordClient{newLocalClient})
	log.Ctx(context.TODO()).Info("register query coord server", zap.Any("enableLocalClient", enableLocal))
}

// RegsterDataCoordServer register data coord server
func RegisterDataCoordServer(server datapb.DataCoordServer) {
	if !enableLocal.EnableDataCoord {
		return
	}
	newLocalClient := grpcclient.NewLocalGRPCClient(&datapb.DataCoord_ServiceDesc, server, datapb.NewDataCoordClient)
	glocalClient.dataCoordClient.Set(&nopCloseDataCoordClient{newLocalClient})
	log.Ctx(context.TODO()).Info("register data coord server", zap.Any("enableLocalClient", enableLocal))
}

// RegisterRootCoordServer register root coord server
func RegisterRootCoordServer(server rootcoordpb.RootCoordServer) {
	if !enableLocal.EnableRootCoord {
		return
	}
	newLocalClient := grpcclient.NewLocalGRPCClient(&rootcoordpb.RootCoord_ServiceDesc, server, rootcoordpb.NewRootCoordClient)
	glocalClient.rootCoordClient.Set(&nopCloseRootCoordClient{newLocalClient})
	log.Ctx(context.TODO()).Info("register root coord server", zap.Any("enableLocalClient", enableLocal))
}

// GetQueryCoordClient return query coord client
func GetQueryCoordClient(ctx context.Context) types.QueryCoordClient {
	var client types.QueryCoordClient
	var err error
	if enableLocal.EnableQueryCoord && paramtable.Get().CommonCfg.LocalRPCEnabled.GetAsBool() {
		client, err = glocalClient.queryCoordClient.GetWithContext(ctx)
	} else {
		// TODO: we should make a singleton here. but most unittest rely on a dedicated client.
		client, err = qcc.NewClient(ctx)
	}
	if err != nil {
		panic(fmt.Sprintf("get query coord client failed: %v", err))
	}
	return client
}

// GetDataCoordClient return data coord client
func GetDataCoordClient(ctx context.Context) types.DataCoordClient {
	var client types.DataCoordClient
	var err error
	if enableLocal.EnableDataCoord && paramtable.Get().CommonCfg.LocalRPCEnabled.GetAsBool() {
		client, err = glocalClient.dataCoordClient.GetWithContext(ctx)
	} else {
		// TODO: we should make a singleton here. but most unittest rely on a dedicated client.
		client, err = dcc.NewClient(ctx)
	}
	if err != nil {
		panic(fmt.Sprintf("get data coord client failed: %v", err))
	}
	return client
}

// GetRootCoordClient return root coord client
func GetRootCoordClient(ctx context.Context) types.RootCoordClient {
	var client types.RootCoordClient
	var err error
	if enableLocal.EnableRootCoord && paramtable.Get().CommonCfg.LocalRPCEnabled.GetAsBool() {
		client, err = glocalClient.rootCoordClient.GetWithContext(ctx)
	} else {
		// TODO: we should make a singleton here. but most unittest rely on a dedicated client.
		client, err = rcc.NewClient(ctx)
	}
	if err != nil {
		panic(fmt.Sprintf("get root coord client failed: %v", err))
	}
	return client
}

// MustGetLocalRootCoordClientFuture return root coord client future,
// panic if root coord client is not enabled
func MustGetLocalRootCoordClientFuture() *syncutil.Future[types.RootCoordClient] {
	return glocalClient.rootCoordClient
}

type nopCloseQueryCoordClient struct {
	querypb.QueryCoordClient
}

func (n *nopCloseQueryCoordClient) Close() error {
	return nil
}

type nopCloseDataCoordClient struct {
	datapb.DataCoordClient
}

func (n *nopCloseDataCoordClient) Close() error {
	return nil
}

type nopCloseRootCoordClient struct {
	rootcoordpb.RootCoordClient
}

func (n *nopCloseRootCoordClient) Close() error {
	return nil
}
