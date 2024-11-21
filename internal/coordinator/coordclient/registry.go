package coordclient

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	dcc "github.com/milvus-io/milvus/internal/distributed/datacoord/client"
	qcc "github.com/milvus-io/milvus/internal/distributed/querycoord/client"
	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// localClient is a client that can access local server directly
type localClient struct {
	queryCoordClient *queryCoordLocalClientImpl
	dataCoordClient  *dataCoordLocalClientImpl
	rootCoordClient  *rootCoordLocalClientImpl
}

var (
	enableLocal  *LocalClientRoleConfig // a global map to store all can be local accessible roles.
	glocalClient *localClient
)

func init() {
	enableLocal = &LocalClientRoleConfig{}
	glocalClient = &localClient{
		queryCoordClient: newQueryCoordLocalClient(),
		dataCoordClient:  newDataCoordLocalClient(),
		rootCoordClient:  newRootCoordLocalClient(),
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
	glocalClient.queryCoordClient.setReadyServer(server)
	log.Info("register query coord server", zap.Any("enableLocalClient", enableLocal))
}

// RegsterDataCoordServer register data coord server
func RegisterDataCoordServer(server datapb.DataCoordServer) {
	if !enableLocal.EnableDataCoord {
		return
	}
	glocalClient.dataCoordClient.setReadyServer(server)
	log.Info("register data coord server", zap.Any("enableLocalClient", enableLocal))
}

// RegisterRootCoordServer register root coord server
func RegisterRootCoordServer(server rootcoordpb.RootCoordServer) {
	if !enableLocal.EnableRootCoord {
		return
	}
	glocalClient.rootCoordClient.setReadyServer(server)
	log.Info("register root coord server", zap.Any("enableLocalClient", enableLocal))
}

// GetQueryCoordClient return query coord client
func GetQueryCoordClient(ctx context.Context) types.QueryCoordClient {
	if enableLocal.EnableQueryCoord {
		return glocalClient.queryCoordClient
	}
	// TODO: we should make a singleton here. but most unittest rely on a dedicated client.
	queryCoordClient, err := qcc.NewClient(ctx)
	if err != nil {
		panic(fmt.Sprintf("get query coord client failed: %v", err))
	}
	return queryCoordClient
}

// GetDataCoordClient return data coord client
func GetDataCoordClient(ctx context.Context) types.DataCoordClient {
	if enableLocal.EnableDataCoord {
		return glocalClient.dataCoordClient
	}
	// TODO: we should make a singleton here. but most unittest rely on a dedicated client.
	dataCoordClient, err := dcc.NewClient(ctx)
	if err != nil {
		panic(fmt.Sprintf("get data coord client failed: %v", err))
	}
	return dataCoordClient
}

// GetRootCoordClient return root coord client
func GetRootCoordClient(ctx context.Context) types.RootCoordClient {
	if enableLocal.EnableRootCoord {
		return glocalClient.rootCoordClient
	}
	// TODO: we should make a singleton here. but most unittest rely on a dedicated client.
	rootCoordClient, err := rcc.NewClient(ctx)
	if err != nil {
		panic(fmt.Sprintf("get root coord client failed: %v", err))
	}
	return rootCoordClient
}
