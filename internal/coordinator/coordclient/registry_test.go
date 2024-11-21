package coordclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestRegistry(t *testing.T) {
	assert.False(t, enableLocal.EnableQueryCoord)
	assert.False(t, enableLocal.EnableDataCoord)
	assert.False(t, enableLocal.EnableRootCoord)

	EnableLocalClientRole(&LocalClientRoleConfig{
		ServerType:       typeutil.RootCoordRole,
		EnableQueryCoord: true,
		EnableDataCoord:  true,
		EnableRootCoord:  true,
	})
	assert.False(t, enableLocal.EnableQueryCoord)
	assert.False(t, enableLocal.EnableDataCoord)
	assert.False(t, enableLocal.EnableRootCoord)

	RegisterRootCoordServer(&rootcoordpb.UnimplementedRootCoordServer{})
	RegisterDataCoordServer(&datapb.UnimplementedDataCoordServer{})
	RegisterQueryCoordServer(&querypb.UnimplementedQueryCoordServer{})
	assert.False(t, glocalClient.dataCoordClient.localDataCoordServer.Ready())
	assert.False(t, glocalClient.queryCoordClient.localQueryCoordServer.Ready())
	assert.False(t, glocalClient.rootCoordClient.localRootCoordServer.Ready())

	enableLocal = &LocalClientRoleConfig{}

	EnableLocalClientRole(&LocalClientRoleConfig{
		ServerType:       typeutil.StandaloneRole,
		EnableQueryCoord: true,
		EnableDataCoord:  true,
		EnableRootCoord:  true,
	})
	assert.True(t, enableLocal.EnableDataCoord)
	assert.True(t, enableLocal.EnableQueryCoord)
	assert.True(t, enableLocal.EnableRootCoord)

	RegisterRootCoordServer(&rootcoordpb.UnimplementedRootCoordServer{})
	RegisterDataCoordServer(&datapb.UnimplementedDataCoordServer{})
	RegisterQueryCoordServer(&querypb.UnimplementedQueryCoordServer{})
	assert.True(t, glocalClient.dataCoordClient.localDataCoordServer.Ready())
	assert.True(t, glocalClient.queryCoordClient.localQueryCoordServer.Ready())
	assert.True(t, glocalClient.rootCoordClient.localRootCoordServer.Ready())

	enableLocal = &LocalClientRoleConfig{}

	EnableLocalClientRole(&LocalClientRoleConfig{
		ServerType:       typeutil.MixtureRole,
		EnableQueryCoord: true,
		EnableDataCoord:  true,
		EnableRootCoord:  true,
	})
	assert.True(t, enableLocal.EnableDataCoord)
	assert.True(t, enableLocal.EnableQueryCoord)
	assert.True(t, enableLocal.EnableRootCoord)

	assert.NotNil(t, GetQueryCoordClient(context.Background()))
	assert.NotNil(t, GetDataCoordClient(context.Background()))
	assert.NotNil(t, GetRootCoordClient(context.Background()))
}
