package coordclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestRegistry(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().CommonCfg.LocalRPCEnabled.Key, "true")

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
	assert.True(t, glocalClient.dataCoordClient.Ready())
	assert.True(t, glocalClient.queryCoordClient.Ready())
	assert.True(t, glocalClient.rootCoordClient.Ready())
	ResetRegistration()

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
	assert.True(t, glocalClient.dataCoordClient.Ready())
	assert.True(t, glocalClient.queryCoordClient.Ready())
	assert.True(t, glocalClient.rootCoordClient.Ready())

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
	GetQueryCoordClient(context.Background()).Close()
	GetDataCoordClient(context.Background()).Close()
	GetRootCoordClient(context.Background()).Close()
}
