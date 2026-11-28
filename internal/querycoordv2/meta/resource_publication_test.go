package meta

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestResourceGroupPublicationFollowsCommittedMembership(t *testing.T) {
	paramtable.Init()
	save := mockey.Mock(querycoord.Catalog.SaveResourceGroup).Return(nil).Build()
	defer save.UnPatch()
	nodes := session.NewNodeManager()
	nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 10}))
	manager := NewResourceManager(querycoord.NewCatalog(nil), nodes)
	var seen [][]int64
	stop := manager.RegisterResourceGroupListener(func(name string, ids []int64) {
		if name == DefaultResourceGroupName {
			seen = append(seen, append([]int64(nil), ids...))
		}
	})
	require.Len(t, seen, 1)
	require.Empty(t, seen[0])
	manager.HandleNodeUp(t.Context(), 10)
	require.Len(t, seen, 2)
	require.Equal(t, []int64{10}, seen[1])
	save.UnPatch()
	failure := mockey.Mock(querycoord.Catalog.SaveResourceGroup).Return(merr.WrapErrServiceUnavailableMsg("persistence failed")).Build()
	defer failure.UnPatch()
	require.ErrorIs(t, manager.HandleNodeDown(t.Context(), 10), merr.ErrServiceUnavailable)
	require.Len(t, seen, 2, "failed persistence must not change published membership")
	failure.UnPatch()
	save = mockey.Mock(querycoord.Catalog.SaveResourceGroup).Return(nil).Build()
	defer save.UnPatch()
	manager.HandleNodeDown(t.Context(), 10)
	require.Len(t, seen, 3)
	require.Empty(t, seen[2])
	require.Equal(t, []int64{10}, seen[1])
	stop()
	manager.HandleNodeUp(t.Context(), 10)
	require.Len(t, seen, 3)
}

func TestReconcileNodeRemovalAfterLostReply(t *testing.T) {
	paramtable.Init()
	nodes := session.NewNodeManager()
	manager := NewResourceManager(querycoord.NewCatalog(nil), nodes)
	var persisted []int64
	var loseReply bool
	save := mockey.Mock(querycoord.Catalog.SaveResourceGroup).To(func(_ querycoord.Catalog, _ context.Context, groups ...*querypb.ResourceGroup) error {
		persisted = groups[0].GetNodes()
		if loseReply {
			return merr.WrapErrIoFailedErr(context.DeadlineExceeded, "save resource group")
		}
		return nil
	}).Build()
	defer save.UnPatch()

	ctx := t.Context()
	node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 10})
	nodes.Add(node)
	manager.HandleNodeUp(ctx, 10)
	require.Equal(t, []int64{10}, persisted)

	nodes.Remove(10)
	loseReply = true
	require.ErrorIs(t, manager.HandleNodeDown(ctx, 10), context.DeadlineExceeded)
	require.Empty(t, persisted, "the removal committed before its response was lost")
	require.Equal(t, DefaultResourceGroupName, manager.GetResourceGroupByNodeID(10))

	nodes.Add(node)
	manager.HandleNodeUp(ctx, 10)
	loseReply = false
	require.NoError(t, manager.ReconcileNodeRemovals(ctx, DefaultResourceGroupName))
	require.Equal(t, []int64{10}, persisted)
	require.Equal(t, DefaultResourceGroupName, manager.GetResourceGroupByNodeID(10))
}
