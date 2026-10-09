package nodeview

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
)

func TestQueryNodeProviderSnapshotMergesResourceGroupBindings(t *testing.T) {
	ctx := context.Background()
	nodeClient := &fakeQueryNodeClient{
		nodes: map[int64]*qnmanager.NodeInfo{
			1: {ServerID: 1, Address: "addr-1"},
			2: {ServerID: 2, Address: "addr-2", Stopping: true},
			3: {ServerID: 3, Address: "addr-3"},
		},
	}
	rgManager := &fakeResourceGroupManager{
		rgs: []string{"rg-a", "rg-b"},
		nodes: map[string][]int64{
			"rg-a": {1, 2},
			"rg-b": {4},
		},
	}
	provider := NewQueryNodeProvider(ctx, nodeClient, rgManager)

	snapshot := provider.Snapshot()

	infos := collectNodeInfos(snapshot)
	require.Len(t, infos, 2)
	assert.Equal(t, "rg-a", infos[1].ResourceGroup)
	assert.True(t, infos[1].Alive)
	assert.False(t, infos[1].Stopping)
	assert.Equal(t, "rg-a", infos[2].ResourceGroup)
	assert.True(t, infos[2].Alive)
	assert.True(t, infos[2].Stopping)
	assert.NotContains(t, infos, int64(3))
	assert.NotContains(t, infos, int64(4))
}

func TestQueryNodeProviderSnapshotPrefersSessionResourceGroupLabel(t *testing.T) {
	ctx := context.Background()
	nodeClient := &fakeQueryNodeClient{
		nodes: map[int64]*qnmanager.NodeInfo{
			1: {
				ServerID:     1,
				Address:      "addr-1",
				ServerLabels: map[string]string{sessionutil.LabelResourceGroup: "rg-session"},
			},
		},
	}
	rgManager := &fakeResourceGroupManager{
		rgs:   []string{"rg-meta"},
		nodes: map[string][]int64{"rg-meta": {1}},
	}
	provider := NewQueryNodeProvider(ctx, nodeClient, rgManager)

	snapshot := provider.Snapshot()

	infos := collectNodeInfos(snapshot)
	require.Len(t, infos, 1)
	assert.Equal(t, "rg-session", infos[1].ResourceGroup)
}

func TestQueryNodeProviderSnapshotKeepsLastSnapshotOnDependencyError(t *testing.T) {
	ctx := context.Background()
	nodeClient := &fakeQueryNodeClient{
		nodes: map[int64]*qnmanager.NodeInfo{
			1: {ServerID: 1, Address: "addr-1"},
		},
	}
	rgManager := &fakeResourceGroupManager{
		rgs:   []string{"rg-a"},
		nodes: map[string][]int64{"rg-a": {1}},
	}
	provider := NewQueryNodeProvider(ctx, nodeClient, rgManager)

	first := provider.Snapshot()
	rgManager.err = errors.New("rg unavailable")

	second := provider.Snapshot()

	assert.Same(t, first, second)
	assert.Equal(t, uint64(1), second.Version())
	infos := collectNodeInfos(second)
	require.Len(t, infos, 1)
	assert.Equal(t, "rg-a", infos[1].ResourceGroup)
}

type fakeQueryNodeClient struct {
	nodes     map[int64]*qnmanager.NodeInfo
	err       error
	notifiers []func()
}

func (c *fakeQueryNodeClient) RegisterNodeChangedNotifier(notifier func()) {
	if notifier != nil {
		c.notifiers = append(c.notifiers, notifier)
	}
}

func (c *fakeQueryNodeClient) GetAllQueryNodes(ctx context.Context) (map[int64]*qnmanager.NodeInfo, error) {
	if c.err != nil {
		return nil, c.err
	}
	out := make(map[int64]*qnmanager.NodeInfo, len(c.nodes))
	for id, node := range c.nodes {
		cp := *node
		out[id] = &cp
	}
	return out, nil
}

type fakeResourceGroupManager struct {
	rgs   []string
	nodes map[string][]int64
	err   error
}

func (m *fakeResourceGroupManager) ListResourceGroups(ctx context.Context) []string {
	return append([]string(nil), m.rgs...)
}

func (m *fakeResourceGroupManager) GetNodes(ctx context.Context, rgName string) ([]int64, error) {
	if m.err != nil {
		return nil, m.err
	}
	return append([]int64(nil), m.nodes[rgName]...), nil
}

func collectNodeInfos(snapshot *balancer.NodeSnapshot) map[int64]*balancer.NodeInfo {
	infos := make(map[int64]*balancer.NodeInfo)
	snapshot.Range(func(id int64, info *balancer.NodeInfo) bool {
		infos[id] = info
		return true
	})
	return infos
}
