package nodeview

import (
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
)

type fakeQueryNodeClient struct{}

type fakeResourceGroupManager struct{}

// Source methods are patched with mockey; no alternative publisher implementation.
func (*fakeQueryNodeClient) RegisterQueryNodeListener(func(int64, *qnmanager.NodeInfo)) func() {
	panic("mock with mockey")
}

func (*fakeResourceGroupManager) RegisterResourceGroupListener(func(string, []int64)) func() {
	panic("mock with mockey")
}

func TestQueryNodePublisherReplayAndTransitions(t *testing.T) {
	var onNode func(int64, *qnmanager.NodeInfo)
	var onGroup func(string, []int64)
	detached := 0
	nodes := mockey.Mock((*fakeQueryNodeClient).RegisterQueryNodeListener).To(func(_ *fakeQueryNodeClient, fn func(int64, *qnmanager.NodeInfo)) func() {
		onNode = fn
		fn(1, &qnmanager.NodeInfo{ServerID: 1})
		return func() { detached++ }
	}).Build()
	defer nodes.UnPatch()
	groups := mockey.Mock((*fakeResourceGroupManager).RegisterResourceGroupListener).To(func(_ *fakeResourceGroupManager, fn func(string, []int64)) func() {
		onGroup = fn
		fn("rg-b", []int64{1})
		return func() { detached++ }
	}).Build()
	defer groups.UnPatch()
	p := NewQueryNodePublisher(&fakeQueryNodeClient{}, &fakeResourceGroupManager{})
	var values []*api.NodeInfo
	stop := p.RegisterNodeListener(func(id int64, node *api.NodeInfo) { require.Equal(t, int64(1), id); values = append(values, node) })
	require.Len(t, values, 1)
	require.Equal(t, "rg-b", values[0].ResourceGroup)
	onGroup("rg-b", []int64{1})
	require.Len(t, values, 1)
	onGroup("rg-a", []int64{1})
	require.Equal(t, "rg-a", values[len(values)-1].ResourceGroup)
	facts := &qnmanager.NodeInfo{ServerID: 1, ServerLabels: map[string]string{sessionutil.LabelResourceGroup: "rg-label"}}
	onNode(1, facts)
	require.Equal(t, "rg-label", values[len(values)-1].ResourceGroup)
	facts.ServerLabels[sessionutil.LabelResourceGroup] = "mutated"
	onGroup("rg-a", nil)
	require.Equal(t, "rg-label", values[len(values)-1].ResourceGroup)
	onNode(1, &qnmanager.NodeInfo{ServerID: 1, Stopping: true})
	require.True(t, values[len(values)-1].Stopping)
	require.Equal(t, "rg-b", values[len(values)-1].ResourceGroup)
	require.False(t, values[0].Stopping)
	onGroup("rg-b", nil)
	require.Nil(t, values[len(values)-1])
	onGroup("rg-c", []int64{1})
	require.NotNil(t, values[len(values)-1])
	onNode(1, nil)
	require.Nil(t, values[len(values)-1])
	count := len(values)
	stop()
	onNode(1, &qnmanager.NodeInfo{ServerID: 1})
	require.Len(t, values, count)
	p.Close()
	require.Equal(t, 2, detached)
}

func TestQueryNodePublisherConcurrentReplay(t *testing.T) {
	p := newQueryNodePublisher()
	var wg sync.WaitGroup
	for id := int64(1); id <= 50; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			p.onNode(id, &qnmanager.NodeInfo{ServerID: id, ServerLabels: map[string]string{sessionutil.LabelResourceGroup: "rg"}})
		}(id)
	}
	seen := make(map[int64]int)
	stop := p.RegisterNodeListener(func(id int64, _ *api.NodeInfo) { seen[id]++ })
	wg.Wait()
	stop()
	require.Len(t, seen, 50)
	for _, n := range seen {
		require.Equal(t, 1, n)
	}
}
