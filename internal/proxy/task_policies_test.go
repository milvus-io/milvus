package proxy

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/stretchr/testify/assert"

	"go.uber.org/zap"
)

func TestUpdateShardsWithRoundRobin(t *testing.T) {
	list := map[string][]nodeInfo{
		"channel-1": {
			{1, "addr1"},
			{2, "addr2"},
		},
		"channel-2": {
			{20, "addr20"},
			{21, "addr21"},
		},
	}

	updateShardsWithRoundRobin(list)

	assert.Equal(t, int64(2), list["channel-1"][0].nodeID)
	assert.Equal(t, "addr2", list["channel-1"][0].address)
	assert.Equal(t, int64(21), list["channel-2"][0].nodeID)
	assert.Equal(t, "addr21", list["channel-2"][0].address)

	t.Run("check print", func(t *testing.T) {
		qns := []nodeInfo{
			{1, "addr1"},
			{2, "addr2"},
			{20, "addr20"},
			{21, "addr21"},
		}

		res := fmt.Sprintf("list: %v", qns)

		log.Debug("Check String func",
			zap.Any("Any", qns),
			zap.Any("ok", qns[0]),
			zap.String("ok2", res),
		)

	})
}

func TestGroupShardLeadersWithSameQueryNode(t *testing.T) {
	var err error

	Params.Init()
	var (
		ctx = context.TODO()
	)

	mgr := newShardClientMgr()

	shard2leaders := map[string][]nodeInfo{
		"c0": {{nodeID: 0, address: "fake"}, {nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}},
		"c1": {{nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c2": {{nodeID: 0, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c3": {{nodeID: 1, address: "fake"}, {nodeID: 3, address: "fake"}, {nodeID: 4, address: "fake"}},
	}
	mgr.UpdateShardLeaders(nil, shard2leaders)
	nexts := map[string]int{
		"c0": 0,
		"c1": 0,
		"c2": 0,
		"c3": 0,
	}
	errSet := map[string]error{}
	node2dmls, qnSet, err := groupShardleadersWithSameQueryNode(ctx, shard2leaders, nexts, errSet, mgr)
	assert.Nil(t, err)
	for nodeID := range node2dmls {
		sort.Slice(node2dmls[nodeID], func(i, j int) bool { return node2dmls[nodeID][i] < node2dmls[nodeID][j] })
	}

	cli0, err := mgr.GetClient(ctx, 0)
	assert.Nil(t, err)
	cli1, err := mgr.GetClient(ctx, 1)
	assert.Nil(t, err)
	cli2, err := mgr.GetClient(ctx, 2)
	assert.Nil(t, err)
	cli3, err := mgr.GetClient(ctx, 3)
	assert.Nil(t, err)

	assert.Equal(t, node2dmls, map[int64][]string{0: {"c0", "c2"}, 1: {"c1", "c3"}})
	assert.Equal(t, qnSet, map[int64]types.QueryNode{0: cli0, 1: cli1})
	assert.Equal(t, nexts, map[string]int{"c0": 1, "c1": 1, "c2": 1, "c3": 1})
	// delete client1 in client mgr
	delete(mgr.clients.data, 1)
	node2dmls, qnSet, err = groupShardleadersWithSameQueryNode(ctx, shard2leaders, nexts, errSet, mgr)
	assert.Nil(t, err)
	for nodeID := range node2dmls {
		sort.Slice(node2dmls[nodeID], func(i, j int) bool { return node2dmls[nodeID][i] < node2dmls[nodeID][j] })
	}
	assert.Equal(t, node2dmls, map[int64][]string{2: {"c1", "c2"}, 3: {"c3"}})
	assert.Equal(t, qnSet, map[int64]types.QueryNode{2: cli2, 3: cli3})
	assert.Equal(t, nexts, map[string]int{"c0": 2, "c1": 2, "c2": 2, "c3": 2})
	assert.NotNil(t, errSet["c0"])

	nexts["c0"] = 3
	_, _, err = groupShardleadersWithSameQueryNode(ctx, shard2leaders, nexts, errSet, mgr)
	assert.Equal(t, err, errSet["c0"])

	nexts["c0"] = 2
	nexts["c1"] = 3
	_, _, err = groupShardleadersWithSameQueryNode(ctx, shard2leaders, nexts, errSet, mgr)
	assert.Equal(t, err, fmt.Errorf("no available shard leader"))
}

func TestMergeRoundRobinPolicy(t *testing.T) {
	var err error

	Params.Init()
	var (
		ctx = context.TODO()
	)

	mgr := newShardClientMgr()

	shard2leaders := map[string][]nodeInfo{
		"c0": {{nodeID: 0, address: "fake"}, {nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}},
		"c1": {{nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c2": {{nodeID: 0, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c3": {{nodeID: 1, address: "fake"}, {nodeID: 3, address: "fake"}, {nodeID: 4, address: "fake"}},
	}
	mgr.UpdateShardLeaders(nil, shard2leaders)

	querier := &mockQuery{}
	querier.init()

	err = mergeRoundRobinPolicy(ctx, mgr, querier.query, shard2leaders)
	assert.Nil(t, err)
	assert.Equal(t, querier.records(), map[UniqueID][]string{0: {"c0", "c2"}, 1: {"c1", "c3"}})

	mockerr := fmt.Errorf("mock query node error")
	querier.init()
	querier.failset[0] = mockerr

	err = mergeRoundRobinPolicy(ctx, mgr, querier.query, shard2leaders)
	assert.Nil(t, err)
	assert.Equal(t, querier.records(), map[int64][]string{1: {"c0", "c1", "c3"}, 2: {"c2"}})

	querier.init()
	querier.failset[0] = mockerr
	querier.failset[2] = mockerr
	querier.failset[3] = mockerr
	err = mergeRoundRobinPolicy(ctx, mgr, querier.query, shard2leaders)
	assert.Equal(t, err, mockerr)
}

func mockQueryNodeCreator(ctx context.Context, address string) (types.QueryNode, error) {
	return &QueryNodeMock{address: address}, nil
}

type mockQuery struct {
	mu       sync.Mutex
	queryset map[UniqueID][]string
	failset  map[UniqueID]error
}

func (m *mockQuery) query(_ context.Context, nodeID UniqueID, qn types.QueryNode, chs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.failset[nodeID]; ok {
		return err
	}
	m.queryset[nodeID] = append(m.queryset[nodeID], chs...)
	return nil
}

func (m *mockQuery) init() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queryset = make(map[int64][]string)
	m.failset = make(map[int64]error)
}

func (m *mockQuery) records() map[UniqueID][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	for nodeID := range m.queryset {
		sort.Slice(m.queryset[nodeID], func(i, j int) bool {
			return m.queryset[nodeID][i] < m.queryset[nodeID][j]
		})
	}
	return m.queryset
}
