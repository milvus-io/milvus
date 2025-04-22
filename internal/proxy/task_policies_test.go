package proxy

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/types"
)

func TestRoundRobinPolicy(t *testing.T) {
	var err error

	ctx := context.TODO()

	mgr := newShardClientMgr()

	shard2leaders := map[string][]nodeInfo{
		"c0": {{nodeID: 0, address: "fake"}, {nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}},
		"c1": {{nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c2": {{nodeID: 0, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c3": {{nodeID: 1, address: "fake"}, {nodeID: 3, address: "fake"}, {nodeID: 4, address: "fake"}},
	}

	querier := &mockQuery{}
	querier.init()

	err = RoundRobinPolicy(ctx, mgr, querier.query, shard2leaders)
	assert.NoError(t, err)
	assert.Equal(t, querier.records(), map[UniqueID][]string{0: {"c0", "c2"}, 1: {"c1", "c3"}})

	mockerr := errors.New("mock query node error")
	querier.init()
	querier.failset[0] = mockerr

	err = RoundRobinPolicy(ctx, mgr, querier.query, shard2leaders)
	assert.NoError(t, err)
	assert.Equal(t, querier.records(), map[int64][]string{1: {"c0", "c1", "c3"}, 2: {"c2"}})

	querier.init()
	querier.failset[0] = mockerr
	querier.failset[2] = mockerr
	querier.failset[3] = mockerr
	err = RoundRobinPolicy(ctx, mgr, querier.query, shard2leaders)
	assert.True(t, strings.Contains(err.Error(), mockerr.Error()))
}

type mockQuery struct {
	mu       sync.Mutex
	queryset map[UniqueID][]string
	failset  map[UniqueID]error
}

func (m *mockQuery) query(_ context.Context, nodeID UniqueID, qn types.QueryNodeClient, chs ...string) error {
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
