// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package simulation

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// WALBalancer is a simulated StreamingCoord balancer: it owns the pchannel ->
// StreamingNode assignment that QueryCoord follows when placing delegators.
type WALBalancer struct {
	balancer.Balancer // unimplemented methods panic: QueryCoord must not call them

	mu        sync.Mutex
	pchannels []string
	assign    map[string]int64
	term      map[string]int64
	nodes     map[int64]string // live streaming node -> resource group
	version   int64
	changed   chan struct{}
	moves     int
}

func newWALBalancer(pchannels int) *WALBalancer {
	b := &WALBalancer{assign: map[string]int64{}, term: map[string]int64{}, nodes: map[int64]string{}, changed: make(chan struct{})}
	for i := 0; i < pchannels; i++ {
		b.pchannels = append(b.pchannels, PChannelName(i))
	}
	return b
}

func (b *WALBalancer) notifyLocked() {
	b.version++
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *WALBalancer) paramLocked() balancer.WatchChannelAssignmentsCallbackParam {
	relations := make([]types.PChannelInfoAssigned, 0, len(b.assign))
	for _, p := range b.pchannels {
		node, ok := b.assign[p]
		if !ok {
			continue
		}
		relations = append(relations, types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{Name: p, Term: b.term[p], AccessMode: types.AccessModeRW},
			Node:    types.StreamingNodeInfo{ServerID: node, Address: fmt.Sprintf("sim-sn-%d", node)},
		})
	}
	return balancer.WatchChannelAssignmentsCallbackParam{
		Version:            typeutil.VersionInt64Pair{Global: 1, Local: b.version},
		CChannelAssignment: &streamingpb.CChannelAssignment{Meta: &streamingpb.CChannelMeta{Pchannel: b.pchannels[0]}},
		Relations:          relations,
	}
}

// AddNode registers a streaming node; when spread is set, pchannels are
// rebalanced evenly (StreamingCoord moves WALs onto new nodes).
func (b *WALBalancer) AddNode(id int64, rg string, spread bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes[id] = rg
	if spread {
		b.spreadLocked()
	} else {
		b.assignOrphansLocked()
	}
	b.notifyLocked()
}

// RemoveNode drops a streaming node and reassigns its WALs to the others.
func (b *WALBalancer) RemoveNode(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.nodes, id)
	for p, node := range b.assign {
		if node == id {
			delete(b.assign, p)
		}
	}
	b.assignOrphansLocked()
	b.notifyLocked()
}

// Move relocates one WAL.
func (b *WALBalancer) Move(pchannel string, to int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.assign[pchannel] == to {
		return
	}
	b.assign[pchannel] = to
	b.term[pchannel]++
	b.moves++
	b.notifyLocked()
}

func (b *WALBalancer) sortedNodesLocked() []int64 {
	ids := make([]int64, 0, len(b.nodes))
	for id := range b.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (b *WALBalancer) countsLocked() map[int64]int {
	counts := make(map[int64]int, len(b.nodes))
	for id := range b.nodes {
		counts[id] = 0
	}
	for _, node := range b.assign {
		counts[node]++
	}
	return counts
}

func (b *WALBalancer) leastLoadedLocked() int64 {
	counts := b.countsLocked()
	best, bestCount := int64(-1), 0
	for _, id := range b.sortedNodesLocked() {
		if best < 0 || counts[id] < bestCount {
			best, bestCount = id, counts[id]
		}
	}
	return best
}

func (b *WALBalancer) assignOrphansLocked() {
	if len(b.nodes) == 0 {
		return
	}
	for _, p := range b.pchannels {
		if _, ok := b.assign[p]; !ok {
			b.assign[p] = b.leastLoadedLocked()
			b.term[p]++
		}
	}
}

// spreadLocked moves WALs from the most to the least loaded node until the
// counts differ by at most one.
func (b *WALBalancer) spreadLocked() {
	b.assignOrphansLocked()
	for {
		counts := b.countsLocked()
		var hi, lo int64 = -1, -1
		for _, id := range b.sortedNodesLocked() {
			if hi < 0 || counts[id] > counts[hi] {
				hi = id
			}
			if lo < 0 || counts[id] < counts[lo] {
				lo = id
			}
		}
		if hi < 0 || counts[hi]-counts[lo] <= 1 {
			return
		}
		for _, p := range b.pchannels {
			if b.assign[p] == hi {
				b.assign[p] = lo
				b.term[p]++
				b.moves++
				break
			}
		}
	}
}

// Located returns the node that owns a pchannel's WAL.
func (b *WALBalancer) Located(pchannel string) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	node, ok := b.assign[pchannel]
	return node, ok
}

// Assignment returns a copy of the pchannel -> node map.
func (b *WALBalancer) Assignment() map[string]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]int64, len(b.assign))
	for k, v := range b.assign {
		out[k] = v
	}
	return out
}

func (b *WALBalancer) GetLatestChannelAssignment() (*balancer.WatchChannelAssignmentsCallbackParam, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.paramLocked()
	return &p, nil
}

func (b *WALBalancer) nodeInfos() map[int64]*types.StreamingNodeInfoWithResourceGroup {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[int64]*types.StreamingNodeInfoWithResourceGroup, len(b.nodes))
	for id, rg := range b.nodes {
		out[id] = &types.StreamingNodeInfoWithResourceGroup{
			StreamingNodeInfo: types.StreamingNodeInfo{ServerID: id, Address: fmt.Sprintf("sim-sn-%d", id)},
			ResourceGroup:     rg,
		}
	}
	return out
}

func (b *WALBalancer) GetAllStreamingNodes(context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	return b.nodeInfos(), nil
}

func (b *WALBalancer) GetAvailableStreamingNodes(context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	return b.nodeInfos(), nil
}

func (b *WALBalancer) GetLatestWALLocated(_ context.Context, pchannel string) (int64, bool) {
	return b.Located(pchannel)
}

func (b *WALBalancer) WatchChannelAssignments(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
	for {
		b.mu.Lock()
		param := b.paramLocked()
		changed := b.changed
		b.mu.Unlock()
		if err := cb(param); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

func (b *WALBalancer) Trigger(context.Context) error { return nil }

func (b *WALBalancer) Close() {}

// StreamingOptions enables streaming mode: delegators live on streaming
// nodes (embedded QueryNodes) that host the WALs.
type StreamingOptions struct {
	PChannels int
	// WALReassignDelay models StreamingCoord's failover time.
	WALReassignDelay time.Duration
}

// AddStreamingNodes starts streaming nodes (an embedded QueryNode plus a WAL
// host each). spreadWAL rebalances existing WALs onto them.
func (s *Sim) AddStreamingNodes(firstID int64, count int, cfg NodeConfig, spreadWAL bool) []int64 {
	if s.wal == nil {
		s.t.Fatalf("streaming mode is not enabled")
	}
	ids := make([]int64, 0, count)
	for i := 0; i < count; i++ {
		id := firstID + int64(i)
		s.Fleet.AddNode(id, cfg).streaming = true
		s.registerNode(s.current(), id)
		s.wal.AddNode(id, meta.DefaultResourceGroupName, spreadWAL && i == count-1)
		ids = append(ids, id)
	}
	return ids
}

// KillStreamingNode crashes a streaming node: its embedded QueryNode loses
// all state and StreamingCoord reassigns its WALs after WALReassignDelay.
func (s *Sim) KillStreamingNode(id int64) {
	s.KillNode(id)
	reassign := func() { s.wal.RemoveNode(id) }
	if s.opts.Streaming.WALReassignDelay > 0 {
		time.AfterFunc(s.opts.Streaming.WALReassignDelay, reassign)
	} else {
		reassign()
	}
}

// WAL exposes the simulated StreamingCoord balancer.
func (s *Sim) WAL() *WALBalancer { return s.wal }
