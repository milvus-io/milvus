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
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
)

// memSpread measures how evenly memory is used across live QueryNodes once
// the cluster is quiet. Utilization is used/capacity, so nodes of different
// sizes compare fairly; rows and segment counts are reported alongside to
// show what QueryCoord itself balances.
func memSpread(sim *Sim) map[string]float64 {
	var util, rows, segs []float64
	perNode := map[int64]float64{}
	for id, v := range sim.Fleet.Snapshot() {
		if !v.Up || sim.Fleet.IsStopping(id) || id >= snBase {
			continue
		}
		capacity := float64(sim.Fleet.node(id).cfg.MemoryBytes)
		u := float64(v.UsedMemory) / capacity
		util = append(util, u)
		perNode[id] = u
		var r float64
		for _, n := range v.Segments {
			r += float64(n)
		}
		rows = append(rows, r)
		segs = append(segs, float64(len(v.Segments)))
	}
	m := map[string]float64{
		"mem_cv":             cv(util),
		"mem_max_over_mean":  maxOverMean(util),
		"mem_min_over_mean":  minOverMean(util),
		"row_cv":             cv(rows),
		"segment_cv":         cv(segs),
		"loads_no_binlog_sz": sim.FleetCounters()["loads_without_binlog_size"],
	}
	merge(m, "", tableSpread(sim))
	ids := make([]int64, 0, len(perNode))
	for id := range perNode {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		m[fmt.Sprintf("util_node_%02d", id)] = math.Round(perNode[id]*1000) / 1000
	}
	return m
}

func maxOverMean(xs []float64) float64 {
	var sum, mx float64
	for _, x := range xs {
		sum += x
		mx = max(mx, x)
	}
	if sum == 0 {
		return 0
	}
	return mx / (sum / float64(len(xs)))
}

func minOverMean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	mn := xs[0]
	for _, x := range xs {
		sum += x
		mn = min(mn, x)
	}
	if sum == 0 {
		return 0
	}
	return mn / (sum / float64(len(xs)))
}

func evenCluster(t *testing.T, nodes int, cfgs ...NodeConfig) *Sim {
	sim := New(t, Options{})
	for i := 0; i < nodes; i++ {
		cfg := CalibratedNodeConfig()
		cfg.LoadJitterSigma = 0
		if i < len(cfgs) {
			cfg = cfgs[i]
		}
		sim.AddNodes(qnBase+int64(i), 1, cfg)
	}
	return sim
}

func settleAndReport(t *testing.T, sim *Sim, name string, ids []int64, extra map[string]float64) {
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(ids...) }), "load did not finish")
	_, ok := waitSettled(sim, 3*time.Minute, 5*time.Second)
	m := memSpread(sim)
	m["settled"] = boolMetric(ok)
	merge(m, "", extra)
	sim.Report(Result{Scenario: name, Metrics: m})
}

// S16a One table, 2 vchannels. ChannelLevelScoreBalancer gives each channel
// an exclusive node group once nodes >= 3 x channels, splitting nodes by
// count: 7 nodes -> 4 + 3, so the 3-node group carries 33% more per node.
// Controls: 6 nodes (3 + 3) and 5 nodes (below the threshold, no groups).
func TestSimMaster_S16a_ChannelGroupSplit(t *testing.T) {
	requireSim(t)
	for _, nodes := range []int{5, 6, 7, 9} {
		t.Run(fmt.Sprintf("nodes%d", nodes), func(t *testing.T) {
			sim := evenCluster(t, nodes)
			sim.Broker.CreateCollection(1, 2, 60, 200_000)
			sim.loadExisting(1, 1)
			settleAndReport(t, sim, fmt.Sprintf("S16a_channel_groups_nodes%d", nodes), []int64{1}, nil)
		})
	}
}

// S16b Many tables, each with 2 vchannels on 7 nodes: does every table put
// its smaller channel group on the same nodes so the skew adds up?
func TestSimMaster_S16b_ChannelGroupsAcrossTables(t *testing.T) {
	requireSim(t)
	sim := evenCluster(t, 7)
	ids := collectionIDs(1, 10)
	for _, id := range ids {
		sim.Broker.CreateCollection(id, 2, 12, 200_000)
		sim.loadExisting(id, 1)
	}
	settleAndReport(t, sim, "S16b_channel_groups_10_tables", ids, nil)
}

// S16c One table whose two vchannels hold 70% / 30% of the data (hot
// partition key, time-skewed ingest) on 6 nodes (3 + 3 exclusive groups).
func TestSimMaster_S16c_UnevenChannelData(t *testing.T) {
	requireSim(t)
	sim := evenCluster(t, 6)
	sim.Broker.CreateCollectionChannels(1, []int{84, 36}, 200_000)
	sim.loadExisting(1, 1)
	settleAndReport(t, sim, "S16c_uneven_channel_data_70_30", []int64{1}, nil)
}

// S16d Bytes per row differ between segments of one table (index built or
// not, text length, nullable fields): QueryCoord balances rows, memory
// follows bytes. 1 vchannel so channel groups do not interfere.
func TestSimMaster_S16d_BytesPerRowVariance(t *testing.T) {
	requireSim(t)
	for _, sigma := range []float64{0, 0.3, 0.6} {
		t.Run(fmt.Sprintf("sigma%.1f", sigma), func(t *testing.T) {
			sim := evenCluster(t, 4)
			sim.Broker.MemSigma = sigma
			sim.Broker.CreateCollection(1, 1, 80, 200_000)
			sim.Broker.MemSigma = 0
			sim.loadExisting(1, 1)
			settleAndReport(t, sim, fmt.Sprintf("S16d_bytes_per_row_sigma%.1f", sigma), []int64{1}, nil)
		})
	}
}

// S16e Few large segments: 10 segments on 4 nodes cannot split evenly
// (3/3/2/2), and the 5% tolerance keeps it there.
func TestSimMaster_S16e_FewLargeSegments(t *testing.T) {
	requireSim(t)
	for _, segs := range []int{10, 40} {
		t.Run(fmt.Sprintf("segs%d", segs), func(t *testing.T) {
			sim := evenCluster(t, 4)
			sim.Broker.CreateCollection(1, 1, segs, 4_000_000/int64(segs)*10)
			sim.loadExisting(1, 1)
			settleAndReport(t, sim, fmt.Sprintf("S16e_segments%d_on_4_nodes", segs), []int64{1}, nil)
		})
	}
}

// S16f Mixed node sizes (two 32GB, two 64GB): QueryCoord weights quotas by
// memory capacity; utilization should come out even.
func TestSimMaster_S16f_MixedNodeSizes(t *testing.T) {
	requireSim(t)
	small := CalibratedNodeConfig()
	small.LoadJitterSigma = 0
	small.MemoryBytes = 32 << 30
	big := small
	big.MemoryBytes = 64 << 30
	sim := evenCluster(t, 4, small, small, big, big)
	ids := collectionIDs(1, 6)
	for _, id := range ids {
		sim.Broker.CreateCollection(id, 1, 30, 200_000)
		sim.loadExisting(id, 1)
	}
	settleAndReport(t, sim, "S16f_mixed_node_sizes", ids, nil)
}

// S16g Many small tables, each with fewer segments than nodes: each table is
// rounded independently; does the global row term even out total memory?
func TestSimMaster_S16g_ManySmallTables(t *testing.T) {
	requireSim(t)
	sim := evenCluster(t, 8)
	ids := collectionIDs(1, 40)
	for _, id := range ids {
		sim.Broker.CreateCollection(id, 1, 3, 300_000)
		sim.loadExisting(id, 1)
	}
	settleAndReport(t, sim, "S16g_40_tables_3_segments_8_nodes", ids, nil)
}

// tableSpread measures how evenly each table is spread over the nodes of
// each of its replicas, independent of any balancer's own definition: a
// node's fair share is the replica's rows split by node capacity.
//
//   - table_max_over_fair_*: the fullest node's rows over its fair share,
//     over tables whose replica has at least two segments per node (where an
//     even spread is possible); 1 is perfect.
//   - tables_unbalanced_frac: tables the balancer would still act on: a node
//     with a segment count outside the share's bounds (TableSegmentCountTolerance of the
//     share, at least one segment), or off its share in rows by more than
//     max(10% of the share, the table's largest segment).
//   - table_max_count_over_fair_*: the fullest node's segment count over its
//     share, over the same tables as table_max_over_fair_*.
//   - tables_on_1_node: tables with two or more segments and nodes, all on one.
func tableSpread(sim *Sim) map[string]float64 {
	qc := sim.current()
	if qc == nil {
		return nil
	}
	views := sim.Fleet.Snapshot()
	type cell struct {
		rows, maxSeg float64
		segs         int
	}
	// (collection, node) -> rows
	on := map[[2]int64]*cell{}
	for node, v := range views {
		if !v.Up || node >= snBase {
			continue
		}
		for seg, rows := range v.Segments {
			k := [2]int64{v.Collection[seg], node}
			c := on[k]
			if c == nil {
				c = &cell{}
				on[k] = c
			}
			c.rows += float64(rows)
			c.maxSeg = max(c.maxSeg, float64(rows))
			c.segs++
		}
	}
	var ratios, countRatios []float64
	tables, unbalanced, onOne := 0, 0, 0
	worstSeen, worstDesc := 0.0, ""
	for _, coll := range qc.meta.CollectionManager.GetAll(qc.ctx) {
		for _, r := range qc.meta.ReplicaManager.GetByCollection(qc.ctx, coll) {
			var nodes []int64
			r.RangeOverRWNodes(func(n int64) bool {
				if v, ok := views[n]; ok && v.Up && !sim.Fleet.IsStopping(n) {
					nodes = append(nodes, n)
				}
				return true
			})
			if len(nodes) < 2 {
				continue
			}
			var total, capSum, maxSeg float64
			segs, used := 0, 0
			for _, n := range nodes {
				capSum += float64(sim.Fleet.node(n).cfg.MemoryBytes)
				if c := on[[2]int64{coll, n}]; c != nil {
					total += c.rows
					maxSeg = max(maxSeg, c.maxSeg)
					segs += c.segs
					used++
				}
			}
			if segs == 0 {
				continue
			}
			tables++
			if segs >= 2 && used == 1 {
				onOne++
			}
			worst, off := 0.0, false
			maxCount := 0
			for _, n := range nodes {
				cnt := 0
				if c := on[[2]int64{coll, n}]; c != nil {
					cnt = c.segs
				}
				maxCount = max(maxCount, cnt)
				fairCount := float64(segs) * float64(sim.Fleet.node(n).cfg.MemoryBytes) / capSum
				slack := max(1, int(math.Ceil(assign.GetTwoLevelConfig().TableSegmentCountTolerance*fairCount)))
				if cnt < max(0, int(math.Floor(fairCount))-slack) || cnt > int(math.Ceil(fairCount))+slack {
					off = true
				}
			}
			if segs >= 2*len(nodes) {
				countRatios = append(countRatios, float64(maxCount)/(float64(segs)/float64(len(nodes))))
			}
			for _, n := range nodes {
				fair := total * float64(sim.Fleet.node(n).cfg.MemoryBytes) / capSum
				var rows float64
				if c := on[[2]int64{coll, n}]; c != nil {
					rows = c.rows
				}
				if fair > 0 {
					worst = max(worst, rows/fair)
				}
				if math.Abs(rows-fair) > max(0.1*fair, maxSeg)+1 {
					off = true
				}
			}
			if off {
				unbalanced++
			}
			if segs >= 2*len(nodes) {
				ratios = append(ratios, worst)
				if worst > worstSeen {
					worstSeen = worst
					var parts []string
					for _, n := range nodes {
						c := on[[2]int64{coll, n}]
						rows, cnt := 0.0, 0
						if c != nil {
							rows, cnt = c.rows, c.segs
						}
						parts = append(parts, fmt.Sprintf("n%d:%d/%.0fk", n, cnt, rows/1000))
					}
					var sizes []string
					for node, v := range views {
						for seg, rows := range v.Segments {
							if v.Collection[seg] == coll {
								sizes = append(sizes, fmt.Sprintf("%.0fk@n%d", float64(rows)/1000, node))
							}
						}
					}
					sort.Strings(sizes)
					worstDesc = fmt.Sprintf("coll %d replica %d: segs=%d rows=%.0fk fair=%.0fk/node tol=max(10%%,maxSeg=%.0fk) per-node[segs/rows]=%v sizes=%v",
						coll, r.GetID(), segs, total/1000, total/float64(len(nodes))/1000, maxSeg/1000, parts, sizes)
				}
			}
		}
	}
	if worstDesc != "" {
		sim.t.Logf("tableSpread worst table: %s", worstDesc)
	}
	m := map[string]float64{
		"tables_measured":  float64(tables),
		"tables_on_1_node": float64(onOne),
	}
	if tables > 0 {
		m["tables_unbalanced_frac"] = float64(unbalanced) / float64(tables)
	}
	if len(countRatios) > 0 {
		sort.Float64s(countRatios)
		m["table_max_count_over_fair_worst"] = countRatios[len(countRatios)-1]
		m["table_max_count_over_fair_p90"] = countRatios[int(0.9*float64(len(countRatios)-1))]
	}
	if len(ratios) > 0 {
		sort.Float64s(ratios)
		var sum float64
		for _, x := range ratios {
			sum += x
		}
		m["table_max_over_fair_worst"] = ratios[len(ratios)-1]
		m["table_max_over_fair_p90"] = ratios[int(0.9*float64(len(ratios)-1))]
		m["table_max_over_fair_mean"] = sum / float64(len(ratios))
	}
	return m
}
