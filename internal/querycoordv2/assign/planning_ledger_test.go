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

package assign

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPlanningLedgerLazySnapshotAndTaskContextRelease(t *testing.T) {
	var builds atomic.Int64
	ctx := WithPlanningLedgerFactory(context.Background(), func() *PlanningLedger {
		builds.Add(1)
		return NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(10, 1000)}, nil, nil, nil, 8)
	})
	if builds.Load() != 0 {
		t.Fatal("an idle checker eagerly scanned distribution")
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if PlanningLedgerFromContext(child).Rows(1, -1) != 10 {
				t.Error("snapshot was not shared")
			}
		}()
	}
	workers.Wait()
	if builds.Load() != 1 {
		t.Fatal("multiple sources constructed independent snapshots")
	}
	ReleasePlanningLedger(ctx)
	if PlanningLedgerFromContext(child) != nil || builds.Load() != 1 {
		t.Fatal("a retained task context retained or rebuilt an expired snapshot")
	}
	idle := WithPlanningLedgerFactory(context.Background(), func() *PlanningLedger { t.Fatal("released idle checker created a snapshot"); return nil })
	ReleasePlanningLedger(idle)
	if PlanningLedgerFromContext(idle) != nil {
		t.Fatal("released idle context returned a ledger")
	}
}

func ledgerTestNode(rows, capacity int64) PlanningNode {
	return PlanningNode{Ready: true, Rows: rows, CapacityBytes: capacity,
		CollectionRows: map[int64]int64{1: rows}, PendingCollectionRows: map[int64]int64{},
		CollectionChannels: map[int64]int64{}, PendingCollectionChannels: map[int64]int64{}}
}

func TestPlanningLedgerSharedSourcesAndShards(t *testing.T) {
	l := NewPlanningLedgerState(map[int64]PlanningNode{28: ledgerTestNode(0, 1000), 31: ledgerTestNode(10, 1000), 29: ledgerTestNode(50, 1000), 30: ledgerTestNode(50, 1000)}, nil, nil, nil, 8)
	first := PlanningLoad{Key: PlanningLoadKey{28, 1, 101}, From: 29, Rows: 50, MemoryBytes: 100}
	if !l.Reserve(first) || l.Rows(28, -1) != 50 || l.Rows(29, -1) != 0 {
		t.Fatal("first source projection did not enter the shared planning baseline")
	}
	// A subsequent independent source/shard call now sees31 below28.
	if l.Rows(31, -1) >= l.Rows(28, -1) {
		t.Fatal("subsequent source sees the original empty-target snapshot")
	}
	second := PlanningLoad{Key: PlanningLoadKey{31, 2, 102}, From: 30, Rows: 50}
	if !l.Reserve(second) || l.Rows(31, 2) != 50 || l.Rows(31, -1) != 60 {
		t.Fatal("cross-collection/shard projection was lost")
	}
	ctx := WithPlanningLedger(context.Background(), l)
	if PlanningLedgerFromContext(ctx) != l {
		t.Fatal("context did not carry the same shared ledger")
	}
}

func TestPlanningLedgerRejectedAddRollbackAndCommittedProjection(t *testing.T) {
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(10, 1000), 2: ledgerTestNode(0, 1000)}, nil, nil, nil, 1)
	load := PlanningLoad{Key: PlanningLoadKey{2, 1, 7}, From: 1, Rows: 10, MemoryBytes: 600}
	if !l.Reserve(load) || !l.Undo(load.Key) || l.Rows(2, -1) != 0 || l.Rows(1, -1) != 10 {
		t.Fatal("scheduler rejection must undo rows and physical reservations")
	}
	if !l.Reserve(load) {
		t.Fatal("rejected reservation still consumes the outstanding cap")
	}
	l.Commit(load.Key)
	if l.Undo(load.Key) || l.Rows(2, -1) != 10 {
		t.Fatal("accepted task projection must survive for the remainder of this snapshot")
	}
}

func TestPlanningLedgerReportedAndSchedulerSameSegmentMerge(t *testing.T) {
	node := ledgerTestNode(20, 1000)
	node.PendingRows = 10
	node.PendingCollectionRows[1] = 10
	node.ReportedLoadingMemory = 100
	local := PlanningLoad{Key: PlanningLoadKey{1, 1, 11}, Rows: 10, MemoryBytes: 80}
	remote := local
	remote.MemoryBytes = 100
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: node}, nil, []PlanningLoad{local}, []PlanningLoad{remote}, 2)
	if l.Rows(1, -1) != 30 || l.Rows(1, 1) != 30 {
		t.Fatal("same loading segment counted twice in projected rows")
	}
	// One merged pending object leaves one slot. A sum of reporters would reject.
	if !l.Reserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 12}, Rows: 1, MemoryBytes: 850}) {
		t.Fatal("same loading segment counted twice in task or memory reservation")
	}
}

func TestPlanningLedgerLoadedReportDoesNotDuplicateActualRows(t *testing.T) {
	node := ledgerTestNode(20, 1000)
	load := PlanningLoad{Key: PlanningLoadKey{1, 1, 11}, Rows: 20, MemoryBytes: 100}
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: node}, []PlanningLoadKey{load.Key}, nil, []PlanningLoad{load}, 1)
	if l.Rows(1, -1) != 20 {
		t.Fatal("reported final-stage loading duplicates distributed rows")
	}
	if l.CanReserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 12}, Rows: 1}) {
		t.Fatal("final-stage loading still occupies the physical outstanding slot")
	}
}

func TestPlanningLedgerEmptyNodeCapAndSourcePhysicalMemory(t *testing.T) {
	node := ledgerTestNode(100, 1000)
	node.MemoryUsageBytes = 900
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: node, 2: ledgerTestNode(0, 1000)}, nil, nil, nil, 2)
	for _, id := range []int64{1, 2} {
		if !l.Reserve(PlanningLoad{Key: PlanningLoadKey{2, 1, id}, From: 1, Rows: 50, MemoryBytes: 100}) {
			t.Fatal("empty node should accept work within its hard Grow cap")
		}
	}
	if l.CanReserve(PlanningLoad{Key: PlanningLoadKey{2, 1, 3}, Rows: 50}) {
		t.Fatal("empty node admitted an unbounded burst")
	}
	if memory, _ := l.PhysicalUsage(1); memory != 900 {
		t.Fatal("planned Reduce credited source physical memory before release")
	}
}

func TestPlanningLedgerAggregateReportDoesNotHideNewPlans(t *testing.T) {
	node := ledgerTestNode(0, 1000)
	node.MemoryUsageBytes = 600
	node.ReportedLoadingMemory = 250
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: node}, nil, nil, nil, 8)
	if !l.Reserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 1}, Rows: 1, MemoryBytes: 100}) {
		t.Fatal("first plan should fit")
	}
	if l.CanReserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 2}, Rows: 1, MemoryBytes: 51}) {
		t.Fatal("aggregate report swallowed the first provisional reservation")
	}
}

func TestPlanningLedgerRecoveryLargestFirstAcrossShards(t *testing.T) {
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(10, 1000), 2: ledgerTestNode(0, 1000)}, nil, nil, nil, 8)
	first := l.PlanRecoveryLoads([]PlanningLoad{
		{Key: PlanningLoadKey{-1, 1, 1}, From: -1, Rows: 5},
		{Key: PlanningLoadKey{-1, 1, 2}, From: -1, Rows: 60},
	}, []int64{1, 2}, 1)
	if len(first) != 1 || first[0].Key.SegmentID != 2 || first[0].Key.NodeID != 2 {
		t.Fatal("recovery must reserve the largest segment first")
	}
	second := l.PlanRecoveryLoads([]PlanningLoad{{Key: PlanningLoadKey{-1, 2, 3}, From: -1, Rows: 50}}, []int64{1, 2}, 8)
	if len(second) != 1 || second[0].Key.NodeID != 1 {
		t.Fatal("second shard reused the original empty-node snapshot")
	}
	if !l.Undo(first[0].Key) {
		t.Fatal("rejected Add did not roll back recovery placement")
	}
	third := l.PlanRecoveryLoads([]PlanningLoad{{Key: PlanningLoadKey{-1, 1, 2}, From: -1, Rows: 60}}, []int64{1, 2}, 8)
	if len(third) != 1 || third[0].Key.NodeID != 2 {
		t.Fatal("rejected recovery plan retained a claim")
	}
}

func TestPlanningLedgerRecoveryNewNodeBoundedBurst(t *testing.T) {
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(100, 1000), 2: ledgerTestNode(0, 1000)}, nil, nil, nil, 2)
	var incoming []PlanningLoad
	for id := int64(1); id <= 6; id++ {
		incoming = append(incoming, PlanningLoad{Key: PlanningLoadKey{-1, 1, id}, From: -1, Rows: 10 - id})
	}
	plans := l.PlanRecoveryLoads(incoming, []int64{1, 2}, 8)
	if len(plans) != 4 {
		t.Fatalf("two-node Grow envelope should admit four loads, got %d", len(plans))
	}
	if plans[0].Key.NodeID != 2 || plans[1].Key.NodeID != 2 || plans[2].Key.NodeID != 1 || plans[3].Key.NodeID != 1 {
		t.Fatal("recovery should prefer the underloaded node until its Grow cap, then use another node")
	}
}

func TestPlanningLedgerRecoveryPostPlacementCapacityAndAdmission(t *testing.T) {
	notReady := ledgerTestNode(0, 1000)
	notReady.Ready = false
	l := NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(0, 100), 2: ledgerTestNode(0, 200), 3: notReady}, nil, nil, nil, 8)
	load := PlanningLoad{Key: PlanningLoadKey{-1, 1, 1}, From: -1, Rows: 100}
	plans := l.PlanRecoveryLoads([]PlanningLoad{load}, []int64{1, 2, 3}, 8)
	if len(plans) != 1 || plans[0].Key.NodeID != 2 {
		t.Fatal("largest segment should choose lower resulting normalized load, excluding unready node")
	}
	unknown := NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(100, 0), 2: ledgerTestNode(101, 1000)}, nil, nil, nil, 8)
	plans = unknown.PlanRecoveryLoads([]PlanningLoad{load}, []int64{1, 2}, 8)
	if len(plans) != 1 || plans[0].Key.NodeID != 1 {
		t.Fatal("unknown capacity must use uniform row comparison, not pretend one-byte capacity")
	}
	if len(l.PlanRecoveryLoads([]PlanningLoad{load}, []int64{1, 2}, 0)) != 0 {
		t.Fatal("zero batch must admit zero plans")
	}
}

func TestPlanningLedgerNonPositiveGrowCapIsOne(t *testing.T) {
	for _, limit := range []int{0, -1} {
		l := NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(0, 1000)}, nil, nil, nil, limit)
		first := PlanningLoad{Key: PlanningLoadKey{1, 1, 1}, From: -1, Rows: 10}
		if !l.Reserve(first) || l.CanReserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 2}, From: -1, Rows: 10}) {
			t.Fatalf("nonpositive Grow cap %d must normalize to one", limit)
		}
	}
}

func TestPlanningLedgerPhysicalEnvelopeMergesReportsAndKeepsSourceResident(t *testing.T) {
	resident := ledgerTestNode(50, 1000)
	resident.MemoryUsageBytes, resident.DiskUsageBytes = 500, 700
	resident.ReportedLoadingMemory, resident.ReportedLoadingDisk = 100, 200
	pending := PlanningLoad{Key: PlanningLoadKey{1, 1, 1}, From: -1, Rows: 10, MemoryBytes: 80, DiskBytes: 150}
	report := pending
	report.MemoryBytes, report.DiskBytes = 100, 200
	ledger := NewPlanningLedgerState(map[int64]PlanningNode{1: resident, 2: ledgerTestNode(0, 1000)}, nil, []PlanningLoad{pending}, []PlanningLoad{report}, 8)
	requireEnvelope := func(memory, disk int64) {
		t.Helper()
		gotMemory, gotDisk := ledger.PhysicalUsage(1)
		if gotMemory != memory || gotDisk != disk {
			t.Fatalf("physical envelope memory/disk = %d/%d, want %d/%d", gotMemory, gotDisk, memory, disk)
		}
	}
	requireEnvelope(600, 900)
	if !ledger.Reserve(PlanningLoad{Key: PlanningLoadKey{2, 1, 2}, From: 1, Rows: 40, MemoryBytes: 400, DiskBytes: 500}) {
		t.Fatal("outgoing source plan should fit target")
	}
	requireEnvelope(600, 900)
	if !ledger.Reserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 3}, From: -1, Rows: 10, MemoryBytes: 50, DiskBytes: 100}) {
		t.Fatal("incoming plan should fit source")
	}
	requireEnvelope(650, 1000)
}

func TestPlanningLedgerTieredAdmissionDefersToQueryNode(t *testing.T) {
	resident := ledgerTestNode(100, 1000)
	resident.MemoryUsageBytes = 1000
	load := PlanningLoad{Key: PlanningLoadKey{1, 1, 1}, From: -1, Rows: 10, MemoryBytes: 400}
	strict := NewPlanningLedgerState(map[int64]PlanningNode{1: resident}, nil, nil, nil, 2)
	if strict.Reserve(load) {
		t.Fatal("a full non-tiered node cannot receive an advisory oversize probe")
	}
	resident.ReclaimableMemory = true
	tiered := NewPlanningLedgerState(map[int64]PlanningNode{1: resident}, nil, nil, nil, 2)
	if !tiered.Reserve(load) {
		t.Fatal("tiered cache residency must defer to native eviction-aware admission")
	}
	if memory, _ := tiered.PhysicalUsage(1); memory != 1400 {
		t.Fatal("tiered admission must retain advisory resource accounting")
	}
	load.Key.SegmentID = 2
	if !tiered.Reserve(load) {
		t.Fatal("tiered nodes may admit additional work within the hard Grow cap")
	}
	load.Key.SegmentID = 3
	if tiered.Reserve(load) {
		t.Fatal("tiered admission must still enforce the hard Grow cap")
	}
}

func TestPlanningLedgerAdvisoryOversizeProbeIsBounded(t *testing.T) {
	resident := ledgerTestNode(100, 1000)
	resident.MemoryUsageBytes = 900
	ledger := NewPlanningLedgerState(map[int64]PlanningNode{1: resident}, nil, nil, nil, 8)
	probe := PlanningLoad{Key: PlanningLoadKey{1, 1, 1}, From: -1, Rows: 10, MemoryBytes: 2000}
	if !ledger.Reserve(probe) {
		t.Fatal("raw-binlog overestimate cannot permanently block every recovery target")
	}
	if memory, _ := ledger.PhysicalUsage(1); memory != 2900 {
		t.Fatal("oversize estimate must remain in the ledger after probe admission")
	}
	if ledger.Reserve(PlanningLoad{Key: PlanningLoadKey{1, 1, 2}, From: -1, Rows: 10, MemoryBytes: 1}) {
		t.Fatal("an oversize probe cannot admit a second Grow onto that node")
	}
	if !ledger.Undo(probe.Key) || !ledger.Reserve(probe) {
		t.Fatal("scheduler rejection must release the advisory probe")
	}
	resident.ReportedLoadingMemory = 100
	full := NewPlanningLedgerState(map[int64]PlanningNode{1: resident}, nil, nil, nil, 8)
	if full.Reserve(probe) {
		t.Fatal("resident plus aggregate loading with no headroom cannot probe")
	}
	pending := PlanningLoad{Key: PlanningLoadKey{1, 1, 3}, Rows: 10}
	busy := NewPlanningLedgerState(map[int64]PlanningNode{1: ledgerTestNode(0, 1000)}, nil, []PlanningLoad{pending}, nil, 8)
	if busy.Reserve(probe) {
		t.Fatal("local pending Grow, even with unknown memory, prevents another oversize probe")
	}
}
