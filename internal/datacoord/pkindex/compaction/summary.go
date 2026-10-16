package compaction

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
)

// group is one flushed table, or all the tables of one run, in the manifest
// list. In identifiers, "L0" means a flushed table (run id 0).
type group struct {
	runID  int64 // 0: one flushed table
	start  int   // index of the first table in the manifest list
	tables int
	bytes  int64
}

// groupSSTs splits the newest-first SST list into groups. The tables of one
// run are contiguous in the list.
func groupSSTs(list []*pkindexpb.SSTRef) []group {
	gs := make([]group, 0, len(list))
	for i, s := range list {
		if s.GetRunId() != 0 && len(gs) > 0 && gs[len(gs)-1].runID == s.GetRunId() {
			g := &gs[len(gs)-1]
			g.tables++
			g.bytes += s.GetSize()
			continue
		}
		gs = append(gs, group{runID: s.GetRunId(), start: i, tables: 1, bytes: s.GetSize()})
	}
	return gs
}

// l0Prefix returns the number of leading flushed-table groups.
func l0Prefix(gs []group) int {
	n := 0
	for _, g := range gs {
		if g.runID != 0 {
			break
		}
		n++
	}
	return n
}

// summary is what the inspector keeps per vchannel between inspections.
type summary struct {
	version int64
	groups  []group
	dirtyAt time.Time
}

func summarize(m *pkindexpb.Manifest, now time.Time) *summary {
	return &summary{version: m.GetVersion(), groups: groupSSTs(m.GetSsts()), dirtyAt: now}
}
