package dql

import (
	"container/heap"
	"context"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// groupByMerge keeps only each shard's current head. Quota rejections advance
// that head too; the heap does not change the grouping or offset policy.
type groupByMerge struct {
	ctx     context.Context
	sources []*schemapb.SearchResultData
	offsets [][]int64
	cursors []int64
	qi      int64
	heads   []int
	linear  bool
}

func newGroupByMerge(ctx context.Context, sources []*schemapb.SearchResultData, offsets [][]int64, cursors []int64, qi int64) *groupByMerge {
	m := &groupByMerge{ctx: ctx, sources: sources, offsets: offsets, cursors: cursors, qi: qi, linear: len(sources) <= 2}
	if m.linear {
		return m
	}
	for i, source := range sources {
		if source.Topks[qi] == 0 {
			continue
		}
		if math.IsNaN(float64(source.Scores[offsets[i][qi]])) {
			m.linear = true
			return m
		}
		m.heads = append(m.heads, i)
	}
	heap.Init(m)
	return m
}

func (m *groupByMerge) peek() (int, int64) {
	if m.linear {
		return selectHighestScoreIndex(m.ctx, m.sources, m.offsets, m.cursors, m.qi)
	}
	if len(m.heads) == 0 {
		return -1, -1
	}
	source := m.heads[0]
	return source, m.offsets[source][m.qi] + m.cursors[source]
}

func (m *groupByMerge) advance(source int) {
	m.cursors[source]++
	if m.linear {
		return
	}
	if m.cursors[source] == m.sources[source].Topks[m.qi] {
		heap.Pop(m)
		return
	}
	row := m.offsets[source][m.qi] + m.cursors[source]
	if math.IsNaN(float64(m.sources[source].Scores[row])) {
		// The legacy scan's NaN behavior depends on source order and cannot
		// be expressed by a heap comparator; use it for the rest of this query.
		m.linear = true
		return
	}
	heap.Fix(m, 0)
}

func (m *groupByMerge) Len() int { return len(m.heads) }
func (m *groupByMerge) Less(i, j int) bool {
	a, b := m.heads[i], m.heads[j]
	ai, bi := m.offsets[a][m.qi]+m.cursors[a], m.offsets[b][m.qi]+m.cursors[b]
	as, bs := m.sources[a].Scores[ai], m.sources[b].Scores[bi]
	if as != bs {
		return as > bs
	}
	ap, bp := typeutil.GetPK(m.sources[a].GetIds(), ai), typeutil.GetPK(m.sources[b].GetIds(), bi)
	if ap != bp {
		return typeutil.ComparePK(ap, bp)
	}
	return a < b
}
func (m *groupByMerge) Swap(i, j int)  { m.heads[i], m.heads[j] = m.heads[j], m.heads[i] }
func (m *groupByMerge) Push(value any) { m.heads = append(m.heads, value.(int)) }
func (m *groupByMerge) Pop() any {
	last := m.heads[len(m.heads)-1]
	m.heads = m.heads[:len(m.heads)-1]
	return last
}
