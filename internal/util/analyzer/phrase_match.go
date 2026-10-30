package analyzer

import (
	"container/heap"
	"math"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

const unmatchedPhraseSlop int64 = -1

type phrasePosition struct {
	positions []int64
	offset    int64
}

func (p phrasePosition) value() int64 { return p.positions[0] + p.offset }

type phraseHeap []phrasePosition

func (h phraseHeap) Len() int           { return len(h) }
func (h phraseHeap) Less(i, j int) bool { return h[i].value() < h[j].value() }
func (h phraseHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *phraseHeap) Push(x any)        { *h = append(*h, x.(phrasePosition)) }
func (h *phraseHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// phraseMatchSlop mirrors Tantivy's smallest-range calculation over token
// positions. Offsets align each data occurrence with its query position. The
// minimum range covering all query terms is the required slop. Repeated query
// terms intentionally share occurrence lists, matching the native API.
func phraseMatchSlop(query, data []*milvuspb.AnalyzerToken) int64 {
	if len(query) == 0 {
		return 0
	}
	positions := make(map[string][]int64)
	for _, token := range data {
		positions[token.GetToken()] = append(positions[token.GetToken()], token.GetPosition())
	}
	for _, list := range positions {
		slices.Sort(list)
	}
	queue := make(phraseHeap, 0, len(query))
	maximum := int64(math.MinInt64)
	for _, token := range query {
		list := positions[token.GetToken()]
		if len(list) == 0 {
			return unmatchedPhraseSlop
		}
		item := phrasePosition{positions: list, offset: -token.GetPosition()}
		queue = append(queue, item)
		maximum = max(maximum, item.value())
	}
	heap.Init(&queue)
	slop := int64(math.MaxInt64)
	for {
		slop = min(slop, maximum-queue[0].value())
		if slop == 0 || len(queue[0].positions) == 1 {
			return slop
		}
		queue[0].positions = queue[0].positions[1:]
		maximum = max(maximum, queue[0].value())
		heap.Fix(&queue, 0)
	}
}
