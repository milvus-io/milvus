package planparserv2

import (
	"unsafe"

	"github.com/antlr4-go/antlr/v4"

	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
)

type exprCacheEntry struct {
	value          any
	estimatedBytes int
}

func newExprCacheErrorEntry(key string, err error) exprCacheEntry {
	return exprCacheEntry{
		value:          err,
		estimatedBytes: len(key) + len(err.Error()) + int(unsafe.Sizeof(exprCacheEntry{})),
	}
}

func estimateExprCacheBytes(key string, input *antlr.InputStream, tokens *antlr.CommonTokenStream) int {
	// Approximate tree storage as one rule context, one terminal node and two
	// child references per buffered token. This is a heuristic, not a heap size:
	// shared parser/lexer state, allocator rounding and LRU metadata are excluded.
	const treeBytesPerToken = unsafe.Sizeof(parser.ExprContext{}) +
		unsafe.Sizeof(antlr.TerminalNodeImpl{}) + 2*unsafe.Sizeof(antlr.Tree(nil))
	return len(key) + input.Size()*int(unsafe.Sizeof(rune(0))) +
		tokens.Size()*int(unsafe.Sizeof(antlr.CommonToken{})+treeBytesPerToken) +
		cap(tokens.GetAllTokens())*int(unsafe.Sizeof(antlr.Token(nil))) +
		int(unsafe.Sizeof(exprCacheEntry{})+unsafe.Sizeof(antlr.InputStream{})+unsafe.Sizeof(antlr.CommonTokenStream{}))
}

func exprCacheEstimatedBytes() int {
	// Values takes a snapshot of unexpired entries under the LRU lock. Summing
	// their precomputed sizes also handles replacement, eviction and purge.
	var total int
	for _, entry := range exprCache.Values() {
		total += entry.estimatedBytes
	}
	return total
}
