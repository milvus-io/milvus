package highlight

import (
	"sort"
	"unicode/utf8"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// BuildFragments matches analyzed search/query tokens against corpus tokens.
// Analyzer offsets are bytes; returned fragments use rune offsets for rendering.
func BuildFragments(task *querypb.HighlightTask, topks []int64, results [][]*milvuspb.AnalyzerToken) ([]*querypb.HighlightResult, error) {
	if task.GetSearchTextNum() < 0 || task.GetCorpusTextNum() < 0 || len(results) != len(task.GetTexts()) || int64(len(results)) != task.GetSearchTextNum()+task.GetCorpusTextNum()+int64(len(task.GetQueries())) {
		return nil, merr.WrapErrServiceInternalMsg("highlight analyzer result count does not match texts")
	}
	var rows int64
	for _, topk := range topks {
		if topk < 0 {
			return nil, merr.WrapErrServiceInternalMsg("negative highlight topk")
		}
		rows += topk
	}
	if rows != task.GetCorpusTextNum() {
		return nil, merr.WrapErrServiceInternalMsg("highlight corpus count does not match topks")
	}
	result := make([]*querypb.HighlightResult, 0, rows)
	// analyze result of search text
	searchResults := results[0:task.SearchTextNum]
	// analyze result of query text
	queryResults := results[task.SearchTextNum : task.SearchTextNum+int64(len(task.Queries))]
	// analyze result of corpus text
	corpusStartOffset := int(task.SearchTextNum) + len(task.Queries)
	corpusResults := results[corpusStartOffset:]

	// query for all corpus texts
	// only support text match now
	// build match set for all analyze result of query text
	// TODO: support more query types
	queryTokenSet := typeutil.NewSet[string]()
	for _, tokens := range queryResults {
		for _, token := range tokens {
			queryTokenSet.Insert(token.GetToken())
		}
	}

	corpusIdx := 0
	for i, topk := range topks {
		tokenSet := typeutil.NewSet[string]()
		if len(searchResults) > i {
			for _, token := range searchResults[i] {
				tokenSet.Insert(token.GetToken())
			}
		}

		for j := int64(0); j < topk; j++ {
			spans := SpanList{}
			for _, token := range corpusResults[corpusIdx] {
				if tokenSet.Contain(token.GetToken()) || queryTokenSet.Contain(token.GetToken()) {
					spans = append(spans, Span{token.GetStartOffset(), token.GetEndOffset()})
				}
			}
			spans = mergeOffsets(spans)

			// Convert byte offsets from analyzer to rune (character) offsets
			corpusText := task.Texts[corpusStartOffset+corpusIdx]
			err := bytesOffsetToRuneOffset(corpusText, spans)
			if err != nil {
				return nil, err
			}

			frags := fetchFragmentsFromOffsets(corpusText, spans,
				task.GetOptions().GetFragmentOffset(),
				task.GetOptions().GetFragmentSize(),
				task.GetOptions().GetNumOfFragments())
			result = append(result, &querypb.HighlightResult{Fragments: frags})
			corpusIdx++
		}
	}
	return result, nil
}

type (
	Span     [2]int64
	SpanList []Span
)

func (a SpanList) Len() int      { return len(a) }
func (a SpanList) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SpanList) Less(i, j int) bool {
	if a[i][0] == a[j][0] {
		return a[i][1] < a[j][1]
	}
	return a[i][0] < a[j][0]
}

// merge repeated segments
func mergeOffsets(input SpanList) SpanList {
	sort.Sort(input)
	maxEndOffset := int64(-1)
	offsets := SpanList{}
	for _, pair := range input {
		if pair[1] > maxEndOffset {
			if len(offsets) == 0 || pair[0] > offsets[len(offsets)-1][1] {
				// if start offset > max offset before,
				// no any intersection with previous one,
				// use all pair.
				offsets = append(offsets, pair)
			} else {
				// if start offset <= max offset before,
				// has intersection with previous one,
				// merge two offset to one.
				offsets[len(offsets)-1][1] = pair[1]
			}
			maxEndOffset = pair[1]
		}
	}
	return offsets
}

func bytesOffsetToRuneOffset(text string, spans SpanList) error {
	byteOffsetSet := typeutil.NewSet[int64]()
	for _, span := range spans {
		byteOffsetSet.Insert(span[0])
		byteOffsetSet.Insert(span[1])
	}
	offsetMap := map[int64]int64{0: 0, int64(len(text)): int64(utf8.RuneCountInString(text))}

	cnt := int64(0)
	for i := range text {
		if byteOffsetSet.Contain(int64(i)) {
			offsetMap[int64(i)] = cnt
		}
		cnt++
	}

	// convert spans from byte offsets to rune offsets
	for i, span := range spans {
		startOffset, ok := offsetMap[span[0]]
		if !ok {
			return merr.WrapErrServiceInternalMsg("start offset: %d not found (text: %d bytes)", span[0], len(text))
		}
		endOffset, ok := offsetMap[span[1]]
		if !ok {
			return merr.WrapErrServiceInternalMsg("end offset: %d not found (text: %d bytes)", span[1], len(text))
		}
		spans[i][0] = startOffset
		spans[i][1] = endOffset
	}
	return nil
}

func fetchFragmentsFromOffsets(text string, spans SpanList, fragmentOffset int64, fragmentSize int64, numOfFragments int64) []*querypb.HighlightFragment {
	result := make([]*querypb.HighlightFragment, 0)
	textRuneLen := int64(utf8.RuneCountInString(text))

	var frag *querypb.HighlightFragment = nil
	next := func(span *Span) bool {
		startOffset := max(0, span[0]-fragmentOffset)
		endOffset := min(max(span[1], startOffset+fragmentSize), textRuneLen)
		if frag != nil {
			result = append(result, frag)
		}
		if len(result) >= int(numOfFragments) {
			frag = nil
			return false
		}
		frag = &querypb.HighlightFragment{
			StartOffset: startOffset,
			EndOffset:   endOffset,
			Offsets:     []int64{span[0], span[1]},
		}
		return true
	}

	for i, span := range spans {
		if frag == nil || span[0] > frag.EndOffset {
			if !next(&span) {
				break
			}
		} else {
			// append rune offset to fragment
			frag.Offsets = append(frag.Offsets, spans[i][0], spans[i][1])
			// extend fragment end offset if this span goes beyond current boundary
			if span[1] > frag.EndOffset {
				frag.EndOffset = span[1]
			}
		}
	}

	if frag != nil {
		result = append(result, frag)
	}
	return result
}
