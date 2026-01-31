package delegator

import (
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func SetBM25Params(req *internalpb.SearchRequest, avgdl float64) error {
	log := log.With(zap.Int64("collection", req.GetCollectionID()))

	serializedPlan := req.GetSerializedExprPlan()
	// plan not found
	if serializedPlan == nil {
		log.Warn("serialized plan not found")
		return merr.WrapErrParameterInvalid("serialized search plan", "nil")
	}

	plan := planpb.PlanNode{}
	err := proto.Unmarshal(serializedPlan, &plan)
	if err != nil {
		log.Warn("failed to unmarshal plan", zap.Error(err))
		return merr.WrapErrParameterInvalid("valid serialized search plan", "no unmarshalable one", err.Error())
	}

	switch plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		queryInfo := plan.GetVectorAnns().GetQueryInfo()
		queryInfo.Bm25Avgdl = avgdl
		serializedExprPlan, err := proto.Marshal(&plan)
		if err != nil {
			log.Warn("failed to marshal optimized plan", zap.Error(err))
			return merr.WrapErrParameterInvalid("marshalable search plan", "plan with marshal error", err.Error())
		}
		req.SerializedExprPlan = serializedExprPlan
		log.Debug("add bm25 avgdl to search params done", zap.Any("queryInfo", queryInfo))
	default:
		log.Warn("not supported node type", zap.String("nodeType", fmt.Sprintf("%T", plan.GetNode())))
	}
	return nil
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
			return errors.Errorf("start offset: %d not found (text: %d bytes)", span[0], len(text))
		}
		endOffset, ok := offsetMap[span[1]]
		if !ok {
			return errors.Errorf("end offset: %d not found (text: %d bytes)", span[1], len(text))
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
