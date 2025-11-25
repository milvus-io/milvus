package delegator

import (
	"fmt"
	"sort"
	"unicode/utf8"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func BuildSparseFieldData(field *schemapb.FieldSchema, sparseArray *schemapb.SparseFloatArray) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: sparseArray.GetDim(),
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: sparseArray,
				},
			},
		},
		FieldId: field.GetFieldID(),
	}
}

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

func fetchFragmentsFromOffsets(text string, span SpanList, fragmentSize int64, numOfFragments int64) []*querypb.HighlightFragment {
	result := make([]*querypb.HighlightFragment, 0)
	endPosition := int(fragmentSize)
	nowOffset := 0
	frag := &querypb.HighlightFragment{
		StartOffset: 0,
	}

	next := func() {
		endPosition += int(fragmentSize)
		frag.EndOffset = int64(nowOffset)
		result = append(result, frag)
		frag = &querypb.HighlightFragment{
			StartOffset: int64(nowOffset),
		}
	}

	cursor := 0
	spanNum := len(span)
	for i, r := range text {
		nowOffset += utf8.RuneLen(r)

		// append if span was included in current fragment
		for ; cursor < spanNum && span[cursor][1] <= int64(nowOffset); cursor++ {
			if span[cursor][0] >= frag.StartOffset {
				frag.Offsets = append(frag.Offsets, span[cursor][0], span[cursor][1])
			} else {
				// if some span cross fragment start, append the part in current fragment
				frag.Offsets = append(frag.Offsets, frag.StartOffset, span[cursor][1])
			}
		}

		if i >= endPosition {
			// if some span cross fragment end, append the part in current fragment
			if cursor < spanNum && span[cursor][0] < int64(nowOffset) {
				frag.Offsets = append(frag.Offsets, span[cursor][0], int64(nowOffset))
			}
			next()
			// skip all if no span remain or get enough num of fragments
			if cursor >= spanNum || int64(len(result)) >= numOfFragments {
				break
			}
		}
	}

	if nowOffset > int(frag.StartOffset) {
		next()
	}
	return result
}
