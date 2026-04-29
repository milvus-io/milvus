package segments

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SearchReduce interface {
	ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) (*schemapb.SearchResultData, error)
}

type elementSearchResultKey struct {
	pk           interface{}
	elementIndex int64
}

func getSearchResultDedupKey(data *schemapb.SearchResultData, idx int64, pk interface{}, hasElementIndices bool) (interface{}, error) {
	if !hasElementIndices {
		return pk, nil
	}
	elementIndices := data.GetElementIndices()
	if elementIndices == nil || idx < 0 || idx >= int64(len(elementIndices.GetData())) {
		return nil, fmt.Errorf("element-level search result missing element index at offset %d", idx)
	}
	return elementSearchResultKey{
		pk:           pk,
		elementIndex: elementIndices.GetData()[idx],
	}, nil
}

type SearchCommonReduce struct{}

func (scr *SearchCommonReduce) ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) (*schemapb.SearchResultData, error) {
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceSearchResultData")
	defer sp.End()
	log := log.Ctx(ctx)

	if len(searchResultData) == 0 {
		return &schemapb.SearchResultData{
			NumQueries: info.GetNq(),
			TopK:       info.GetTopK(),
			FieldsData: make([]*schemapb.FieldData, 0),
			Scores:     make([]float32, 0),
			Ids:        &schemapb.IDs{},
			Topks:      make([]int64, 0),
		}, nil
	}
	nq := info.GetNq()
	topk := info.GetTopK()
	ret := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0, nq*topk),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0, nq),
	}

	// Determine element-level flag: any result having ElementIndices means element-level search.
	hasElementIndices := false
	for _, data := range searchResultData {
		if data.ElementIndices != nil {
			hasElementIndices = true
			break
		}
	}
	if hasElementIndices {
		for i, data := range searchResultData {
			// If any result has ElementIndices, all results must have ElementIndices or no doc is hit in the segment
			if data.ElementIndices == nil {
				ids := data.GetIds()
				// When a segment returns 0 hits, C++ reduce creates an empty LongArray
				// which proto3 serializes as absent (nil). Back-fill for uniformity.
				if ids == nil || typeutil.GetSizeOfIDs(ids) == 0 {
					data.ElementIndices = &schemapb.LongArray{}
				} else {
					return nil, fmt.Errorf("inconsistent element-level flag in search results: result has data but missing ElementIndices at index %d", i)
				}
			}
		}
		ret.ElementIndices = &schemapb.LongArray{
			Data: make([]int64, 0),
		}
	}

	resultOffsets := make([][]int64, len(searchResultData))
	totalOffsetElements := 0
	for i, data := range searchResultData {
		if int64(len(data.Topks)) < nq {
			return nil, fmt.Errorf("invalid search result topks length at index %d: got %d, expected at least %d", i, len(data.Topks), nq)
		}
		totalOffsetElements += len(data.Topks)
	}
	offsetBacking := make([]int64, totalOffsetElements)
	for i := 0; i < len(searchResultData); i++ {
		data := searchResultData[i]
		topks := data.Topks
		n := len(topks)
		resultOffsets[i] = offsetBacking[:n:n]
		offsetBacking = offsetBacking[n:]
		for j := 1; j < n; j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + topks[j-1]
		}
		ret.AllSearchCount += data.GetAllSearchCount()
	}

	idxComputers := make([]*typeutil.FieldDataIdxComputer, len(searchResultData))
	for i, srd := range searchResultData {
		idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.FieldsData)
	}

	numResults := len(searchResultData)
	var skipDupCnt int64
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for i := int64(0); i < nq; i++ {
		offsets := make([]int64, numResults)
		dedupSet := make(map[interface{}]struct{})
		var j int64
		for j = 0; j < topk; {
			sel := SelectSearchResultData(searchResultData, resultOffsets, offsets, i)
			if sel == -1 {
				break
			}
			idx := resultOffsets[sel][i] + offsets[sel]

			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			score := searchResultData[sel].Scores[idx]
			dedupKey, err := getSearchResultDedupKey(searchResultData[sel], idx, id, hasElementIndices)
			if err != nil {
				return nil, err
			}

			// remove duplicates
			if _, ok := dedupSet[dedupKey]; !ok {
				fieldsData := searchResultData[sel].FieldsData
				fieldIdxs := idxComputers[sel].Compute(idx)
				retSize += typeutil.AppendFieldData(ret.FieldsData, fieldsData, idx, fieldIdxs...)
				typeutil.AppendPKs(ret.Ids, id)
				ret.Scores = append(ret.Scores, score)
				if searchResultData[sel].ElementIndices != nil && ret.ElementIndices != nil {
					ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].ElementIndices.Data[idx])
				}
				dedupSet[dedupKey] = struct{}{}
				j++
			} else {
				// skip the same row-level entity or the same element-level hit
				skipDupCnt++
			}
			offsets[sel]++
		}

		// if realTopK != -1 && realTopK != j {
		// 	log.Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
		// 	// return nil, errors.New("the length (topk) between all result of query is different")
		// }
		ret.Topks = append(ret.Topks, j)

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	log.Debug("skip duplicated search result", zap.Int64("count", skipDupCnt))
	return ret, nil
}

type SearchGroupByReduce struct{}

func (sbr *SearchGroupByReduce) ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) (*schemapb.SearchResultData, error) {
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceSearchResultData")
	defer sp.End()
	log := log.Ctx(ctx)

	if len(searchResultData) == 0 {
		log.Debug("Shortcut return SearchGroupByReduce, directly return empty result", zap.Any("result info", info))
		return &schemapb.SearchResultData{
			NumQueries: info.GetNq(),
			TopK:       info.GetTopK(),
			FieldsData: make([]*schemapb.FieldData, 0),
			Scores:     make([]float32, 0),
			Ids:        &schemapb.IDs{},
			Topks:      make([]int64, 0),
		}, nil
	}
	nq := info.GetNq()
	topk := info.GetTopK()
	ret := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0, nq*topk),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0, nq),
	}

	// Determine element-level flag: any result having ElementIndices means element-level search.
	hasElementIndices := false
	for _, data := range searchResultData {
		if data.ElementIndices != nil {
			hasElementIndices = true
			break
		}
	}
	if hasElementIndices {
		// If any result has ElementIndices, all results must have ElementIndices or no doc is hit in the segment
		for i, data := range searchResultData {
			if data.ElementIndices == nil {
				ids := data.GetIds()
				// When a segment returns 0 hits, C++ reduce creates an empty LongArray
				// which proto3 serializes as absent (nil). Back-fill for uniformity.
				if ids == nil || typeutil.GetSizeOfIDs(ids) == 0 {
					data.ElementIndices = &schemapb.LongArray{}
				} else {
					return nil, fmt.Errorf("inconsistent element-level flag in search results: result has data but missing ElementIndices at index %d", i)
				}
			}
		}
		ret.ElementIndices = &schemapb.LongArray{
			Data: make([]int64, 0),
		}
	}

	resultOffsets := make([][]int64, len(searchResultData))
	totalOffsetElements := 0
	for i, data := range searchResultData {
		if int64(len(data.Topks)) < nq {
			return nil, fmt.Errorf("invalid search result topks length at index %d: got %d, expected at least %d", i, len(data.Topks), nq)
		}
		totalOffsetElements += len(data.Topks)
	}
	offsetBacking := make([]int64, totalOffsetElements)
	groupByValIterator := make([]func(int) any, len(searchResultData))
	for i := range searchResultData {
		data := searchResultData[i]
		topks := data.Topks
		n := len(topks)
		resultOffsets[i] = offsetBacking[:n:n]
		offsetBacking = offsetBacking[n:]
		for j := 1; j < n; j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + topks[j-1]
		}
		ret.AllSearchCount += data.GetAllSearchCount()
		groupByValIterator[i] = typeutil.GetDataIterator(data.GetGroupByFieldValue())
	}
	gpFieldBuilder, err := typeutil.NewFieldDataBuilder(searchResultData[0].GetGroupByFieldValue().GetType(), true, int(info.GetTopK()))
	if err != nil {
		return ret, merr.WrapErrServiceInternal("failed to construct group by field data builder, this is abnormal as segcore should always set up a group by field, no matter data status, check code on qn", err.Error())
	}

	idxComputers := make([]*typeutil.FieldDataIdxComputer, len(searchResultData))
	for i, srd := range searchResultData {
		idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.FieldsData)
	}

	var filteredCount int64
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	groupSize := info.GetGroupSize()
	if groupSize <= 0 {
		groupSize = 1
	}
	groupBound := info.GetTopK() * groupSize

	for i := int64(0); i < info.GetNq(); i++ {
		offsets := make([]int64, len(searchResultData))

		dedupSet := make(map[interface{}]struct{})
		groupByValueMap := make(map[interface{}]int64)

		var j int64
		for j = 0; j < groupBound; {
			sel := SelectSearchResultData(searchResultData, resultOffsets, offsets, i)
			if sel == -1 {
				break
			}
			idx := resultOffsets[sel][i] + offsets[sel]

			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			groupByVal := groupByValIterator[sel](int(idx))
			score := searchResultData[sel].Scores[idx]
			dedupKey, err := getSearchResultDedupKey(searchResultData[sel], idx, id, hasElementIndices)
			if err != nil {
				return nil, err
			}

			if _, ok := dedupSet[dedupKey]; !ok {
				groupCount := groupByValueMap[groupByVal]
				if groupCount == 0 && int64(len(groupByValueMap)) >= info.GetTopK() {
					// exceed the limit for group count, filter this entity
					filteredCount++
				} else if groupCount >= groupSize {
					// exceed the limit for each group, filter this entity
					filteredCount++
				} else {
					fieldsData := searchResultData[sel].FieldsData
					fieldIdxs := idxComputers[sel].Compute(idx)
					retSize += typeutil.AppendFieldData(ret.FieldsData, fieldsData, idx, fieldIdxs...)
					typeutil.AppendPKs(ret.Ids, id)
					ret.Scores = append(ret.Scores, score)
					if searchResultData[sel].ElementIndices != nil && ret.ElementIndices != nil {
						ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].ElementIndices.Data[idx])
					}
					gpFieldBuilder.Add(groupByVal)
					groupByValueMap[groupByVal] += 1
					dedupSet[dedupKey] = struct{}{}
					j++
				}
			} else {
				// skip the same row-level entity or the same element-level hit
				filteredCount++
			}
			offsets[sel]++
		}
		ret.Topks = append(ret.Topks, j)

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	ret.GroupByFieldValue = gpFieldBuilder.Build()
	if float64(filteredCount) >= 0.3*float64(groupBound) {
		log.Warn("GroupBy reduce filtered too many results, "+
			"this may influence the final result seriously",
			zap.Int64("filteredCount", filteredCount),
			zap.Int64("groupBound", groupBound))
	}
	log.Debug("skip duplicated search result", zap.Int64("count", filteredCount))
	return ret, nil
}

func InitSearchReducer(info *reduce.ResultInfo) SearchReduce {
	if info.GetGroupByFieldId() > 0 {
		return &SearchGroupByReduce{}
	}
	return &SearchCommonReduce{}
}
