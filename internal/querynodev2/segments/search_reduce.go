package segments

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SearchReduce interface {
	ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) (*schemapb.SearchResultData, error)
}

type SearchCommonReduce struct{}

type elementSearchResultKey struct {
	pk           interface{}
	elementIndex int64
}

func checkElementIndices(searchResultData []*schemapb.SearchResultData) (bool, error) {
	if len(searchResultData) == 0 {
		return false, nil
	}

	hasElementIndices := false
	for _, data := range searchResultData {
		if data.GetElementIndices() != nil {
			hasElementIndices = true
			break
		}
	}
	if !hasElementIndices {
		return false, nil
	}

	for i, data := range searchResultData {
		if data.GetElementIndices() == nil {
			if ids := data.GetIds(); ids == nil || typeutil.GetSizeOfIDs(ids) == 0 {
				data.ElementIndices = &schemapb.LongArray{Data: []int64{}}
				continue
			}
			return false, merr.WrapErrServiceInternalMsg("inconsistent element-level search result: result[%d] has hits but misses element indices", i)
		}
		if len(data.GetElementIndices().GetData()) != len(data.GetScores()) {
			return false, merr.WrapErrServiceInternalMsg("invalid element-level search result: element indices length %d does not match scores length %d",
				len(data.GetElementIndices().GetData()), len(data.GetScores()))
		}
	}
	return true, nil
}

func getSearchResultDedupKey(data *schemapb.SearchResultData, idx int64, id interface{}, hasElementIndices bool) (interface{}, error) {
	if !hasElementIndices {
		return id, nil
	}

	elementIndices := data.GetElementIndices()
	if elementIndices == nil || idx < 0 || idx >= int64(len(elementIndices.GetData())) {
		return nil, merr.WrapErrServiceInternalMsg("element-level search result missing element index at offset %d", idx)
	}
	return elementSearchResultKey{
		pk:           id,
		elementIndex: elementIndices.GetData()[idx],
	}, nil
}

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
	ret := &schemapb.SearchResultData{
		NumQueries: info.GetNq(),
		TopK:       info.GetTopK(),
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}
	hasElementIndices, err := checkElementIndices(searchResultData)
	if err != nil {
		return nil, err
	}
	if hasElementIndices {
		ret.ElementIndices = &schemapb.LongArray{Data: make([]int64, 0)}
	}

	resultOffsets := make([][]int64, len(searchResultData))
	for i := 0; i < len(searchResultData); i++ {
		resultOffsets[i] = make([]int64, len(searchResultData[i].Topks))
		for j := int64(1); j < info.GetNq(); j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + searchResultData[i].Topks[j-1]
		}
		ret.AllSearchCount += searchResultData[i].GetAllSearchCount()
	}

	idxComputers := make([]*typeutil.FieldDataIdxComputer, len(searchResultData))
	for i, srd := range searchResultData {
		idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.FieldsData)
	}

	var skipDupCnt int64
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for i := int64(0); i < info.GetNq(); i++ {
		offsets := make([]int64, len(searchResultData))
		idSet := make(map[interface{}]struct{})
		var j int64
		for j = 0; j < info.GetTopK(); {
			sel := SelectSearchResultData(searchResultData, resultOffsets, offsets, i)
			if sel == -1 {
				break
			}
			idx := resultOffsets[sel][i] + offsets[sel]

			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			score := searchResultData[sel].Scores[idx]

			// remove duplicates
			key, err := getSearchResultDedupKey(searchResultData[sel], idx, id, hasElementIndices)
			if err != nil {
				return nil, err
			}
			if _, ok := idSet[key]; !ok {
				fieldsData := searchResultData[sel].FieldsData
				fieldIdxs := idxComputers[sel].Compute(idx)
				retSize += typeutil.AppendFieldData(ret.FieldsData, fieldsData, idx, fieldIdxs...)
				typeutil.AppendPKs(ret.Ids, id)
				ret.Scores = append(ret.Scores, score)
				if hasElementIndices {
					ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].GetElementIndices().GetData()[idx])
				}
				idSet[key] = struct{}{}
				j++
			} else {
				// skip entity with same id
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
			return nil, merr.WrapErrParameterInvalidMsg("search results exceed the maxOutputSize Limit %d", maxOutputSize)
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
	ret := &schemapb.SearchResultData{
		NumQueries: info.GetNq(),
		TopK:       info.GetTopK(),
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}
	hasElementIndices, err := checkElementIndices(searchResultData)
	if err != nil {
		return nil, err
	}
	if hasElementIndices {
		ret.ElementIndices = &schemapb.LongArray{Data: make([]int64, 0)}
	}

	resultOffsets := make([][]int64, len(searchResultData))
	groupByValIterator := make([]func(int) any, len(searchResultData))
	for i := range searchResultData {
		resultOffsets[i] = make([]int64, len(searchResultData[i].Topks))
		for j := int64(1); j < info.GetNq(); j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + searchResultData[i].Topks[j-1]
		}
		ret.AllSearchCount += searchResultData[i].GetAllSearchCount()
		groupByValIterator[i] = typeutil.GetDataIterator(searchResultData[i].GetGroupByFieldValue())
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

		idSet := make(map[interface{}]struct{})
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
			key, err := getSearchResultDedupKey(searchResultData[sel], idx, id, hasElementIndices)
			if err != nil {
				return nil, err
			}
			if _, ok := idSet[key]; !ok {
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
					if hasElementIndices {
						ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].GetElementIndices().GetData()[idx])
					}
					gpFieldBuilder.Add(groupByVal)
					groupByValueMap[groupByVal] += 1
					idSet[key] = struct{}{}
					j++
				}
			} else {
				// skip entity with same pk
				filteredCount++
			}
			offsets[sel]++
		}
		ret.Topks = append(ret.Topks, j)

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, merr.WrapErrParameterInvalidMsg("search results exceed the maxOutputSize Limit %d", maxOutputSize)
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
