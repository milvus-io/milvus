package segments

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SearchReduce interface {
	ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) (*schemapb.SearchResultData, error)
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
	ret := &schemapb.SearchResultData{
		NumQueries: info.GetNq(),
		TopK:       info.GetTopK(),
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}

	// Check element-level consistency: all results must have ElementIndices or none
	hasElementIndices := searchResultData[0].ElementIndices != nil
	for i, data := range searchResultData {
		if (data.ElementIndices != nil) != hasElementIndices {
			return nil, fmt.Errorf("inconsistent element-level flag in search results: result[0] has ElementIndices=%v, but result[%d] has ElementIndices=%v",
				hasElementIndices, i, data.ElementIndices != nil)
		}
	}
	if hasElementIndices {
		ret.ElementIndices = &schemapb.LongArray{
			Data: make([]int64, 0),
		}
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
			if _, ok := idSet[id]; !ok {
				fieldsData := searchResultData[sel].FieldsData
				fieldIdxs := idxComputers[sel].Compute(idx)
				retSize += typeutil.AppendFieldData(ret.FieldsData, fieldsData, idx, fieldIdxs...)
				typeutil.AppendPKs(ret.Ids, id)
				ret.Scores = append(ret.Scores, score)
				if searchResultData[sel].ElementIndices != nil && ret.ElementIndices != nil {
					ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].ElementIndices.Data[idx])
				}
				idSet[id] = struct{}{}
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
	ret := &schemapb.SearchResultData{
		NumQueries: info.GetNq(),
		TopK:       info.GetTopK(),
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}

	// Check element-level consistency: all results must have ElementIndices or none
	hasElementIndices := searchResultData[0].ElementIndices != nil
	for i, data := range searchResultData {
		if (data.ElementIndices != nil) != hasElementIndices {
			return nil, fmt.Errorf("inconsistent element-level flag in search results: result[0] has ElementIndices=%v, but result[%d] has ElementIndices=%v",
				hasElementIndices, i, data.ElementIndices != nil)
		}
	}
	if hasElementIndices {
		ret.ElementIndices = &schemapb.LongArray{
			Data: make([]int64, 0),
		}
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
			if _, ok := idSet[id]; !ok {
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
					idSet[id] = struct{}{}
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

// buildOrderByIterators creates iterators for order_by fields from OrderByFieldValue
// Similar to how GroupByFieldValue is used, OrderByFieldValue contains the order_by field data
// even if it's not in output_fields
func buildOrderByIterators(
	searchResultData []*schemapb.SearchResultData,
	orderByFields []*planpb.OrderByField,
) [][]func(int) any {
	if len(orderByFields) == 0 {
		return nil
	}

	iterators := make([][]func(int) any, len(searchResultData))
	for i, srd := range searchResultData {
		iterators[i] = make([]func(int) any, len(orderByFields))
		// Use OrderByFieldValue (similar to GroupByFieldValue)
		// This contains the first order_by field's data (since OrderByFieldValue is a single FieldData)
		if srd.OrderByFieldValue != nil {
			// For now, use the first order_by field from OrderByFieldValue
			// If there are multiple order_by fields, only the first one is available in OrderByFieldValue
			if len(orderByFields) > 0 {
				// Check if the first order_by field matches the OrderByFieldValue
				// Since OrderByFieldValue is populated for the first order_by field
				iterators[i][0] = typeutil.GetDataIterator(srd.OrderByFieldValue)
				// For additional order_by fields, try to find them in FieldsData as fallback
				for fieldIdx := 1; fieldIdx < len(orderByFields); fieldIdx++ {
					fieldData := getOrderByFieldData(srd.FieldsData, orderByFields[fieldIdx].FieldId)
					if fieldData != nil {
						iterators[i][fieldIdx] = typeutil.GetDataIterator(fieldData)
					} else {
						// Field not found, return nil iterator
						iterators[i][fieldIdx] = func(int) any { return nil }
					}
				}
			}
		} else {
			// Fallback: try to find order_by fields in FieldsData (for backward compatibility)
			for fieldIdx, field := range orderByFields {
				fieldData := getOrderByFieldData(srd.FieldsData, field.FieldId)
				if fieldData != nil {
					iterators[i][fieldIdx] = typeutil.GetDataIterator(fieldData)
				} else {
					// Field not found, return nil iterator
					iterators[i][fieldIdx] = func(int) any { return nil }
				}
			}
		}
	}
	return iterators
}

// getOrderByFieldData finds the FieldData for a given order_by field_id (fallback helper)
func getOrderByFieldData(fieldsData []*schemapb.FieldData, fieldId int64) *schemapb.FieldData {
	for _, fieldData := range fieldsData {
		if fieldData != nil && fieldData.FieldId == fieldId {
			return fieldData
		}
	}
	return nil
}

type SearchOrderByReduce struct{}

func (sor *SearchOrderByReduce) ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) (*schemapb.SearchResultData, error) {
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

	orderByFields := info.GetOrderByFields()
	if len(orderByFields) == 0 {
		// Fallback to common reduce if no order_by fields
		scr := &SearchCommonReduce{}
		return scr.ReduceSearchResultData(ctx, searchResultData, info)
	}

	ret := &schemapb.SearchResultData{
		NumQueries: info.GetNq(),
		TopK:       info.GetTopK(),
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}

	// Check element-level consistency
	hasElementIndices := searchResultData[0].ElementIndices != nil
	for i, data := range searchResultData {
		if (data.ElementIndices != nil) != hasElementIndices {
			return nil, fmt.Errorf("inconsistent element-level flag in search results: result[0] has ElementIndices=%v, but result[%d] has ElementIndices=%v",
				hasElementIndices, i, data.ElementIndices != nil)
		}
	}
	if hasElementIndices {
		ret.ElementIndices = &schemapb.LongArray{
			Data: make([]int64, 0),
		}
	}

	resultOffsets := make([][]int64, len(searchResultData))
	for i := 0; i < len(searchResultData); i++ {
		resultOffsets[i] = make([]int64, len(searchResultData[i].Topks))
		for j := int64(1); j < info.GetNq(); j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + searchResultData[i].Topks[j-1]
		}
		ret.AllSearchCount += searchResultData[i].GetAllSearchCount()
	}

	// Build order_by iterators
	orderByIterators := buildOrderByIterators(searchResultData, orderByFields)

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
			sel := SelectSearchResultDataByOrderBy(searchResultData, resultOffsets, offsets, i, orderByFields, orderByIterators)
			if sel == -1 {
				break
			}
			idx := resultOffsets[sel][i] + offsets[sel]

			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			score := searchResultData[sel].Scores[idx]

			// remove duplicates
			if _, ok := idSet[id]; !ok {
				fieldsData := searchResultData[sel].FieldsData
				fieldIdxs := idxComputers[sel].Compute(idx)
				retSize += typeutil.AppendFieldData(ret.FieldsData, fieldsData, idx, fieldIdxs...)
				typeutil.AppendPKs(ret.Ids, id)
				ret.Scores = append(ret.Scores, score)
				if searchResultData[sel].ElementIndices != nil && ret.ElementIndices != nil {
					ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].ElementIndices.Data[idx])
				}
				idSet[id] = struct{}{}
				j++
			} else {
				// skip entity with same id
				skipDupCnt++
			}
			offsets[sel]++
		}

		ret.Topks = append(ret.Topks, j)

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	log.Debug("skip duplicated search result", zap.Int64("count", skipDupCnt))
	return ret, nil
}

type SearchGroupOrderByReduce struct{}

func (sgor *SearchGroupOrderByReduce) ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) (*schemapb.SearchResultData, error) {
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceSearchResultData")
	defer sp.End()
	log := log.Ctx(ctx)

	if len(searchResultData) == 0 {
		log.Debug("Shortcut return SearchGroupOrderByReduce, directly return empty result", zap.Any("result info", info))
		return &schemapb.SearchResultData{
			NumQueries: info.GetNq(),
			TopK:       info.GetTopK(),
			FieldsData: make([]*schemapb.FieldData, 0),
			Scores:     make([]float32, 0),
			Ids:        &schemapb.IDs{},
			Topks:      make([]int64, 0),
		}, nil
	}

	orderByFields := info.GetOrderByFields()
	if len(orderByFields) == 0 {
		// Fallback to group by reduce if no order_by fields
		sbr := &SearchGroupByReduce{}
		return sbr.ReduceSearchResultData(ctx, searchResultData, info)
	}

	ret := &schemapb.SearchResultData{
		NumQueries: info.GetNq(),
		TopK:       info.GetTopK(),
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}

	// Check element-level consistency
	hasElementIndices := searchResultData[0].ElementIndices != nil
	for i, data := range searchResultData {
		if (data.ElementIndices != nil) != hasElementIndices {
			return nil, fmt.Errorf("inconsistent element-level flag in search results: result[0] has ElementIndices=%v, but result[%d] has ElementIndices=%v",
				hasElementIndices, i, data.ElementIndices != nil)
		}
	}
	if hasElementIndices {
		ret.ElementIndices = &schemapb.LongArray{
			Data: make([]int64, 0),
		}
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

	// Build order_by iterators
	orderByIterators := buildOrderByIterators(searchResultData, orderByFields)

	// Map to cache each group's first item's order_by values
	// Key: (dataIndex, groupByValue), Value: first item's order_by values
	type groupKey struct {
		dataIndex  int
		groupByVal interface{}
	}
	groupOrderByMap := make(map[groupKey][]any)

	// Helper to get first item's order_by values for a group
	getFirstItemOrderByValues := func(dataIndex int, groupByVal interface{}, startIdx int64, endIdx int64) []any {
		key := groupKey{dataIndex: dataIndex, groupByVal: groupByVal}
		if cached, ok := groupOrderByMap[key]; ok {
			return cached
		}

		// Find first item in this group
		var firstIdx int64 = -1
		for idx := startIdx; idx < endIdx; idx++ {
			if groupByValIterator[dataIndex](int(idx)) == groupByVal {
				firstIdx = idx
				break
			}
		}

		if firstIdx == -1 {
			return nil
		}

		// Get order_by values for first item
		firstItemOrderByVals := make([]any, len(orderByFields))
		for fieldIdx := range orderByFields {
			if fieldIdx < len(orderByIterators[dataIndex]) {
				firstItemOrderByVals[fieldIdx] = orderByIterators[dataIndex][fieldIdx](int(firstIdx))
			}
		}

		groupOrderByMap[key] = firstItemOrderByVals
		return firstItemOrderByVals
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

		// Map to track groups and their first item's order_by values for selection
		groupFirstItemOrderByMap := make(map[interface{}][]any)

		var j int64
		for j = 0; j < groupBound; {
			// Select based on group's first item's order_by values
			sel := -1
			var selGroupByVal interface{}
			var selOrderByVals []any

			for dataIdx, offset := range offsets {
				if offset >= searchResultData[dataIdx].Topks[i] {
					continue
				}

				idx := resultOffsets[dataIdx][i] + offset
				groupByVal := groupByValIterator[dataIdx](int(idx))

				// Get or cache this group's first item's order_by values
				var orderByVals []any
				if cached, ok := groupFirstItemOrderByMap[groupByVal]; ok {
					orderByVals = cached
				} else {
					startIdx := resultOffsets[dataIdx][i]
					endIdx := startIdx + searchResultData[dataIdx].Topks[i]
					orderByVals = getFirstItemOrderByValues(dataIdx, groupByVal, startIdx, endIdx)
					groupFirstItemOrderByMap[groupByVal] = orderByVals
				}

				if sel == -1 {
					sel = dataIdx
					selGroupByVal = groupByVal
					selOrderByVals = orderByVals
					continue
				}

				// Compare groups by their first item's order_by values
				cmp := compareOrderByValues(selOrderByVals, orderByVals, orderByFields)
				if cmp > 0 {
					// Current group is better
					sel = dataIdx
					selGroupByVal = groupByVal
					selOrderByVals = orderByVals
				} else if cmp == 0 {
					// Equal order_by values, use distance as tie-breaker
					selIdx := resultOffsets[sel][i] + offsets[sel]
					currIdx := resultOffsets[dataIdx][i] + offset
					selDistance := searchResultData[sel].Scores[selIdx]
					currDistance := searchResultData[dataIdx].Scores[currIdx]
					if currDistance > selDistance {
						sel = dataIdx
						selGroupByVal = groupByVal
						selOrderByVals = orderByVals
					}
				}
			}

			if sel == -1 {
				break
			}

			idx := resultOffsets[sel][i] + offsets[sel]
			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			groupByVal := selGroupByVal
			score := searchResultData[sel].Scores[idx]

			if _, ok := idSet[id]; !ok {
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
					idSet[id] = struct{}{}
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
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	ret.GroupByFieldValue = gpFieldBuilder.Build()
	if float64(filteredCount) >= 0.3*float64(groupBound) {
		log.Warn("GroupOrderBy reduce filtered too many results, "+
			"this may influence the final result seriously",
			zap.Int64("filteredCount", filteredCount),
			zap.Int64("groupBound", groupBound))
	}
	log.Debug("skip duplicated search result", zap.Int64("count", filteredCount))
	return ret, nil
}

// compareOrderByValues compares two order_by value arrays
// Returns: -1 if lhs < rhs, 0 if lhs == rhs, 1 if lhs > rhs
func compareOrderByValues(
	lhsVals []any,
	rhsVals []any,
	orderByFields []*planpb.OrderByField,
) int {
	for fieldIdx, field := range orderByFields {
		if fieldIdx >= len(lhsVals) || fieldIdx >= len(rhsVals) {
			break
		}

		lhsVal := lhsVals[fieldIdx]
		rhsVal := rhsVals[fieldIdx]

		// Handle null values
		if lhsVal == nil && rhsVal == nil {
			continue // Both null, compare next field
		}
		if lhsVal == nil {
			if field.Ascending {
				return -1 // null < non-null for ascending
			}
			return 1 // null > non-null for descending
		}
		if rhsVal == nil {
			if field.Ascending {
				return 1 // non-null > null for ascending
			}
			return -1 // non-null < null for descending
		}

		// Compare values
		cmp := compareValues(lhsVal, rhsVal)
		if cmp != 0 {
			if field.Ascending {
				return cmp
			}
			return -cmp // Reverse for descending
		}
		// Equal, continue to next field
	}
	return 0 // All fields equal
}

func InitSearchReducer(info *reduce.ResultInfo) SearchReduce {
	hasGroupBy := info.GetGroupByFieldId() > 0
	hasOrderBy := len(info.GetOrderByFields()) > 0

	if hasGroupBy && hasOrderBy {
		return &SearchGroupOrderByReduce{}
	} else if hasOrderBy {
		return &SearchOrderByReduce{}
	} else if hasGroupBy {
		return &SearchGroupByReduce{}
	}
	return &SearchCommonReduce{}
}
