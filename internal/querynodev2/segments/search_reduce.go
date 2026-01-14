package segments

import (
	"context"
	"fmt"
	"sort"

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

// buildOrderByIterators creates iterators for order_by fields from OrderByFieldValue.
// OrderByFieldValue is a repeated field containing data for ALL order_by fields.
// Each element in the slice corresponds to one order_by field in order.
// This is separate from FieldsData (output_fields) and GroupByFieldValue.
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
		orderByFieldValues := srd.GetOrderByFieldValues()

		// Build iterator for each order_by field from the repeated OrderByFieldValue
		for fieldIdx := range orderByFields {
			if fieldIdx < len(orderByFieldValues) && orderByFieldValues[fieldIdx] != nil {
				iterators[i][fieldIdx] = typeutil.GetDataIterator(orderByFieldValues[fieldIdx])
			} else {
				// Field data not present - this indicates a data consistency issue
				// Log warning and return nil iterator (comparison will treat as null)
				log.Warn("OrderByFieldValue missing for segment, ordering may be affected",
					zap.Int("segmentIdx", i),
					zap.Int("fieldIdx", fieldIdx),
					zap.Int("expectedFields", len(orderByFields)),
					zap.Int("actualFields", len(orderByFieldValues)))
				iterators[i][fieldIdx] = func(int) any { return nil }
			}
		}
	}
	return iterators
}

type groupReduceInfo struct {
	subSearchIdx int
	resultIdx    int64
	score        float32
	id           interface{}
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

	// Create order_by field builders for each order_by field
	// Find the first non-empty OrderByFieldValue across all segments to get type information
	var orderByFieldBuilders []*typeutil.FieldDataBuilder
	if len(orderByFields) > 0 {
		var orderByFieldValues []*schemapb.FieldData
		for _, srd := range searchResultData {
			if vals := srd.GetOrderByFieldValues(); len(vals) > 0 {
				orderByFieldValues = vals
				break
			}
		}
		if len(orderByFieldValues) > 0 {
			orderByFieldBuilders = make([]*typeutil.FieldDataBuilder, len(orderByFieldValues))
			for fieldIdx, fieldData := range orderByFieldValues {
				if fieldData != nil {
					builder, err := typeutil.NewFieldDataBuilder(fieldData.GetType(), true, int(info.GetTopK()*info.GetNq()))
					if err != nil {
						log.Warn("failed to construct order by field data builder", zap.Error(err), zap.Int("fieldIdx", fieldIdx))
						continue
					}
					builder.SetFieldId(fieldData.GetFieldId())
					orderByFieldBuilders[fieldIdx] = builder
				}
			}
		}
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

				// Add order_by field values
				if len(orderByFieldBuilders) > 0 && orderByIterators != nil && sel < len(orderByIterators) {
					iterators := orderByIterators[sel]
					for fieldIdx, builder := range orderByFieldBuilders {
						if builder != nil && fieldIdx < len(iterators) && iterators[fieldIdx] != nil {
							orderByVal := iterators[fieldIdx](int(idx))
							builder.Add(orderByVal)
						}
					}
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

	// Set OrderByFieldValue in result
	if len(orderByFieldBuilders) > 0 {
		ret.OrderByFieldValues = make([]*schemapb.FieldData, len(orderByFieldBuilders))
		for fieldIdx, builder := range orderByFieldBuilders {
			if builder != nil {
				ret.OrderByFieldValues[fieldIdx] = builder.Build()
			}
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

	// Create order_by field builders for each order_by field
	// Find the first non-empty OrderByFieldValue across all segments to get type information
	var orderByFieldBuilders []*typeutil.FieldDataBuilder
	if len(orderByFields) > 0 {
		var orderByFieldValues []*schemapb.FieldData
		for _, srd := range searchResultData {
			if vals := srd.GetOrderByFieldValues(); len(vals) > 0 {
				orderByFieldValues = vals
				break
			}
		}
		if len(orderByFieldValues) > 0 {
			orderByFieldBuilders = make([]*typeutil.FieldDataBuilder, len(orderByFieldValues))
			for fieldIdx, fieldData := range orderByFieldValues {
				if fieldData != nil {
					builder, err := typeutil.NewFieldDataBuilder(fieldData.GetType(), true, int(info.GetTopK()*info.GetNq()*info.GetGroupSize()))
					if err != nil {
						log.Warn("failed to construct order by field data builder", zap.Error(err), zap.Int("fieldIdx", fieldIdx))
						continue
					}
					builder.SetFieldId(fieldData.GetFieldId())
					orderByFieldBuilders[fieldIdx] = builder
				}
			}
		}
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
		groupByValueMap := make(map[interface{}][]*groupReduceInfo)
		groupOrder := make([]interface{}, 0, int(info.GetTopK()))

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
				groupCount := int64(len(groupByValueMap[groupByVal]))
				if groupCount == 0 && int64(len(groupByValueMap)) >= info.GetTopK() {
					// exceed the limit for group count, filter this entity
					filteredCount++
				} else if groupCount >= groupSize {
					// exceed the limit for each group, filter this entity
					filteredCount++
				} else {
					if groupCount == 0 {
						groupOrder = append(groupOrder, groupByVal)
					}
					groupByValueMap[groupByVal] = append(groupByValueMap[groupByVal], &groupReduceInfo{
						subSearchIdx: sel,
						resultIdx:    idx,
						score:        score,
						id:           id,
					})
					idSet[id] = struct{}{}
					j++
				}
			} else {
				// skip entity with same pk
				filteredCount++
			}
			offsets[sel]++
		}

		type groupOrderByInfo struct {
			groupVal    interface{}
			orderByVals []any
		}
		groupInfos := make([]groupOrderByInfo, 0, len(groupByValueMap))
		for _, groupVal := range groupOrder {
			entries := groupByValueMap[groupVal]
			if len(entries) == 0 {
				continue
			}
			first := entries[0]
			orderByVals := make([]any, len(orderByFields))
			for fieldIdx := range orderByFields {
				if fieldIdx < len(orderByIterators[first.subSearchIdx]) {
					orderByVals[fieldIdx] = orderByIterators[first.subSearchIdx][fieldIdx](int(first.resultIdx))
				}
			}
			groupInfos = append(groupInfos, groupOrderByInfo{
				groupVal:    groupVal,
				orderByVals: orderByVals,
			})
		}
		sort.SliceStable(groupInfos, func(i, j int) bool {
			return compareOrderByValues(groupInfos[i].orderByVals, groupInfos[j].orderByVals, orderByFields) < 0
		})

		var outputCount int64
		for _, group := range groupInfos {
			groupEntities := groupByValueMap[group.groupVal]
			for _, groupEntity := range groupEntities {
				fieldsData := searchResultData[groupEntity.subSearchIdx].FieldsData
				fieldIdxs := idxComputers[groupEntity.subSearchIdx].Compute(groupEntity.resultIdx)
				retSize += typeutil.AppendFieldData(ret.FieldsData, fieldsData, groupEntity.resultIdx, fieldIdxs...)
				typeutil.AppendPKs(ret.Ids, groupEntity.id)
				ret.Scores = append(ret.Scores, groupEntity.score)
				if searchResultData[groupEntity.subSearchIdx].ElementIndices != nil && ret.ElementIndices != nil {
					ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[groupEntity.subSearchIdx].ElementIndices.Data[groupEntity.resultIdx])
				}
				gpFieldBuilder.Add(group.groupVal)

				// Add order_by field values
				if len(orderByFieldBuilders) > 0 && orderByIterators != nil && groupEntity.subSearchIdx < len(orderByIterators) {
					iterators := orderByIterators[groupEntity.subSearchIdx]
					for fieldIdx, builder := range orderByFieldBuilders {
						if builder != nil && fieldIdx < len(iterators) && iterators[fieldIdx] != nil {
							orderByVal := iterators[fieldIdx](int(groupEntity.resultIdx))
							builder.Add(orderByVal)
						}
					}
				}

				outputCount++
			}
		}
		ret.Topks = append(ret.Topks, outputCount)

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	ret.GroupByFieldValue = gpFieldBuilder.Build()

	// Set OrderByFieldValue in result
	if len(orderByFieldBuilders) > 0 {
		ret.OrderByFieldValues = make([]*schemapb.FieldData, len(orderByFieldBuilders))
		for fieldIdx, builder := range orderByFieldBuilders {
			if builder != nil {
				ret.OrderByFieldValues[fieldIdx] = builder.Build()
			}
		}
	}

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
