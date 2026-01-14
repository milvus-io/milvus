package proxy

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// reduceSearchResult dispatches to the appropriate reduce function based on reduceInfo
func reduceSearchResult(ctx context.Context, subSearchResultData []*schemapb.SearchResultData, reduceInfo *reduce.ResultInfo) (*milvuspb.SearchResults, error) {
	hasGroupBy := reduceInfo.GetGroupByFieldId() > 0
	hasOrderBy := len(reduceInfo.GetOrderByFields()) > 0
	isAdvance := reduceInfo.GetIsAdvance()

	// Handle advance search with group by (special case)
	if hasGroupBy && isAdvance {
		// for hybrid search group by, we cannot reduce result for results from one single search path,
		// because the final score has not been accumulated, also, offset cannot be applied
		return reduceAdvanceGroupBy(ctx,
			subSearchResultData, reduceInfo.GetNq(), reduceInfo.GetTopK(), reduceInfo.GetPkType(), reduceInfo.GetMetricType())
	}

	// Dispatch based on combination of groupBy and orderBy
	if hasGroupBy && hasOrderBy {
		return reduceSearchResultDataWithGroupOrderBy(ctx,
			subSearchResultData,
			reduceInfo.GetNq(),
			reduceInfo.GetTopK(),
			reduceInfo.GetMetricType(),
			reduceInfo.GetPkType(),
			reduceInfo.GetOffset(),
			reduceInfo.GetGroupSize(),
			reduceInfo.GetOrderByFields())
	} else if hasOrderBy {
		return reduceSearchResultDataWithOrderBy(ctx,
			subSearchResultData,
			reduceInfo.GetNq(),
			reduceInfo.GetTopK(),
			reduceInfo.GetMetricType(),
			reduceInfo.GetPkType(),
			reduceInfo.GetOffset(),
			reduceInfo.GetOrderByFields())
	} else if hasGroupBy {
		return reduceSearchResultDataWithGroupBy(ctx,
			subSearchResultData,
			reduceInfo.GetNq(),
			reduceInfo.GetTopK(),
			reduceInfo.GetMetricType(),
			reduceInfo.GetPkType(),
			reduceInfo.GetOffset(),
			reduceInfo.GetGroupSize())
	}

	// Default: no group by, no order by
	return reduceSearchResultDataNoGroupBy(ctx,
		subSearchResultData,
		reduceInfo.GetNq(),
		reduceInfo.GetTopK(),
		reduceInfo.GetMetricType(),
		reduceInfo.GetPkType(),
		reduceInfo.GetOffset())
}

func checkResultDatas(ctx context.Context, subSearchResultData []*schemapb.SearchResultData,
	nq int64, topK int64,
) (int64, int, error) {
	var allSearchCount int64
	var hitNum int
	for i, sData := range subSearchResultData {
		pkLength := typeutil.GetSizeOfIDs(sData.GetIds())
		log.Ctx(ctx).Debug("subSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Int("length of pks", pkLength),
			zap.Int("length of FieldsData", len(sData.FieldsData)))
		allSearchCount += sData.GetAllSearchCount()
		hitNum += pkLength
		if err := checkSearchResultData(sData, nq, topK, pkLength); err != nil {
			log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
			return allSearchCount, hitNum, err
		}
	}
	return allSearchCount, hitNum, nil
}

func reduceAdvanceGroupBy(ctx context.Context, subSearchResultData []*schemapb.SearchResultData,
	nq int64, topK int64, pkType schemapb.DataType, metricType string,
) (*milvuspb.SearchResults, error) {
	log.Ctx(ctx).Debug("reduceAdvanceGroupBY", zap.Int("len(subSearchResultData)", len(subSearchResultData)), zap.Int64("nq", nq))
	// for advance group by, offset is not applied, so just return when there's only one channel
	if len(subSearchResultData) == 1 {
		return &milvuspb.SearchResults{
			Status:  merr.Success(),
			Results: subSearchResultData[0],
		}, nil
	}

	ret := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topK,
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}

	var limit int64
	if allSearchCount, hitNum, err := checkResultDatas(ctx, subSearchResultData, nq, topK); err != nil {
		log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
		return ret, err
	} else {
		ret.GetResults().AllSearchCount = allSearchCount
		limit = int64(hitNum)
		// Find the first non-empty FieldsData as template
		for _, result := range subSearchResultData {
			if len(result.GetFieldsData()) > 0 {
				ret.GetResults().FieldsData = typeutil.PrepareResultFieldData(result.GetFieldsData(), limit)
				break
			}
		}
	}

	if err := setupIdListForSearchResult(ret, pkType, limit); err != nil {
		return ret, nil
	}

	var (
		subSearchNum = len(subSearchResultData)
		// for results of each subSearchResultData, storing the start offset of each query of nq queries
		subSearchNqOffset = make([][]int64, subSearchNum)
	)
	for i := 0; i < subSearchNum; i++ {
		subSearchNqOffset[i] = make([]int64, subSearchResultData[i].GetNumQueries())
		for j := int64(1); j < nq; j++ {
			subSearchNqOffset[i][j] = subSearchNqOffset[i][j-1] + subSearchResultData[i].Topks[j-1]
		}
	}

	gpFieldBuilder, err := typeutil.NewFieldDataBuilder(subSearchResultData[0].GetGroupByFieldValue().GetType(), true, int(limit))
	if err != nil {
		return ret, merr.WrapErrServiceInternal("failed to construct group by field data builder, this is abnormal as segcore should always set up a group by field, no matter data status, check code on qn", err.Error())
	}
	// reducing nq * topk results
	for nqIdx := int64(0); nqIdx < nq; nqIdx++ {
		dataCount := int64(0)
		for subIdx := 0; subIdx < subSearchNum; subIdx += 1 {
			subData := subSearchResultData[subIdx]
			subPks := subData.GetIds()
			subScores := subData.GetScores()
			subGroupByVals := subData.GetGroupByFieldValue()

			nqTopK := subData.Topks[nqIdx]
			groupByValIterator := typeutil.GetDataIterator(subGroupByVals)

			for i := int64(0); i < nqTopK; i++ {
				innerIdx := subSearchNqOffset[subIdx][nqIdx] + i
				pk := typeutil.GetPK(subPks, innerIdx)
				score := subScores[innerIdx]
				groupByVal := groupByValIterator(int(innerIdx))
				gpFieldBuilder.Add(groupByVal)
				typeutil.AppendPKs(ret.Results.Ids, pk)
				ret.Results.Scores = append(ret.Results.Scores, score)

				// Handle ElementIndices if present
				if subData.ElementIndices != nil {
					if ret.Results.ElementIndices == nil {
						ret.Results.ElementIndices = &schemapb.LongArray{
							Data: make([]int64, 0, limit),
						}
					}
					elemIdx := subData.ElementIndices.GetData()[innerIdx]
					ret.Results.ElementIndices.Data = append(ret.Results.ElementIndices.Data, elemIdx)
				}

				dataCount += 1
			}
		}
		ret.Results.Topks = append(ret.Results.Topks, dataCount)
	}

	ret.Results.GroupByFieldValue = gpFieldBuilder.Build()
	ret.Results.TopK = topK // realTopK is the topK of the nq-th query
	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

type MilvusPKType interface{}

type groupReduceInfo struct {
	subSearchIdx int
	resultIdx    int64
	score        float32
	id           MilvusPKType
}

func reduceSearchResultDataWithGroupBy(ctx context.Context, subSearchResultData []*schemapb.SearchResultData,
	nq int64, topk int64, metricType string,
	pkType schemapb.DataType,
	offset int64,
	groupSize int64,
) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("reduceSearchResultData")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	limit := topk - offset
	log.Ctx(ctx).Debug("reduceSearchResultData",
		zap.Int("len(subSearchResultData)", len(subSearchResultData)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit),
		zap.String("metricType", metricType))

	ret := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: []*schemapb.FieldData{},
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}
	outputBound := groupSize * limit
	if err := setupIdListForSearchResult(ret, pkType, outputBound); err != nil {
		return ret, err
	}

	if allSearchCount, _, err := checkResultDatas(ctx, subSearchResultData, nq, topk); err != nil {
		log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
		return ret, err
	} else {
		ret.GetResults().AllSearchCount = allSearchCount
	}

	// Find the first non-empty FieldsData as template
	for _, result := range subSearchResultData {
		if len(result.GetFieldsData()) > 0 {
			ret.GetResults().FieldsData = typeutil.PrepareResultFieldData(result.GetFieldsData(), limit)
			break
		}
	}

	var (
		subSearchNum = len(subSearchResultData)
		// for results of each subSearchResultData, storing the start offset of each query of nq queries
		subSearchNqOffset                 = make([][]int64, subSearchNum)
		totalResCount               int64 = 0
		subSearchGroupByValIterator       = make([]func(int) any, subSearchNum)
	)
	for i := 0; i < subSearchNum; i++ {
		subSearchNqOffset[i] = make([]int64, subSearchResultData[i].GetNumQueries())
		for j := int64(1); j < nq; j++ {
			subSearchNqOffset[i][j] = subSearchNqOffset[i][j-1] + subSearchResultData[i].Topks[j-1]
		}
		totalResCount += subSearchNqOffset[i][nq-1]
		subSearchGroupByValIterator[i] = typeutil.GetDataIterator(subSearchResultData[i].GetGroupByFieldValue())
	}

	gpFieldBuilder, err := typeutil.NewFieldDataBuilder(subSearchResultData[0].GetGroupByFieldValue().GetType(), true, int(limit))
	if err != nil {
		return ret, merr.WrapErrServiceInternal("failed to construct group by field data builder, this is abnormal as segcore should always set up a group by field, no matter data status, check code on qn", err.Error())
	}

	idxComputers := make([]*typeutil.FieldDataIdxComputer, subSearchNum)
	for i, srd := range subSearchResultData {
		idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.FieldsData)
	}

	var realTopK int64 = -1
	var retSize int64

	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	selectionBound := groupSize * topk
	// reducing nq * topk results
	for i := int64(0); i < nq; i++ {
		var (
			// cursor of current data of each subSearch for merging the j-th data of TopK.
			// sum(cursors) == j
			cursors = make([]int64, subSearchNum)

			j              int64
			groupByValMap  = make(map[interface{}][]*groupReduceInfo)
			skipOffsetMap  = make(map[interface{}]bool)
			groupByValList = make([]interface{}, limit)
			groupByValIdx  = 0
		)

		for j = 0; j < selectionBound; {
			subSearchIdx, resultDataIdx := selectHighestScoreIndex(ctx, subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}
			subSearchRes := subSearchResultData[subSearchIdx]

			id := typeutil.GetPK(subSearchRes.GetIds(), resultDataIdx)
			score := subSearchRes.GetScores()[resultDataIdx]
			groupByVal := subSearchGroupByValIterator[subSearchIdx](int(resultDataIdx))

			if int64(len(skipOffsetMap)) < offset || skipOffsetMap[groupByVal] {
				skipOffsetMap[groupByVal] = true
				// the first offset's group will be ignored
			} else if len(groupByValMap[groupByVal]) == 0 && int64(len(groupByValMap)) >= limit {
				// skip when groupbyMap has been full and found new groupByVal
			} else if int64(len(groupByValMap[groupByVal])) >= groupSize {
				// skip when target group has been full
			} else {
				if len(groupByValMap[groupByVal]) == 0 {
					groupByValList[groupByValIdx] = groupByVal
					groupByValIdx++
				}
				groupByValMap[groupByVal] = append(groupByValMap[groupByVal], &groupReduceInfo{
					subSearchIdx: subSearchIdx,
					resultIdx:    resultDataIdx, id: id, score: score,
				})
				j++
			}

			cursors[subSearchIdx]++
		}

		// assemble all eligible values in group
		// values in groupByValList is sorted by the highest score in each group
		for _, groupVal := range groupByValList {
			groupEntities := groupByValMap[groupVal]
			for _, groupEntity := range groupEntities {
				subResData := subSearchResultData[groupEntity.subSearchIdx]
				if len(ret.Results.FieldsData) > 0 {
					fieldIdxs := idxComputers[groupEntity.subSearchIdx].Compute(groupEntity.resultIdx)
					retSize += typeutil.AppendFieldData(ret.Results.FieldsData, subResData.FieldsData, groupEntity.resultIdx, fieldIdxs...)
				}
				typeutil.AppendPKs(ret.Results.Ids, groupEntity.id)
				ret.Results.Scores = append(ret.Results.Scores, groupEntity.score)

				// Handle ElementIndices if present
				if subResData.ElementIndices != nil {
					if ret.Results.ElementIndices == nil {
						ret.Results.ElementIndices = &schemapb.LongArray{
							Data: make([]int64, 0, limit),
						}
					}
					elemIdx := subResData.ElementIndices.GetData()[groupEntity.resultIdx]
					ret.Results.ElementIndices.Data = append(ret.Results.ElementIndices.Data, elemIdx)
				}

				gpFieldBuilder.Add(groupVal)
			}
		}

		if realTopK != -1 && realTopK != j {
			log.Ctx(ctx).Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
		}
		realTopK = j
		ret.Results.Topks = append(ret.Results.Topks, realTopK)
		ret.Results.GroupByFieldValue = gpFieldBuilder.Build()

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	ret.Results.TopK = realTopK // realTopK is the topK of the nq-th query
	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

func reduceSearchResultDataNoGroupBy(ctx context.Context, subSearchResultData []*schemapb.SearchResultData, nq int64, topk int64, metricType string, pkType schemapb.DataType, offset int64) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("reduceSearchResultData")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	limit := topk - offset
	log.Ctx(ctx).Debug("reduceSearchResultData",
		zap.Int("len(subSearchResultData)", len(subSearchResultData)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit),
		zap.String("metricType", metricType))

	ret := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: []*schemapb.FieldData{},
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}

	if err := setupIdListForSearchResult(ret, pkType, limit); err != nil {
		return ret, nil
	}

	if allSearchCount, _, err := checkResultDatas(ctx, subSearchResultData, nq, topk); err != nil {
		log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
		return ret, err
	} else {
		ret.GetResults().AllSearchCount = allSearchCount
	}

	// Find the first non-empty FieldsData as template
	for _, result := range subSearchResultData {
		if len(result.GetFieldsData()) > 0 {
			ret.GetResults().FieldsData = typeutil.PrepareResultFieldData(result.GetFieldsData(), limit)
			break
		}
	}

	subSearchNum := len(subSearchResultData)
	if subSearchNum == 1 && offset == 0 {
		// sorting is not needed if there is only one shard and no offset, assigning the result directly.
		//  we still need to adjust the scores later.
		ret.Results = subSearchResultData[0]
		// realTopK is the topK of the nq-th query, it is used in proxy but not handled by delegator.
		topks := subSearchResultData[0].Topks
		if len(topks) > 0 {
			ret.Results.TopK = topks[len(topks)-1]
		}
	} else {
		var realTopK int64 = -1
		var retSize int64

		// for results of each subSearchResultData, storing the start offset of each query of nq queries
		subSearchNqOffset := make([][]int64, subSearchNum)
		for i := 0; i < subSearchNum; i++ {
			subSearchNqOffset[i] = make([]int64, subSearchResultData[i].GetNumQueries())
			for j := int64(1); j < nq; j++ {
				subSearchNqOffset[i][j] = subSearchNqOffset[i][j-1] + subSearchResultData[i].Topks[j-1]
			}
		}

		idxComputers := make([]*typeutil.FieldDataIdxComputer, subSearchNum)
		for i, srd := range subSearchResultData {
			idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.FieldsData)
		}

		maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
		// reducing nq * topk results
		for i := int64(0); i < nq; i++ {
			var (
				// cursor of current data of each subSearch for merging the j-th data of TopK.
				// sum(cursors) == j
				cursors = make([]int64, subSearchNum)
				j       int64
			)

			// skip offset results
			for k := int64(0); k < offset; k++ {
				subSearchIdx, _ := selectHighestScoreIndex(ctx, subSearchResultData, subSearchNqOffset, cursors, i)
				if subSearchIdx == -1 {
					break
				}

				cursors[subSearchIdx]++
			}

			// keep limit results
			for j = 0; j < limit; j++ {
				// From all the sub-query result sets of the i-th query vector,
				//   find the sub-query result set index of the score j-th data,
				//   and the index of the data in schemapb.SearchResultData
				subSearchIdx, resultDataIdx := selectHighestScoreIndex(ctx, subSearchResultData, subSearchNqOffset, cursors, i)
				if subSearchIdx == -1 {
					break
				}
				score := subSearchResultData[subSearchIdx].Scores[resultDataIdx]

				if len(ret.Results.FieldsData) > 0 {
					fieldsData := subSearchResultData[subSearchIdx].FieldsData
					fieldIdxs := idxComputers[subSearchIdx].Compute(resultDataIdx)
					retSize += typeutil.AppendFieldData(ret.Results.FieldsData, fieldsData, resultDataIdx, fieldIdxs...)
				}
				typeutil.CopyPk(ret.Results.Ids, subSearchResultData[subSearchIdx].GetIds(), int(resultDataIdx))
				ret.Results.Scores = append(ret.Results.Scores, score)

				// Handle ElementIndices if present
				if subSearchResultData[subSearchIdx].ElementIndices != nil {
					if ret.Results.ElementIndices == nil {
						ret.Results.ElementIndices = &schemapb.LongArray{
							Data: make([]int64, 0, limit),
						}
					}
					elemIdx := subSearchResultData[subSearchIdx].ElementIndices.GetData()[resultDataIdx]
					ret.Results.ElementIndices.Data = append(ret.Results.ElementIndices.Data, elemIdx)
				}

				cursors[subSearchIdx]++
			}
			if realTopK != -1 && realTopK != j {
				log.Ctx(ctx).Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
				// return nil, errors.New("the length (topk) between all result of query is different")
			}
			realTopK = j
			ret.Results.Topks = append(ret.Results.Topks, realTopK)

			// limit search result to avoid oom
			if retSize > maxOutputSize {
				return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
			}
		}
		ret.Results.TopK = realTopK // realTopK is the topK of the nq-th query
	}

	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

// reduceSearchResultDataWithOrderBy reduces search results with order_by (no group_by)
func reduceSearchResultDataWithOrderBy(ctx context.Context, subSearchResultData []*schemapb.SearchResultData,
	nq int64, topk int64, metricType string, pkType schemapb.DataType, offset int64,
	orderByFields []*planpb.OrderByField,
) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("reduceSearchResultDataWithOrderBy")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	limit := topk - offset
	log.Ctx(ctx).Debug("reduceSearchResultDataWithOrderBy",
		zap.Int("len(subSearchResultData)", len(subSearchResultData)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit),
		zap.String("metricType", metricType))

	ret := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: []*schemapb.FieldData{},
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}

	if err := setupIdListForSearchResult(ret, pkType, limit); err != nil {
		return ret, nil
	}

	if allSearchCount, _, err := checkResultDatas(ctx, subSearchResultData, nq, topk); err != nil {
		log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
		return ret, err
	} else {
		ret.GetResults().AllSearchCount = allSearchCount
	}

	// Find the first non-empty FieldsData as template
	for _, result := range subSearchResultData {
		if len(result.GetFieldsData()) > 0 {
			ret.GetResults().FieldsData = typeutil.PrepareResultFieldData(result.GetFieldsData(), limit)
			break
		}
	}

	subSearchNum := len(subSearchResultData)
	if subSearchNum == 1 && offset == 0 && len(orderByFields) == 0 {
		// sorting is not needed if there is only one shard, no offset, and no order_by
		ret.Results = subSearchResultData[0]
		topks := subSearchResultData[0].Topks
		if len(topks) > 0 {
			ret.Results.TopK = topks[len(topks)-1]
		}
	} else {
		var realTopK int64 = -1
		var retSize int64

		// for results of each subSearchResultData, storing the start offset of each query of nq queries
		subSearchNqOffset := make([][]int64, subSearchNum)
		for i := 0; i < subSearchNum; i++ {
			subSearchNqOffset[i] = make([]int64, subSearchResultData[i].GetNumQueries())
			for j := int64(1); j < nq; j++ {
				subSearchNqOffset[i][j] = subSearchNqOffset[i][j-1] + subSearchResultData[i].Topks[j-1]
			}
		}

		// Build order_by iterators
		orderByIterators := buildOrderByIterators(subSearchResultData, orderByFields)

		// Create order_by field builders for each order_by field
		// Find the first non-empty OrderByFieldValue across all shards to get type information
		var orderByFieldBuilders []*typeutil.FieldDataBuilder
		if len(orderByFields) > 0 && len(subSearchResultData) > 0 {
			var orderByFieldValues []*schemapb.FieldData
			for _, srd := range subSearchResultData {
				if vals := srd.GetOrderByFieldValues(); len(vals) > 0 {
					orderByFieldValues = vals
					break
				}
			}
			if len(orderByFieldValues) > 0 {
				orderByFieldBuilders = make([]*typeutil.FieldDataBuilder, len(orderByFieldValues))
				for fieldIdx, fieldData := range orderByFieldValues {
					if fieldData != nil {
						builder, err := typeutil.NewFieldDataBuilder(fieldData.GetType(), true, int(limit*nq))
						if err != nil {
							log.Ctx(ctx).Warn("failed to construct order by field data builder", zap.Error(err), zap.Int("fieldIdx", fieldIdx))
							continue
						}
						builder.SetFieldId(fieldData.GetFieldId())
						orderByFieldBuilders[fieldIdx] = builder
					}
				}
			}
		}

		idxComputers := make([]*typeutil.FieldDataIdxComputer, subSearchNum)
		for i, srd := range subSearchResultData {
			idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.FieldsData)
		}

		maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
		// reducing nq * topk results
		for i := int64(0); i < nq; i++ {
			var (
				cursors = make([]int64, subSearchNum)
				j       int64
			)

			// skip offset results
			for k := int64(0); k < offset; k++ {
				subSearchIdx, _ := selectIndexByOrderBy(ctx, subSearchResultData, subSearchNqOffset, cursors, i, orderByFields, orderByIterators)
				if subSearchIdx == -1 {
					break
				}
				cursors[subSearchIdx]++
			}

			// keep limit results
			for j = 0; j < limit; j++ {
				subSearchIdx, resultDataIdx := selectIndexByOrderBy(ctx, subSearchResultData, subSearchNqOffset, cursors, i, orderByFields, orderByIterators)
				if subSearchIdx == -1 {
					break
				}
				score := subSearchResultData[subSearchIdx].Scores[resultDataIdx]

				if len(ret.Results.FieldsData) > 0 {
					fieldsData := subSearchResultData[subSearchIdx].FieldsData
					fieldIdxs := idxComputers[subSearchIdx].Compute(resultDataIdx)
					retSize += typeutil.AppendFieldData(ret.Results.FieldsData, fieldsData, resultDataIdx, fieldIdxs...)
				}
				typeutil.CopyPk(ret.Results.Ids, subSearchResultData[subSearchIdx].GetIds(), int(resultDataIdx))
				ret.Results.Scores = append(ret.Results.Scores, score)

				// Handle ElementIndices if present
				if subSearchResultData[subSearchIdx].ElementIndices != nil {
					if ret.Results.ElementIndices == nil {
						ret.Results.ElementIndices = &schemapb.LongArray{
							Data: make([]int64, 0, limit),
						}
					}
					elemIdx := subSearchResultData[subSearchIdx].ElementIndices.GetData()[resultDataIdx]
					ret.Results.ElementIndices.Data = append(ret.Results.ElementIndices.Data, elemIdx)
				}

				// Handle OrderByFieldValue if present - add values for all order_by fields
				if len(orderByFieldBuilders) > 0 && orderByIterators != nil && subSearchIdx < len(orderByIterators) {
					iterators := orderByIterators[subSearchIdx]
					for fieldIdx, builder := range orderByFieldBuilders {
						if builder != nil && fieldIdx < len(iterators) && iterators[fieldIdx] != nil {
							orderByVal := iterators[fieldIdx](int(resultDataIdx))
							builder.Add(orderByVal)
						}
					}
				}

				cursors[subSearchIdx]++
			}
			if realTopK != -1 && realTopK != j {
				log.Ctx(ctx).Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
			}
			realTopK = j
			ret.Results.Topks = append(ret.Results.Topks, realTopK)

			// limit search result to avoid oom
			if retSize > maxOutputSize {
				return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
			}
		}
		ret.Results.TopK = realTopK

		// Set OrderByFieldValue in result - build all field data
		if len(orderByFieldBuilders) > 0 {
			ret.Results.OrderByFieldValues = make([]*schemapb.FieldData, len(orderByFieldBuilders))
			for fieldIdx, builder := range orderByFieldBuilders {
				if builder != nil {
					ret.Results.OrderByFieldValues[fieldIdx] = builder.Build()
				}
			}
		}
	}

	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

// reduceSearchResultDataWithGroupOrderBy reduces search results with both group_by and order_by.
//
// Group + OrderBy Behavior:
// When both group_by and order_by are specified, groups are sorted by the order_by field value
// of the FIRST (best-scoring) item within each group. This means:
//   - Items within each group are first selected by their similarity score (distance)
//   - The first item (highest score) in each group represents the group for ordering purposes
//   - Groups are then sorted based on this representative item's order_by field value
//
// Example: With group_by=category, order_by=price ASC, group_size=3
//   - Group "Electronics" has items with prices [$999, $799, $1299] (ordered by score)
//   - Group "Books" has items with prices [$15, $25, $10] (ordered by score)
//   - Electronics group is represented by $999 (first item's price)
//   - Books group is represented by $15 (first item's price)
//   - Result: Books group comes first because $15 < $999
func reduceSearchResultDataWithGroupOrderBy(ctx context.Context, subSearchResultData []*schemapb.SearchResultData,
	nq int64, topk int64, metricType string, pkType schemapb.DataType, offset int64, groupSize int64,
	orderByFields []*planpb.OrderByField,
) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("reduceSearchResultDataWithGroupOrderBy")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	limit := topk - offset
	log.Ctx(ctx).Debug("reduceSearchResultDataWithGroupOrderBy",
		zap.Int("len(subSearchResultData)", len(subSearchResultData)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit),
		zap.String("metricType", metricType))

	ret := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topk,
			FieldsData: []*schemapb.FieldData{},
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}
	outputBound := groupSize * limit
	if err := setupIdListForSearchResult(ret, pkType, outputBound); err != nil {
		return ret, err
	}

	if allSearchCount, _, err := checkResultDatas(ctx, subSearchResultData, nq, topk); err != nil {
		log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
		return ret, err
	} else {
		ret.GetResults().AllSearchCount = allSearchCount
	}

	// Find the first non-empty FieldsData as template
	for _, result := range subSearchResultData {
		if len(result.GetFieldsData()) > 0 {
			ret.GetResults().FieldsData = typeutil.PrepareResultFieldData(result.GetFieldsData(), limit)
			break
		}
	}

	var (
		subSearchNum                = len(subSearchResultData)
		subSearchNqOffset           = make([][]int64, subSearchNum)
		subSearchGroupByValIterator = make([]func(int) any, subSearchNum)
	)
	for i := 0; i < subSearchNum; i++ {
		subSearchNqOffset[i] = make([]int64, subSearchResultData[i].GetNumQueries())
		for j := int64(1); j < nq; j++ {
			subSearchNqOffset[i][j] = subSearchNqOffset[i][j-1] + subSearchResultData[i].Topks[j-1]
		}
		subSearchGroupByValIterator[i] = typeutil.GetDataIterator(subSearchResultData[i].GetGroupByFieldValue())
	}

	gpFieldBuilder, err := typeutil.NewFieldDataBuilder(subSearchResultData[0].GetGroupByFieldValue().GetType(), true, int(limit))
	if err != nil {
		return ret, merr.WrapErrServiceInternal("failed to construct group by field data builder, this is abnormal as segcore should always set up a group by field, no matter data status, check code on qn", err.Error())
	}

	// Build order_by iterators
	orderByIterators := buildOrderByIterators(subSearchResultData, orderByFields)

	// Create order_by field builders for each order_by field
	// Find the first non-empty OrderByFieldValue across all shards to get type information
	var orderByFieldBuilders []*typeutil.FieldDataBuilder
	if len(orderByFields) > 0 && len(subSearchResultData) > 0 {
		var orderByFieldValues []*schemapb.FieldData
		for _, srd := range subSearchResultData {
			if vals := srd.GetOrderByFieldValues(); len(vals) > 0 {
				orderByFieldValues = vals
				break
			}
		}
		if len(orderByFieldValues) > 0 {
			orderByFieldBuilders = make([]*typeutil.FieldDataBuilder, len(orderByFieldValues))
			for fieldIdx, fieldData := range orderByFieldValues {
				if fieldData != nil {
					builder, err := typeutil.NewFieldDataBuilder(fieldData.GetType(), true, int(outputBound))
					if err != nil {
						log.Ctx(ctx).Warn("failed to construct order by field data builder", zap.Error(err), zap.Int("fieldIdx", fieldIdx))
						continue
					}
					builder.SetFieldId(fieldData.GetFieldId())
					orderByFieldBuilders[fieldIdx] = builder
				}
			}
		}
	}

	idxComputers := make([]*typeutil.FieldDataIdxComputer, subSearchNum)
	for i, srd := range subSearchResultData {
		idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.FieldsData)
	}

	var realTopK int64 = -1
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	selectionBound := groupSize * topk

	// reducing nq * topk results
	for i := int64(0); i < nq; i++ {
		var (
			cursors        = make([]int64, subSearchNum)
			j              int64
			groupByValMap  = make(map[interface{}][]*groupReduceInfo)
			groupByValList = make([]interface{}, 0, int(topk))
		)

		for j = 0; j < selectionBound; {
			subSearchIdx, resultDataIdx := selectHighestScoreIndex(ctx, subSearchResultData, subSearchNqOffset, cursors, i)

			if subSearchIdx == -1 {
				break
			}

			subSearchRes := subSearchResultData[subSearchIdx]

			id := typeutil.GetPK(subSearchRes.GetIds(), resultDataIdx)
			score := subSearchRes.GetScores()[resultDataIdx]
			groupByVal := subSearchGroupByValIterator[subSearchIdx](int(resultDataIdx))

			if len(groupByValMap[groupByVal]) == 0 && int64(len(groupByValMap)) >= topk {
				// skip when groupbyMap has been full and found new groupByVal
			} else if int64(len(groupByValMap[groupByVal])) >= groupSize {
				// skip when target group has been full
			} else {
				if len(groupByValMap[groupByVal]) == 0 {
					groupByValList = append(groupByValList, groupByVal)
				}
				groupByValMap[groupByVal] = append(groupByValMap[groupByVal], &groupReduceInfo{
					subSearchIdx: subSearchIdx,
					resultIdx:    resultDataIdx,
					id:           id,
					score:        score,
				})
				j++
			}

			cursors[subSearchIdx]++
		}

		type groupOrderByInfo struct {
			groupVal    interface{}
			orderByVals []any
		}
		groupInfos := make([]groupOrderByInfo, 0, len(groupByValMap))
		for _, groupVal := range groupByValList {
			groupEntities := groupByValMap[groupVal]
			if len(groupEntities) == 0 {
				continue
			}
			first := groupEntities[0]
			orderByVals := make([]any, len(orderByFields))
			// Defensive checks for nil iterators
			if orderByIterators != nil && first.subSearchIdx < len(orderByIterators) {
				iterators := orderByIterators[first.subSearchIdx]
				for fieldIdx := range orderByFields {
					if iterators != nil && fieldIdx < len(iterators) && iterators[fieldIdx] != nil {
						orderByVals[fieldIdx] = iterators[fieldIdx](int(first.resultIdx))
					}
				}
			}
			groupInfos = append(groupInfos, groupOrderByInfo{
				groupVal:    groupVal,
				orderByVals: orderByVals,
			})
		}
		sort.SliceStable(groupInfos, func(i, j int) bool {
			return compareOrderByValuesProxy(groupInfos[i].orderByVals, groupInfos[j].orderByVals, orderByFields) < 0
		})

		var outputCount int64
		var groupIdx int64
		for i := range groupInfos {
			group := &groupInfos[i] // Use pointer to avoid copying struct (~40 bytes)
			if groupIdx < offset {
				groupIdx++
				continue
			}
			if groupIdx >= offset+limit {
				break
			}
			groupIdx++
			groupEntities := groupByValMap[group.groupVal]
			for _, groupEntity := range groupEntities {
				subResData := subSearchResultData[groupEntity.subSearchIdx]
				if len(ret.Results.FieldsData) > 0 {
					fieldIdxs := idxComputers[groupEntity.subSearchIdx].Compute(groupEntity.resultIdx)
					retSize += typeutil.AppendFieldData(ret.Results.FieldsData, subResData.FieldsData, groupEntity.resultIdx, fieldIdxs...)
				}
				typeutil.AppendPKs(ret.Results.Ids, groupEntity.id)
				ret.Results.Scores = append(ret.Results.Scores, groupEntity.score)

				// Handle ElementIndices if present
				if subResData.ElementIndices != nil {
					if ret.Results.ElementIndices == nil {
						ret.Results.ElementIndices = &schemapb.LongArray{
							Data: make([]int64, 0, limit),
						}
					}
					elemIdx := subResData.ElementIndices.GetData()[groupEntity.resultIdx]
					ret.Results.ElementIndices.Data = append(ret.Results.ElementIndices.Data, elemIdx)
				}

				gpFieldBuilder.Add(group.groupVal)

				// Handle OrderByFieldValue if present - add values for all order_by fields
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

		if realTopK != -1 && realTopK != outputCount {
			log.Ctx(ctx).Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
		}
		realTopK = outputCount
		ret.Results.Topks = append(ret.Results.Topks, realTopK)
		ret.Results.GroupByFieldValue = gpFieldBuilder.Build()

		// limit search result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	ret.Results.TopK = realTopK

	// Set OrderByFieldValue in result - build all field data
	if len(orderByFieldBuilders) > 0 {
		ret.Results.OrderByFieldValues = make([]*schemapb.FieldData, len(orderByFieldBuilders))
		for fieldIdx, builder := range orderByFieldBuilders {
			if builder != nil {
				ret.Results.OrderByFieldValues[fieldIdx] = builder.Build()
			}
		}
	}

	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

// compareOrderByValuesProxy compares two order_by value arrays
// Returns: -1 if lhs < rhs, 0 if lhs == rhs, 1 if lhs > rhs
func compareOrderByValuesProxy(
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
		cmp := compareValuesProxy(lhsVal, rhsVal)
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

func compareKey(keyI interface{}, keyJ interface{}) bool {
	switch keyI.(type) {
	case int64:
		return keyI.(int64) < keyJ.(int64)
	case string:
		return keyI.(string) < keyJ.(string)
	}
	return false
}

func setupIdListForSearchResult(searchResult *milvuspb.SearchResults, pkType schemapb.DataType, capacity int64) error {
	switch pkType {
	case schemapb.DataType_Int64:
		searchResult.GetResults().Ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: make([]int64, 0, capacity),
			},
		}
	case schemapb.DataType_VarChar:
		searchResult.GetResults().Ids.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: make([]string, 0, capacity),
			},
		}
	default:
		return errors.New("unsupported pk type")
	}
	return nil
}

func fillInEmptyResult(numQueries int64) *milvuspb.SearchResults {
	return &milvuspb.SearchResults{
		Status: merr.Success("search result is empty"),
		Results: &schemapb.SearchResultData{
			NumQueries: numQueries,
			Topks:      make([]int64, numQueries),
		},
	}
}

func reduceResults(ctx context.Context, toReduceResults []*internalpb.SearchResults, nq, topK, offset int64, metricType string, pkType schemapb.DataType, queryInfo *planpb.QueryInfo, isAdvance bool, collectionID int64, partitionIDs []int64) (*milvuspb.SearchResults, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "reduceResults")
	defer sp.End()

	log := log.Ctx(ctx)
	// Decode all search results
	validSearchResults, err := decodeSearchResults(ctx, toReduceResults)
	if err != nil {
		log.Warn("failed to decode search results", zap.Error(err))
		return nil, err
	}

	if len(validSearchResults) <= 0 {
		log.Debug("reduced search results is empty, fill in empty result")
		return fillInEmptyResult(nq), nil
	}

	// Reduce all search results
	log.Debug("proxy search post execute reduce",
		zap.Int64("collection", collectionID),
		zap.Int64s("partitionIDs", partitionIDs),
		zap.Int("number of valid search results", len(validSearchResults)))
	var result *milvuspb.SearchResults
	reduceInfo := reduce.NewReduceSearchResultInfo(nq, topK).
		WithMetricType(metricType).
		WithPkType(pkType).
		WithOffset(offset).
		WithGroupByField(queryInfo.GetGroupByFieldId()).
		WithGroupSize(queryInfo.GetGroupSize()).
		WithAdvance(isAdvance)

	// Add order_by_fields if present
	if len(queryInfo.GetOrderByFields()) > 0 {
		reduceInfo = reduceInfo.WithOrderByFields(queryInfo.GetOrderByFields())
	}

	result, err = reduceSearchResult(ctx, validSearchResults, reduceInfo)
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		return nil, err
	}
	return result, nil
}

func decodeSearchResults(ctx context.Context, searchResults []*internalpb.SearchResults) ([]*schemapb.SearchResultData, error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "decodeSearchResults")
	defer sp.End()
	tr := timerecord.NewTimeRecorder("decodeSearchResults")
	results := make([]*schemapb.SearchResultData, 0)
	for _, partialSearchResult := range searchResults {
		if partialSearchResult.SlicedBlob == nil {
			continue
		}

		var partialResultData schemapb.SearchResultData
		err := proto.Unmarshal(partialSearchResult.SlicedBlob, &partialResultData)
		if err != nil {
			return nil, err
		}
		results = append(results, &partialResultData)
	}
	tr.CtxElapse(ctx, "decodeSearchResults done")
	return results, nil
}

func checkSearchResultData(data *schemapb.SearchResultData, nq int64, topk int64, pkHitNum int) error {
	if data.NumQueries != nq {
		return fmt.Errorf("search result's nq(%d) mis-match with %d", data.NumQueries, nq)
	}
	if data.TopK != topk {
		return fmt.Errorf("search result's topk(%d) mis-match with %d", data.TopK, topk)
	}

	if len(data.Scores) != pkHitNum {
		return fmt.Errorf("search result's score length invalid, score length=%d, expectedLength=%d",
			len(data.Scores), pkHitNum)
	}
	return nil
}

func selectHighestScoreIndex(ctx context.Context, subSearchResultData []*schemapb.SearchResultData, subSearchNqOffset [][]int64, cursors []int64, qi int64) (int, int64) {
	var (
		subSearchIdx        = -1
		resultDataIdx int64 = -1
	)
	maxScore := minFloat32
	for i := range cursors {
		if cursors[i] >= subSearchResultData[i].Topks[qi] {
			continue
		}
		sIdx := subSearchNqOffset[i][qi] + cursors[i]
		sScore := subSearchResultData[i].Scores[sIdx]

		// Choose the larger score idx or the smaller pk idx with the same score
		if subSearchIdx == -1 || sScore > maxScore {
			subSearchIdx = i
			resultDataIdx = sIdx
			maxScore = sScore
		} else if sScore == maxScore {
			if subSearchIdx == -1 {
				// A bad case happens where Knowhere returns distance/score == +/-maxFloat32
				// by mistake.
				log.Ctx(ctx).Error("a bad score is returned, something is wrong here!", zap.Float32("score", sScore))
			} else if typeutil.ComparePK(
				typeutil.GetPK(subSearchResultData[i].GetIds(), sIdx),
				typeutil.GetPK(subSearchResultData[subSearchIdx].GetIds(), resultDataIdx)) {
				subSearchIdx = i
				resultDataIdx = sIdx
				maxScore = sScore
			}
		}
	}
	return subSearchIdx, resultDataIdx
}

// buildOrderByIterators creates iterators for order_by fields from OrderByFieldValue.
// OrderByFieldValue is a repeated field containing data for ALL order_by fields.
// Each element in the slice corresponds to one order_by field in order.
// This is separate from FieldsData (output_fields) and GroupByFieldValue.
func buildOrderByIterators(
	subSearchResultData []*schemapb.SearchResultData,
	orderByFields []*planpb.OrderByField,
) [][]func(int) any {
	if len(orderByFields) == 0 {
		return nil
	}

	iterators := make([][]func(int) any, len(subSearchResultData))
	for i, srd := range subSearchResultData {
		iterators[i] = make([]func(int) any, len(orderByFields))
		orderByFieldValues := srd.GetOrderByFieldValues()

		// Build iterator for each order_by field from the repeated OrderByFieldValue
		for fieldIdx := range orderByFields {
			if fieldIdx < len(orderByFieldValues) && orderByFieldValues[fieldIdx] != nil {
				iterators[i][fieldIdx] = typeutil.GetDataIterator(orderByFieldValues[fieldIdx])
			} else {
				// Field data not present - this indicates a data consistency issue
				// Log warning and return nil iterator (comparison will treat as null)
				log.Warn("OrderByFieldValue missing for shard, ordering may be affected",
					zap.Int("shardIdx", i),
					zap.Int("fieldIdx", fieldIdx),
					zap.Int("expectedFields", len(orderByFields)),
					zap.Int("actualFields", len(orderByFieldValues)))
				iterators[i][fieldIdx] = func(int) any { return nil }
			}
		}
	}
	return iterators
}

// selectIndexByOrderBy selects the next result index based on order_by field values
func selectIndexByOrderBy(
	ctx context.Context,
	subSearchResultData []*schemapb.SearchResultData,
	subSearchNqOffset [][]int64,
	cursors []int64,
	qi int64,
	orderByFields []*planpb.OrderByField,
	orderByIterators [][]func(int) any,
) (int, int64) {
	if len(orderByFields) == 0 {
		// Fallback to score-based selection
		return selectHighestScoreIndex(ctx, subSearchResultData, subSearchNqOffset, cursors, qi)
	}

	var (
		subSearchIdx        = -1
		resultDataIdx int64 = -1
	)

	for i := range cursors {
		if cursors[i] >= subSearchResultData[i].Topks[qi] {
			continue
		}

		sIdx := subSearchNqOffset[i][qi] + cursors[i]

		if subSearchIdx == -1 {
			subSearchIdx = i
			resultDataIdx = sIdx
			continue
		}

		// Compare by order_by fields
		compareResult := compareByOrderByFieldsProxy(
			subSearchResultData[i], sIdx, orderByIterators[i],
			subSearchResultData[subSearchIdx], resultDataIdx, orderByIterators[subSearchIdx],
			orderByFields,
		)

		if compareResult < 0 {
			// Current result is better
			subSearchIdx = i
			resultDataIdx = sIdx
		} else if compareResult == 0 {
			// Equal order_by values, use distance as tie-breaker
			sScore := subSearchResultData[i].Scores[sIdx]
			selScore := subSearchResultData[subSearchIdx].Scores[resultDataIdx]
			if sScore > selScore {
				subSearchIdx = i
				resultDataIdx = sIdx
			}
			// If scores are also equal, preserve current selection (relative order)
			// Don't use PK as tie-breaker; PK uniqueness is guaranteed by pk_set
		}
	}

	return subSearchIdx, resultDataIdx
}

// compareByOrderByFieldsProxy compares two results by order_by fields
// Returns: -1 if lhs < rhs, 0 if lhs == rhs, 1 if lhs > rhs
func compareByOrderByFieldsProxy(
	lhsData *schemapb.SearchResultData,
	lhsIdx int64,
	lhsIterators []func(int) any,
	rhsData *schemapb.SearchResultData,
	rhsIdx int64,
	rhsIterators []func(int) any,
	orderByFields []*planpb.OrderByField,
) int {
	for fieldIdx, field := range orderByFields {
		if fieldIdx >= len(lhsIterators) || fieldIdx >= len(rhsIterators) {
			break
		}

		// Defensive nil checks for iterator functions
		var lhsVal, rhsVal any
		if lhsIterators[fieldIdx] != nil {
			lhsVal = lhsIterators[fieldIdx](int(lhsIdx))
		}
		if rhsIterators[fieldIdx] != nil {
			rhsVal = rhsIterators[fieldIdx](int(rhsIdx))
		}

		// Handle null values: null < non-null
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
		cmp := compareValuesProxy(lhsVal, rhsVal)
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

// compareValuesProxy compares two values of any type
// Returns: -1 if lhs < rhs, 0 if lhs == rhs, 1 if lhs > rhs
func compareValuesProxy(lhs, rhs any) int {
	switch l := lhs.(type) {
	case bool:
		if r, ok := rhs.(bool); ok {
			if l == r {
				return 0
			}
			if l {
				return 1
			}
			return -1
		}
	case int8:
		if r, ok := rhs.(int8); ok {
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	case int16:
		if r, ok := rhs.(int16); ok {
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	case int32:
		if r, ok := rhs.(int32); ok {
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	case int64:
		if r, ok := rhs.(int64); ok {
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	case float32:
		if r, ok := rhs.(float32); ok {
			// Handle NaN: NaN < non-NaN (consistent with C++ CompareOrderByValue)
			lIsNaN := math.IsNaN(float64(l))
			rIsNaN := math.IsNaN(float64(r))
			if lIsNaN && rIsNaN {
				return 0
			}
			if lIsNaN {
				return -1 // NaN < non-NaN
			}
			if rIsNaN {
				return 1 // non-NaN > NaN
			}
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	case float64:
		if r, ok := rhs.(float64); ok {
			// Handle NaN: NaN < non-NaN (consistent with C++ CompareOrderByValue)
			lIsNaN := math.IsNaN(l)
			rIsNaN := math.IsNaN(r)
			if lIsNaN && rIsNaN {
				return 0
			}
			if lIsNaN {
				return -1 // NaN < non-NaN
			}
			if rIsNaN {
				return 1 // non-NaN > NaN
			}
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	case string:
		if r, ok := rhs.(string); ok {
			if l < r {
				return -1
			}
			if l > r {
				return 1
			}
			return 0
		}
	}
	// Type mismatch or unsupported
	return 0
}
