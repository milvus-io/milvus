package proxy

import (
	"context"
	"fmt"

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

// reduceMode classifies a reduce request into the three disjoint execution
// paths below. Kept local to the dispatcher so the switch makes the coverage
// matrix explicit rather than nesting HasGroupBy / IsAdvance conditions.
//
// reduceModeGroupBy subsumes both legacy single-field and new multi-field
// group-by; the unified entry reduceSearchResultDataWithGroupBy branches on
// len(groupByFieldIDs) internally for bucket storage.
type reduceMode int

const (
	reduceModeNoGroupBy reduceMode = iota
	reduceModeGroupBy
	reduceModeAdvanceGroupBy
)

func selectReduceMode(r *reduce.ResultInfo) reduceMode {
	switch {
	case r.HasGroupBy() && r.GetIsAdvance():
		// hybrid-search sub-path: per-search-path scores are not yet fused,
		// so offset/groupSize cannot be applied here.
		return reduceModeAdvanceGroupBy
	case r.HasGroupBy():
		return reduceModeGroupBy
	default:
		return reduceModeNoGroupBy
	}
}

func reduceSearchResult(ctx context.Context, subSearchResultData []*schemapb.SearchResultData, reduceInfo *reduce.ResultInfo) (*milvuspb.SearchResults, error) {
	switch selectReduceMode(reduceInfo) {
	case reduceModeGroupBy:
		return reduceSearchResultDataWithGroupBy(ctx,
			subSearchResultData,
			reduceInfo.GetNq(),
			reduceInfo.GetTopK(),
			reduceInfo.GetMetricType(),
			reduceInfo.GetPkType(),
			reduceInfo.EffectiveOffset(),
			reduceInfo.GetGroupSize(),
			reduceInfo.GetGroupByFieldIds(),
			reduceInfo.GetIsSearchAggregation())
	case reduceModeAdvanceGroupBy:
		return reduceAdvanceGroupBy(ctx,
			subSearchResultData,
			reduceInfo.GetNq(),
			reduceInfo.GetTopK(),
			reduceInfo.GetPkType(),
			reduceInfo.GetMetricType(),
			reduceInfo.GetGroupByFieldIds())
	default:
		return reduceSearchResultDataNoGroupBy(ctx,
			subSearchResultData,
			reduceInfo.GetNq(),
			reduceInfo.GetTopK(),
			reduceInfo.GetMetricType(),
			reduceInfo.GetPkType(),
			reduceInfo.GetOffset())
	}
}

// reduceSearchResultDataWithGroupBy emits plural group_by_field_values.
// N=1 uses a typed Go map; N>=2 uses a uint64 hash + values-equality chain
// (because []any is not a valid map key). Non-agg requests regroup N>=2
// rows by bucket at emit to match the N=1 shape; agg requests stream in
// walk order since the downstream aggOp reorganizes anyway.
func reduceSearchResultDataWithGroupBy(
	ctx context.Context,
	subSearchResultData []*schemapb.SearchResultData,
	nq, topk int64,
	metricType string,
	pkType schemapb.DataType,
	offset int64,
	groupSize int64,
	groupByFieldIDs []int64,
	isSearchAggregation bool,
) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("reduceSearchResultDataWithGroupBy")
	defer func() { tr.CtxElapse(ctx, "done") }()

	limit := topk - offset
	log.Ctx(ctx).Debug("reduceSearchResultDataWithGroupBy",
		zap.Int("subSearchCount", len(subSearchResultData)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit),
		zap.Int64("groupSize", groupSize),
		zap.Int("groupByFieldCount", len(groupByFieldIDs)),
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
	groupBound := groupSize * limit
	if err := setupIdListForSearchResult(ret, pkType, groupBound); err != nil {
		return ret, err
	}
	allSearchCount, hitNum, err := checkResultDatas(ctx, subSearchResultData, nq, topk)
	if err != nil {
		log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
		return ret, err
	}
	ret.GetResults().AllSearchCount = allSearchCount

	for _, result := range subSearchResultData {
		if len(result.GetFieldsData()) > 0 {
			ret.GetResults().FieldsData = typeutil.PrepareResultFieldData(result.GetFieldsData(), limit)
			break
		}
	}

	singleFieldGroupBy := len(groupByFieldIDs) == 1
	if err := reduce.ValidateGroupByFieldsPresent(subSearchResultData, groupByFieldIDs, singleFieldGroupBy); err != nil {
		return ret, merr.WrapErrServiceInternal("failed to construct group by field data builder", err.Error())
	}
	if hitNum == 0 {
		ret.Results.Topks = make([]int64, nq)
		return ret, nil
	}

	subSearchNum := len(subSearchResultData)
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

	var acceptedRows []reduce.RowRef
	if len(groupByFieldIDs) == 1 {
		acceptedRows, err = runSingleFieldGroupByHotLoop(ctx, ret, subSearchResultData, subSearchNqOffset, idxComputers,
			nq, offset, groupSize, limit, groupBound, groupByFieldIDs[0], maxOutputSize)
	} else {
		acceptedRows, err = runMultiFieldGroupByHotLoop(ctx, ret, subSearchResultData, subSearchNqOffset, idxComputers,
			nq, offset, groupSize, limit, groupBound, groupByFieldIDs, maxOutputSize, isSearchAggregation)
	}
	if err != nil {
		return nil, err
	}

	if err := reduce.WriteGroupByFieldValues(ret.Results, acceptedRows, subSearchResultData, groupByFieldIDs); err != nil {
		return ret, merr.WrapErrServiceInternal("failed to construct group by field data builder", err.Error())
	}

	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

// runSingleFieldGroupByHotLoop handles the N=1 case using a typed Go map.
// Reads the group-by column from whichever channel it is present in: the
// legacy singular channel (GroupByFieldValue) for legacy wire, or plural[0]
// for 1-field SearchAggregation wire.
func runSingleFieldGroupByHotLoop(
	ctx context.Context,
	ret *milvuspb.SearchResults,
	subSearchResultData []*schemapb.SearchResultData,
	subSearchNqOffset [][]int64,
	idxComputers []*typeutil.FieldDataIdxComputer,
	nq, offset, groupSize, limit, groupBound int64,
	groupByFieldID int64,
	maxOutputSize int64,
) ([]reduce.RowRef, error) {
	subSearchNum := len(subSearchResultData)
	subSearchGroupByValIterator := make([]func(int) any, subSearchNum)
	for i := range subSearchResultData {
		fd := reduce.FindGroupByFieldData(subSearchResultData[i], groupByFieldID, true)
		subSearchGroupByValIterator[i] = typeutil.GetDataIterator(fd)
	}

	acceptedRows := make([]reduce.RowRef, 0, nq*groupBound)
	var realTopK int64 = -1
	var retSize int64

	for i := int64(0); i < nq; i++ {
		cursors := make([]int64, subSearchNum)
		// Bucket stores just (subIdx, rowIdx) — id and score are re-fetched
		// from the source shard at emit time. Saves 24B per accepted row
		// plus interface-boxing allocs for int64/string pk values.
		groupByValMap := make(map[any][]reduce.RowRef)
		skipOffsetMap := make(map[any]bool)
		groupByValList := make([]any, 0, limit)

		var j int64
		for j = 0; j < groupBound; {
			subSearchIdx, resultDataIdx := selectHighestScoreIndex(ctx, subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}
			groupByVal := subSearchGroupByValIterator[subSearchIdx](int(resultDataIdx))

			if int64(len(skipOffsetMap)) < offset || skipOffsetMap[groupByVal] {
				skipOffsetMap[groupByVal] = true
			} else if bucket, exists := groupByValMap[groupByVal]; !exists && int64(len(groupByValMap)) >= limit {
				// topK distinct groups reached; drop new groups
			} else if exists && int64(len(bucket)) >= groupSize {
				// group full
			} else {
				if !exists {
					groupByValList = append(groupByValList, groupByVal)
				}
				groupByValMap[groupByVal] = append(groupByValMap[groupByVal], reduce.RowRef{
					ResultIdx: subSearchIdx, RowIdx: resultDataIdx,
				})
				j++
			}
			cursors[subSearchIdx]++
		}

		for _, key := range groupByValList {
			for _, ref := range groupByValMap[key] {
				subResData := subSearchResultData[ref.ResultIdx]
				if len(ret.Results.FieldsData) > 0 {
					fieldIdxs := idxComputers[ref.ResultIdx].Compute(ref.RowIdx)
					retSize += typeutil.AppendFieldData(ret.Results.FieldsData, subResData.FieldsData, ref.RowIdx, fieldIdxs...)
				}
				typeutil.AppendPKs(ret.Results.Ids, typeutil.GetPK(subResData.GetIds(), ref.RowIdx))
				ret.Results.Scores = append(ret.Results.Scores, subResData.GetScores()[ref.RowIdx])
				if subResData.ElementIndices != nil {
					if ret.Results.ElementIndices == nil {
						ret.Results.ElementIndices = &schemapb.LongArray{Data: make([]int64, 0, limit)}
					}
					ret.Results.ElementIndices.Data = append(ret.Results.ElementIndices.Data, subResData.ElementIndices.GetData()[ref.RowIdx])
				}
				acceptedRows = append(acceptedRows, ref)
			}
		}

		if realTopK != -1 && realTopK != j {
			log.Ctx(ctx).Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
		}
		realTopK = j
		ret.Results.Topks = append(ret.Results.Topks, realTopK)

		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}

	ret.Results.TopK = realTopK
	return acceptedRows, nil
}

// runMultiFieldGroupByHotLoop handles N>=2 group-by. Non-agg regroups by
// bucket at emit (match N=1 shape); agg streams in walk order.
func runMultiFieldGroupByHotLoop(
	ctx context.Context,
	ret *milvuspb.SearchResults,
	subSearchResultData []*schemapb.SearchResultData,
	subSearchNqOffset [][]int64,
	idxComputers []*typeutil.FieldDataIdxComputer,
	nq, offset, groupSize, limit, groupBound int64,
	groupByFieldIDs []int64,
	maxOutputSize int64,
	isSearchAggregation bool,
) ([]reduce.RowRef, error) {
	subSearchNum := len(subSearchResultData)
	subSearchKeyExtractors := make([]multiGroupKeyExtractor, subSearchNum)
	for i := range subSearchResultData {
		subSearchKeyExtractors[i] = buildMultiGroupKeyExtractor(subSearchResultData[i], groupByFieldIDs)
	}

	acceptedRows := make([]reduce.RowRef, 0, nq*groupBound)
	var realTopK int64 = -1
	var retSize int64

	for i := int64(0); i < nq; i++ {
		cursors := make([]int64, subSearchNum)
		groupBuckets := make(map[uint64][]*multiGroupEntry)
		skipGroups := make(map[uint64][]*multiGroupEntry)
		distinctSkipped := int64(0)
		totalGroups := int64(0)
		perNqAccepted := make([]reduce.RowRef, 0, groupBound)
		// Non-agg only: insertion-order list for regroup-on-emit.
		var groupOrder []*multiGroupEntry
		if !isSearchAggregation {
			groupOrder = make([]*multiGroupEntry, 0, limit)
		}

		var j int64
		for j = 0; j < groupBound; {
			subSearchIdx, resultDataIdx := selectHighestScoreIndex(ctx, subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}
			hash, values := subSearchKeyExtractors[subSearchIdx](int(resultDataIdx))

			if offset > 0 {
				if findMultiGroupEntry(skipGroups[hash], values) != nil {
					cursors[subSearchIdx]++
					continue
				}
				if distinctSkipped < offset {
					skipGroups[hash] = append(skipGroups[hash], &multiGroupEntry{values: values})
					distinctSkipped++
					cursors[subSearchIdx]++
					continue
				}
			}

			entry := findMultiGroupEntry(groupBuckets[hash], values)
			isNewGroup := entry == nil
			switch {
			case isNewGroup && totalGroups >= limit:
				// topK distinct groups reached
			case !isNewGroup && entry.count >= groupSize:
				// group full
			default:
				if isNewGroup {
					entry = &multiGroupEntry{values: values}
					groupBuckets[hash] = append(groupBuckets[hash], entry)
					totalGroups++
					if !isSearchAggregation {
						groupOrder = append(groupOrder, entry)
					}
				}
				entry.count++
				ref := reduce.RowRef{ResultIdx: subSearchIdx, RowIdx: resultDataIdx}
				if isSearchAggregation {
					perNqAccepted = append(perNqAccepted, ref)
				} else {
					entry.refs = append(entry.refs, ref)
				}
				j++
			}
			cursors[subSearchIdx]++
		}

		if !isSearchAggregation {
			for _, entry := range groupOrder {
				perNqAccepted = append(perNqAccepted, entry.refs...)
			}
		}

		for _, ref := range perNqAccepted {
			subResData := subSearchResultData[ref.ResultIdx]
			if len(ret.Results.FieldsData) > 0 {
				fieldIdxs := idxComputers[ref.ResultIdx].Compute(ref.RowIdx)
				retSize += typeutil.AppendFieldData(ret.Results.FieldsData, subResData.FieldsData, ref.RowIdx, fieldIdxs...)
			}
			typeutil.AppendPKs(ret.Results.Ids, typeutil.GetPK(subResData.GetIds(), ref.RowIdx))
			ret.Results.Scores = append(ret.Results.Scores, subResData.GetScores()[ref.RowIdx])
			if subResData.ElementIndices != nil {
				if ret.Results.ElementIndices == nil {
					ret.Results.ElementIndices = &schemapb.LongArray{Data: make([]int64, 0, limit)}
				}
				ret.Results.ElementIndices.Data = append(ret.Results.ElementIndices.Data, subResData.ElementIndices.GetData()[ref.RowIdx])
			}
		}
		acceptedRows = append(acceptedRows, perNqAccepted...)

		if realTopK != -1 && realTopK != j {
			log.Ctx(ctx).Warn("Proxy Reduce Search Result", zap.Error(errors.New("the length (topk) between all result of query is different")))
		}
		realTopK = j
		ret.Results.Topks = append(ret.Results.Topks, realTopK)

		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}

	ret.Results.TopK = realTopK
	return acceptedRows, nil
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
	nq int64, topK int64, pkType schemapb.DataType, metricType string, groupByFieldIDs []int64,
) (*milvuspb.SearchResults, error) {
	allowSingularFallback := len(groupByFieldIDs) == 1
	if err := reduce.ValidateGroupByFieldsPresent(subSearchResultData, groupByFieldIDs, allowSingularFallback); err != nil {
		return &milvuspb.SearchResults{Status: merr.Success(), Results: &schemapb.SearchResultData{}}, err
	}

	// for advance group by, offset is not applied, so just return when there's only one channel
	if len(subSearchResultData) == 1 {
		subResult := subSearchResultData[0]
		groupByFieldValues := make([]*schemapb.FieldData, 0, len(groupByFieldIDs))
		for _, fieldID := range groupByFieldIDs {
			gbv := reduce.FindGroupByFieldData(subResult, fieldID, allowSingularFallback)
			if gbv == nil {
				continue
			}
			if gbv.GetValidData() != nil {
				fieldName := gbv.GetFieldName()
				totalRows := len(gbv.GetValidData())
				gpFieldBuilder, err := typeutil.NewFieldDataBuilder(gbv.GetType(), true, totalRows)
				if err != nil {
					return nil, err
				}
				iter := typeutil.GetDataIterator(gbv)
				for i := 0; i < totalRows; i++ {
					gpFieldBuilder.Add(iter(i))
				}
				gbv = gpFieldBuilder.Build()
				gbv.FieldId = fieldID
				gbv.FieldName = fieldName
			}
			gbv.FieldId = fieldID
			groupByFieldValues = append(groupByFieldValues, gbv)
		}
		// segcore returns scores already negated for distance metrics (L2,
		// HAMMING, JACCARD, ...). The multi-shard path below applies a final
		// negation at the end of the function to restore the metric's natural
		// direction (smaller = better) before passing data to downstream
		// rerank/chain code. The single-shard early return must do the same,
		// otherwise the chain receives -L2 and any normalization expression
		// (1 − 2·atan(d)/π) gets applied to a negated value, producing a
		// monotonically inverted score and silently flipping result ordering.
		if !metric.PositivelyRelated(metricType) {
			for k := range subResult.Scores {
				subResult.Scores[k] *= -1
			}
		}
		if len(groupByFieldValues) > 0 {
			subResult.GroupByFieldValues = groupByFieldValues
			subResult.GroupByFieldValue = nil
		}
		return &milvuspb.SearchResults{
			Status:  merr.Success(),
			Results: subResult,
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
		return ret, err
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

	acceptedRows := make([]reduce.RowRef, 0, limit)
	// reducing nq * topk results
	for nqIdx := int64(0); nqIdx < nq; nqIdx++ {
		dataCount := int64(0)
		for subIdx := 0; subIdx < subSearchNum; subIdx += 1 {
			subData := subSearchResultData[subIdx]
			nqTopK := subData.Topks[nqIdx]
			if nqTopK == 0 {
				continue
			}
			subPks := subData.GetIds()
			subScores := subData.GetScores()

			for i := int64(0); i < nqTopK; i++ {
				innerIdx := subSearchNqOffset[subIdx][nqIdx] + i
				pk := typeutil.GetPK(subPks, innerIdx)
				score := subScores[innerIdx]
				acceptedRows = append(acceptedRows, reduce.RowRef{ResultIdx: subIdx, RowIdx: innerIdx})
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

	if err := reduce.WriteGroupByFieldValues(ret.Results, acceptedRows, subSearchResultData, groupByFieldIDs); err != nil {
		return ret, merr.WrapErrServiceInternal("failed to write group by field values", err.Error())
	}
	ret.Results.TopK = topK // realTopK is the topK of the nq-th query
	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

type multiGroupKeyExtractor func(idx int) (uint64, []any)

type multiGroupEntry struct {
	values []any
	count  int64
	// Non-agg only: rows accumulated for regroup-on-emit. Nil for agg and
	// for skip-set entries.
	refs []reduce.RowRef
}

func findMultiGroupEntry(bucket []*multiGroupEntry, values []any) *multiGroupEntry {
	for _, e := range bucket {
		if reduce.EqualGroupValues(e.values, values) {
			return e
		}
	}
	return nil
}

func buildMultiGroupKeyExtractor(data *schemapb.SearchResultData, groupByFieldIDs []int64) multiGroupKeyExtractor {
	iters := make([]func(int) any, len(groupByFieldIDs))
	for i, fid := range groupByFieldIDs {
		fd := reduce.FindGroupByFieldData(data, fid, len(groupByFieldIDs) == 1)
		if fd != nil {
			iters[i] = typeutil.GetDataIterator(fd)
		}
	}
	return reduce.MakeCompositeKeyExtractor(iters)
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

func reduceResults(ctx context.Context, toReduceResults []*internalpb.SearchResults, nq, topK, offset int64, metricType string, pkType schemapb.DataType, queryInfo *planpb.QueryInfo, isAdvance bool, isSearchAggregation bool, collectionID int64, partitionIDs []int64) (*milvuspb.SearchResults, error) {
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
	result, err = reduceSearchResult(ctx, validSearchResults, reduce.NewReduceSearchResultInfo(nq, topK).WithMetricType(metricType).WithPkType(pkType).
		WithOffset(offset).WithGroupSize(queryInfo.GetGroupSize()).
		WithGroupByFieldIdsFromProto(queryInfo.GetGroupByFieldId(), queryInfo.GetGroupByFieldIds()).
		WithAdvance(isAdvance).WithSearchAggregation(isSearchAggregation))
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
		if partialSearchResult.ResultData != nil {
			// Pre-decoded by delegator — use directly, no unmarshal needed.
			results = append(results, partialSearchResult.ResultData)
		} else if partialSearchResult.SlicedBlob != nil {
			var partialResultData schemapb.SearchResultData
			err := proto.Unmarshal(partialSearchResult.SlicedBlob, &partialResultData)
			if err != nil {
				return nil, err
			}
			results = append(results, &partialResultData)
		}
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
