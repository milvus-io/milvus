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
	acceptedRows := make([]reduce.RowRef, 0, nq*groupBound)

	// N=1 uses a typed Go map keyed by the raw value; N>=2 uses a uint64
	// hash map with values-equality chain (because []any is not a map key).
	groupByFieldIDs := info.GetGroupByFieldIds()
	singleField := len(groupByFieldIDs) == 1
	if err := reduce.ValidateGroupByFieldsPresent(searchResultData, groupByFieldIDs, singleField); err != nil {
		return nil, err
	}
	var singleIters []func(int) any
	var multiExtractors []keyExtractor
	if singleField {
		singleIters = buildSingleFieldIterators(searchResultData, groupByFieldIDs[0])
	} else {
		multiExtractors = buildMultiFieldExtractors(searchResultData, groupByFieldIDs)
	}

	for i := int64(0); i < info.GetNq(); i++ {
		var j, fdelta, rsize int64
		var err error
		if singleField {
			j, fdelta, rsize, err = reduceGroupBySinglePerNq(searchResultData, resultOffsets, i,
				info.GetTopK(), groupSize, groupBound, singleIters, idxComputers, ret, &acceptedRows, hasElementIndices)
		} else {
			j, fdelta, rsize, err = reduceGroupByMultiPerNq(searchResultData, resultOffsets, i,
				info.GetTopK(), groupSize, groupBound, multiExtractors, idxComputers, ret, &acceptedRows, hasElementIndices)
		}
		if err != nil {
			return nil, err
		}
		filteredCount += fdelta
		retSize += rsize
		ret.Topks = append(ret.Topks, j)

		if retSize > maxOutputSize {
			return nil, fmt.Errorf("search results exceed the maxOutputSize Limit %d", maxOutputSize)
		}
	}
	if err := writeGroupByOutput(ret, acceptedRows, searchResultData, info); err != nil {
		return ret, merr.WrapErrServiceInternal("failed to construct group by output", err.Error())
	}
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
	if info.HasGroupBy() {
		return &SearchGroupByReduce{}
	}
	return &SearchCommonReduce{}
}

// reduceGroupEntry tracks one composite-key bucket during SearchGroupByReduce.
// Held inside a map[uint64][]*reduceGroupEntry keyed by hash, so collisions
// resolve via linear scan with reduce.EqualGroupValues.
type reduceGroupEntry struct {
	values []any
	count  int64
}

func findReduceGroupEntry(bucket []*reduceGroupEntry, values []any) *reduceGroupEntry {
	for _, e := range bucket {
		if reduce.EqualGroupValues(e.values, values) {
			return e
		}
	}
	return nil
}

// reduceGroupBySinglePerNq is the N=1 per-nq hot loop: typed map[any]int64
// tracks per-group counts directly; no hash+chain indirection.
func reduceGroupBySinglePerNq(
	searchResultData []*schemapb.SearchResultData,
	resultOffsets [][]int64,
	nqIdx int64,
	topK, groupSize, groupBound int64,
	iterators []func(int) any,
	idxComputers []*typeutil.FieldDataIdxComputer,
	ret *schemapb.SearchResultData,
	acceptedRows *[]reduce.RowRef,
	hasElementIndices bool,
) (j int64, filtered int64, retSize int64, err error) {
	offsets := make([]int64, len(searchResultData))
	dedupSet := make(map[interface{}]struct{})
	groupCounts := make(map[any]int64)

	for j = 0; j < groupBound; {
		sel := SelectSearchResultData(searchResultData, resultOffsets, offsets, nqIdx)
		if sel == -1 {
			break
		}
		idx := resultOffsets[sel][nqIdx] + offsets[sel]

		id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
		val := iterators[sel](int(idx))
		score := searchResultData[sel].Scores[idx]
		dedupKey, dedupErr := getSearchResultDedupKey(searchResultData[sel], idx, id, hasElementIndices)
		if dedupErr != nil {
			return j, filtered, retSize, dedupErr
		}
		if _, ok := dedupSet[dedupKey]; !ok {
			cnt, exists := groupCounts[val]
			switch {
			case !exists && int64(len(groupCounts)) >= topK:
				filtered++
			case exists && cnt >= groupSize:
				filtered++
			default:
				fieldIdxs := idxComputers[sel].Compute(idx)
				retSize += typeutil.AppendFieldData(ret.FieldsData, searchResultData[sel].FieldsData, idx, fieldIdxs...)
				typeutil.AppendPKs(ret.Ids, id)
				ret.Scores = append(ret.Scores, score)
				if searchResultData[sel].ElementIndices != nil && ret.ElementIndices != nil {
					ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].ElementIndices.Data[idx])
				}
				groupCounts[val] = cnt + 1
				dedupSet[dedupKey] = struct{}{}
				*acceptedRows = append(*acceptedRows, reduce.RowRef{ResultIdx: sel, RowIdx: idx})
				j++
			}
		} else {
			filtered++
		}
		offsets[sel]++
	}
	return
}

// reduceGroupByMultiPerNq is the N>=2 per-nq hot loop: uint64 hash map with
// values-equality collision chain (because []any is not a map key).
func reduceGroupByMultiPerNq(
	searchResultData []*schemapb.SearchResultData,
	resultOffsets [][]int64,
	nqIdx int64,
	topK, groupSize, groupBound int64,
	extractors []keyExtractor,
	idxComputers []*typeutil.FieldDataIdxComputer,
	ret *schemapb.SearchResultData,
	acceptedRows *[]reduce.RowRef,
	hasElementIndices bool,
) (j int64, filtered int64, retSize int64, err error) {
	offsets := make([]int64, len(searchResultData))
	dedupSet := make(map[interface{}]struct{})
	groupBuckets := make(map[uint64][]*reduceGroupEntry)
	totalGroups := int64(0)

	for j = 0; j < groupBound; {
		sel := SelectSearchResultData(searchResultData, resultOffsets, offsets, nqIdx)
		if sel == -1 {
			break
		}
		idx := resultOffsets[sel][nqIdx] + offsets[sel]

		id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
		hash, values := extractors[sel](int(idx))
		score := searchResultData[sel].Scores[idx]
		dedupKey, dedupErr := getSearchResultDedupKey(searchResultData[sel], idx, id, hasElementIndices)
		if dedupErr != nil {
			return j, filtered, retSize, dedupErr
		}
		if _, ok := dedupSet[dedupKey]; !ok {
			entry := findReduceGroupEntry(groupBuckets[hash], values)
			isNewGroup := entry == nil
			switch {
			case isNewGroup && totalGroups >= topK:
				filtered++
			case !isNewGroup && entry.count >= groupSize:
				filtered++
			default:
				if isNewGroup {
					entry = &reduceGroupEntry{values: values}
					groupBuckets[hash] = append(groupBuckets[hash], entry)
					totalGroups++
				}
				fieldIdxs := idxComputers[sel].Compute(idx)
				retSize += typeutil.AppendFieldData(ret.FieldsData, searchResultData[sel].FieldsData, idx, fieldIdxs...)
				typeutil.AppendPKs(ret.Ids, id)
				ret.Scores = append(ret.Scores, score)
				if searchResultData[sel].ElementIndices != nil && ret.ElementIndices != nil {
					ret.ElementIndices.Data = append(ret.ElementIndices.Data, searchResultData[sel].ElementIndices.Data[idx])
				}
				entry.count++
				dedupSet[dedupKey] = struct{}{}
				*acceptedRows = append(*acceptedRows, reduce.RowRef{ResultIdx: sel, RowIdx: idx})
				j++
			}
		} else {
			filtered++
		}
		offsets[sel]++
	}
	return
}

// keyExtractor returns (hash, normalized values) for the row at idx. The hash
// map drives bucket lookup; the values slice is retained for hash-collision
// disambiguation via reduce.EqualGroupValues. Values are normalized through
// reduce.NormalizeScalar so types align with the proxy-side reducer.
type keyExtractor func(idx int) (uint64, []any)

// buildSingleFieldIterators returns per-shard raw-value iterators for the
// N=1 group-by path. Falls back to the legacy singular channel when segcore
// emitted it without a FieldId stamp.
func buildSingleFieldIterators(searchResultData []*schemapb.SearchResultData, fieldID int64) []func(int) any {
	iters := make([]func(int) any, len(searchResultData))
	for i := range searchResultData {
		fd := reduce.FindGroupByFieldData(searchResultData[i], fieldID, true)
		if fd != nil {
			iters[i] = typeutil.GetDataIterator(fd)
		}
	}
	return iters
}

func buildMultiFieldExtractors(searchResultData []*schemapb.SearchResultData, groupByFieldIDs []int64) []keyExtractor {
	extractors := make([]keyExtractor, len(searchResultData))
	for i := range searchResultData {
		iters := make([]func(int) any, len(groupByFieldIDs))
		for j, fieldID := range groupByFieldIDs {
			fieldData := reduce.FindGroupByFieldData(searchResultData[i], fieldID, false)
			if fieldData != nil {
				iters[j] = typeutil.GetDataIterator(fieldData)
			}
		}
		extractors[i] = reduce.MakeCompositeKeyExtractor(iters)
	}
	return extractors
}

// writeGroupByOutput emits the plural group-by channel unconditionally — the
// proxy-side builders now consolidate legacy singular ids into the unified
// slice via WithGroupByFieldIdsFromProto, so HasGroupBy is always true for
// group-by requests. WriteGroupByFieldValues' singular-channel fallback
// covers the case where upstream segcore emitted the legacy channel shape.
func writeGroupByOutput(ret *schemapb.SearchResultData, acceptedRows []reduce.RowRef, searchResultData []*schemapb.SearchResultData, info *reduce.ResultInfo) error {
	return reduce.WriteGroupByFieldValues(ret, acceptedRows, searchResultData, info.GetGroupByFieldIds())
}
