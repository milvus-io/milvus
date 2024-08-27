package proxy

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type reduceSearchResultInfo struct {
	subSearchResultData []*schemapb.SearchResultData
	nq                  int64
	topK                int64
	metricType          string
	pkType              schemapb.DataType
	offset              int64
	queryInfo           *planpb.QueryInfo
}

func NewReduceSearchResultInfo(
	subSearchResultData []*schemapb.SearchResultData,
	nq int64,
	topK int64,
	metricType string,
	pkType schemapb.DataType,
	offset int64,
	queryInfo *planpb.QueryInfo,
) *reduceSearchResultInfo {
	return &reduceSearchResultInfo{
		subSearchResultData: subSearchResultData,
		nq:                  nq,
		topK:                topK,
		metricType:          metricType,
		pkType:              pkType,
		offset:              offset,
		queryInfo:           queryInfo,
	}
}

func reduceSearchResult(ctx context.Context, reduceInfo *reduceSearchResultInfo) (*milvuspb.SearchResults, error) {
	if reduceInfo.queryInfo.GroupByFieldId > 0 {
		return reduceSearchResultDataWithGroupBy(ctx,
			reduceInfo.subSearchResultData,
			reduceInfo.nq,
			reduceInfo.topK,
			reduceInfo.metricType,
			reduceInfo.pkType,
			reduceInfo.offset,
			reduceInfo.queryInfo.GroupSize)
	}
	return reduceSearchResultDataNoGroupBy(ctx,
		reduceInfo.subSearchResultData,
		reduceInfo.nq,
		reduceInfo.topK,
		reduceInfo.metricType,
		reduceInfo.pkType,
		reduceInfo.offset)
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
			FieldsData: typeutil.PrepareResultFieldData(subSearchResultData[0].GetFieldsData(), limit),
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}

	switch pkType {
	case schemapb.DataType_Int64:
		ret.GetResults().Ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: make([]int64, 0, limit),
			},
		}
	case schemapb.DataType_VarChar:
		ret.GetResults().Ids.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: make([]string, 0, limit),
			},
		}
	default:
		return nil, errors.New("unsupported pk type")
	}
	for i, sData := range subSearchResultData {
		pkLength := typeutil.GetSizeOfIDs(sData.GetIds())
		log.Ctx(ctx).Debug("subSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Int("length of pks", pkLength),
			zap.Int("length of FieldsData", len(sData.FieldsData)))
		ret.Results.AllSearchCount += sData.GetAllSearchCount()
		if err := checkSearchResultData(sData, nq, topk); err != nil {
			log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
			return ret, err
		}
		// printSearchResultData(sData, strconv.FormatInt(int64(i), 10))
	}

	var (
		subSearchNum = len(subSearchResultData)
		// for results of each subSearchResultData, storing the start offset of each query of nq queries
		subSearchNqOffset       = make([][]int64, subSearchNum)
		totalResCount     int64 = 0
	)
	for i := 0; i < subSearchNum; i++ {
		subSearchNqOffset[i] = make([]int64, subSearchResultData[i].GetNumQueries())
		for j := int64(1); j < nq; j++ {
			subSearchNqOffset[i][j] = subSearchNqOffset[i][j-1] + subSearchResultData[i].Topks[j-1]
		}
		totalResCount += subSearchNqOffset[i][nq-1]
	}

	var (
		skipDupCnt int64
		realTopK   int64 = -1
	)

	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	groupBound := groupSize * limit

	// reducing nq * topk results
	for i := int64(0); i < nq; i++ {
		var (
			// cursor of current data of each subSearch for merging the j-th data of TopK.
			// sum(cursors) == j
			cursors = make([]int64, subSearchNum)

			j              int64
			pkSet          = make(map[interface{}]struct{})
			groupByValMap  = make(map[interface{}][]*groupReduceInfo)
			skipOffsetMap  = make(map[interface{}]bool)
			groupByValList = make([]interface{}, limit)
			groupByValIdx  = 0
		)

		for j = 0; j < groupBound; {
			subSearchIdx, resultDataIdx := selectHighestScoreIndex(subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}
			subSearchRes := subSearchResultData[subSearchIdx]

			id := typeutil.GetPK(subSearchRes.GetIds(), resultDataIdx)
			score := subSearchRes.GetScores()[resultDataIdx]
			groupByVal := typeutil.GetData(subSearchRes.GetGroupByFieldValue(), int(resultDataIdx))
			if groupByVal == nil {
				return nil, errors.New("get nil groupByVal from subSearchRes, wrong states, as milvus doesn't support nil value," +
					"there must be sth wrong on queryNode side")
			}
			// remove duplicates
			if _, ok := pkSet[id]; !ok {
				if int64(len(skipOffsetMap)) < offset || skipOffsetMap[groupByVal] {
					skipOffsetMap[groupByVal] = true
					// the first offset's group will be ignored
					skipDupCnt++
				} else if len(groupByValMap[groupByVal]) == 0 && int64(len(groupByValMap)) >= limit {
					// skip when groupbyMap has been full and found new groupByVal
					skipDupCnt++
				} else if int64(len(groupByValMap[groupByVal])) >= groupSize {
					// skip when target group has been full
					skipDupCnt++
				} else {
					if len(groupByValMap[groupByVal]) == 0 {
						groupByValList[groupByValIdx] = groupByVal
						groupByValIdx++
					}
					groupByValMap[groupByVal] = append(groupByValMap[groupByVal], &groupReduceInfo{
						subSearchIdx: subSearchIdx,
						resultIdx:    resultDataIdx, id: id, score: score,
					})
					pkSet[id] = struct{}{}
					j++
				}
			} else {
				skipDupCnt++
			}

			cursors[subSearchIdx]++
		}

		// assemble all eligible values in group
		// values in groupByValList is sorted by the highest score in each group
		for _, groupVal := range groupByValList {
			if groupVal != nil {
				groupEntities := groupByValMap[groupVal]
				for _, groupEntity := range groupEntities {
					subResData := subSearchResultData[groupEntity.subSearchIdx]
					retSize += typeutil.AppendFieldData(ret.Results.FieldsData, subResData.FieldsData, groupEntity.resultIdx)
					typeutil.AppendPKs(ret.Results.Ids, groupEntity.id)
					ret.Results.Scores = append(ret.Results.Scores, groupEntity.score)
					if err := typeutil.AppendGroupByValue(ret.Results, groupVal, subResData.GetGroupByFieldValue().GetType()); err != nil {
						log.Ctx(ctx).Error("failed to append groupByValues", zap.Error(err))
						return ret, err
					}
				}
			}
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
	log.Ctx(ctx).Debug("skip duplicated search result when doing group by", zap.Int64("count", skipDupCnt))

	if float64(skipDupCnt) >= float64(totalResCount)*0.3 {
		log.Warn("GroupBy reduce skipped too many results, "+
			"this may influence the final result seriously",
			zap.Int64("skipDupCnt", skipDupCnt),
			zap.Int64("groupBound", groupBound))
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
			FieldsData: typeutil.PrepareResultFieldData(subSearchResultData[0].GetFieldsData(), limit),
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}

	switch pkType {
	case schemapb.DataType_Int64:
		ret.GetResults().Ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: make([]int64, 0, limit),
			},
		}
	case schemapb.DataType_VarChar:
		ret.GetResults().Ids.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: make([]string, 0, limit),
			},
		}
	default:
		return nil, errors.New("unsupported pk type")
	}
	for i, sData := range subSearchResultData {
		pkLength := typeutil.GetSizeOfIDs(sData.GetIds())
		log.Ctx(ctx).Debug("subSearchResultData",
			zap.Int("result No.", i),
			zap.Int64("nq", sData.NumQueries),
			zap.Int64("topk", sData.TopK),
			zap.Int("length of pks", pkLength),
			zap.Int("length of FieldsData", len(sData.FieldsData)))
		ret.Results.AllSearchCount += sData.GetAllSearchCount()
		if err := checkSearchResultData(sData, nq, topk); err != nil {
			log.Ctx(ctx).Warn("invalid search results", zap.Error(err))
			return ret, err
		}
		// printSearchResultData(sData, strconv.FormatInt(int64(i), 10))
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

	var (
		skipDupCnt int64
		realTopK   int64 = -1
	)

	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()

	// reducing nq * topk results
	for i := int64(0); i < nq; i++ {
		var (
			// cursor of current data of each subSearch for merging the j-th data of TopK.
			// sum(cursors) == j
			cursors = make([]int64, subSearchNum)

			j     int64
			idSet = make(map[interface{}]struct{}, limit)
		)

		// skip offset results
		for k := int64(0); k < offset; k++ {
			subSearchIdx, _ := selectHighestScoreIndex(subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}

			cursors[subSearchIdx]++
		}

		// keep limit results
		for j = 0; j < limit; {
			// From all the sub-query result sets of the i-th query vector,
			//   find the sub-query result set index of the score j-th data,
			//   and the index of the data in schemapb.SearchResultData
			subSearchIdx, resultDataIdx := selectHighestScoreIndex(subSearchResultData, subSearchNqOffset, cursors, i)
			if subSearchIdx == -1 {
				break
			}
			id := typeutil.GetPK(subSearchResultData[subSearchIdx].GetIds(), resultDataIdx)
			score := subSearchResultData[subSearchIdx].Scores[resultDataIdx]

			// remove duplicatessds
			if _, ok := idSet[id]; !ok {
				retSize += typeutil.AppendFieldData(ret.Results.FieldsData, subSearchResultData[subSearchIdx].FieldsData, resultDataIdx)
				typeutil.AppendPKs(ret.Results.Ids, id)
				ret.Results.Scores = append(ret.Results.Scores, score)
				idSet[id] = struct{}{}
				j++
			} else {
				// skip entity with same id
				skipDupCnt++
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
	log.Ctx(ctx).Debug("skip duplicated search result", zap.Int64("count", skipDupCnt))

	if skipDupCnt > 0 {
		log.Info("skip duplicated search result", zap.Int64("count", skipDupCnt))
	}

	ret.Results.TopK = realTopK // realTopK is the topK of the nq-th query
	if !metric.PositivelyRelated(metricType) {
		for k := range ret.Results.Scores {
			ret.Results.Scores[k] *= -1
		}
	}
	return ret, nil
}

func rankSearchResultData(ctx context.Context,
	nq int64,
	params *rankParams,
	pkType schemapb.DataType,
	searchResults []*milvuspb.SearchResults,
) (*milvuspb.SearchResults, error) {
	tr := timerecord.NewTimeRecorder("rankSearchResultData")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	offset := params.offset
	limit := params.limit
	topk := limit + offset
	roundDecimal := params.roundDecimal
	log.Ctx(ctx).Debug("rankSearchResultData",
		zap.Int("len(searchResults)", len(searchResults)),
		zap.Int64("nq", nq),
		zap.Int64("offset", offset),
		zap.Int64("limit", limit))

	ret := &milvuspb.SearchResults{
		Status: merr.Success(),
		Results: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       limit,
			FieldsData: make([]*schemapb.FieldData, 0),
			Scores:     []float32{},
			Ids:        &schemapb.IDs{},
			Topks:      []int64{},
		},
	}

	switch pkType {
	case schemapb.DataType_Int64:
		ret.GetResults().Ids.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: make([]int64, 0),
			},
		}
	case schemapb.DataType_VarChar:
		ret.GetResults().Ids.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: make([]string, 0),
			},
		}
	default:
		return nil, errors.New("unsupported pk type")
	}

	// []map[id]score
	accumulatedScores := make([]map[interface{}]float32, nq)
	for i := int64(0); i < nq; i++ {
		accumulatedScores[i] = make(map[interface{}]float32)
	}

	for _, result := range searchResults {
		scores := result.GetResults().GetScores()
		start := int64(0)
		for i := int64(0); i < nq; i++ {
			realTopk := result.GetResults().Topks[i]
			for j := start; j < start+realTopk; j++ {
				id := typeutil.GetPK(result.GetResults().GetIds(), j)
				accumulatedScores[i][id] += scores[j]
			}
			start += realTopk
		}
	}

	for i := int64(0); i < nq; i++ {
		idSet := accumulatedScores[i]
		keys := make([]interface{}, 0)
		for key := range idSet {
			keys = append(keys, key)
		}
		if int64(len(keys)) <= offset {
			ret.Results.Topks = append(ret.Results.Topks, 0)
			continue
		}

		compareKeys := func(keyI, keyJ interface{}) bool {
			switch keyI.(type) {
			case int64:
				return keyI.(int64) < keyJ.(int64)
			case string:
				return keyI.(string) < keyJ.(string)
			}
			return false
		}

		// sort id by score
		big := func(i, j int) bool {
			if idSet[keys[i]] == idSet[keys[j]] {
				return compareKeys(keys[i], keys[j])
			}
			return idSet[keys[i]] > idSet[keys[j]]
		}

		sort.Slice(keys, big)

		if int64(len(keys)) > topk {
			keys = keys[:topk]
		}

		// set real topk
		ret.Results.Topks = append(ret.Results.Topks, int64(len(keys))-offset)
		// append id and score
		for index := offset; index < int64(len(keys)); index++ {
			typeutil.AppendPKs(ret.Results.Ids, keys[index])
			score := idSet[keys[index]]
			if roundDecimal != -1 {
				multiplier := math.Pow(10.0, float64(roundDecimal))
				score = float32(math.Floor(float64(score)*multiplier+0.5) / multiplier)
			}
			ret.Results.Scores = append(ret.Results.Scores, score)
		}
	}

	return ret, nil
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
