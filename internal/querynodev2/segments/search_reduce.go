package segments

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ReduceInfo struct {
	nq             int64
	topK           int64
	groupByFieldID int64
	groupSize      int64
	metricType     string
}

func NewReduceInfo(nq int64, topK int64, groupByFieldID int64, groupSize int64, metric string) *ReduceInfo {
	return &ReduceInfo{nq, topK, groupByFieldID, groupSize, metric}
}

type SearchReduce interface {
	ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *ReduceInfo) (*schemapb.SearchResultData, error)
}

type SearchCommonReduce struct{}

func (scr *SearchCommonReduce) ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *ReduceInfo) (*schemapb.SearchResultData, error) {
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceSearchResultData")
	defer sp.End()
	log := log.Ctx(ctx)

	if len(searchResultData) == 0 {
		return &schemapb.SearchResultData{
			NumQueries: info.nq,
			TopK:       info.topK,
			FieldsData: make([]*schemapb.FieldData, 0),
			Scores:     make([]float32, 0),
			Ids:        &schemapb.IDs{},
			Topks:      make([]int64, 0),
		}, nil
	}
	ret := &schemapb.SearchResultData{
		NumQueries: info.nq,
		TopK:       info.topK,
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}

	resultOffsets := make([][]int64, len(searchResultData))
	for i := 0; i < len(searchResultData); i++ {
		resultOffsets[i] = make([]int64, len(searchResultData[i].Topks))
		for j := int64(1); j < info.nq; j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + searchResultData[i].Topks[j-1]
		}
		ret.AllSearchCount += searchResultData[i].GetAllSearchCount()
	}

	var skipDupCnt int64
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for i := int64(0); i < info.nq; i++ {
		offsets := make([]int64, len(searchResultData))
		idSet := make(map[interface{}]struct{})
		var j int64
		for j = 0; j < info.topK; {
			sel := SelectSearchResultData(searchResultData, resultOffsets, offsets, i)
			if sel == -1 {
				break
			}
			idx := resultOffsets[sel][i] + offsets[sel]

			id := typeutil.GetPK(searchResultData[sel].GetIds(), idx)
			score := searchResultData[sel].Scores[idx]

			// remove duplicates
			if _, ok := idSet[id]; !ok {
				retSize += typeutil.AppendFieldData(ret.FieldsData, searchResultData[sel].FieldsData, idx)
				typeutil.AppendPKs(ret.Ids, id)
				ret.Scores = append(ret.Scores, score)
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

func (sbr *SearchGroupByReduce) ReduceSearchResultData(ctx context.Context, searchResultData []*schemapb.SearchResultData, info *ReduceInfo) (*schemapb.SearchResultData, error) {
	ctx, sp := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "ReduceSearchResultData")
	defer sp.End()
	log := log.Ctx(ctx)

	if len(searchResultData) == 0 {
		return &schemapb.SearchResultData{
			NumQueries: info.nq,
			TopK:       info.topK,
			FieldsData: make([]*schemapb.FieldData, 0),
			Scores:     make([]float32, 0),
			Ids:        &schemapb.IDs{},
			Topks:      make([]int64, 0),
		}, nil
	}
	ret := &schemapb.SearchResultData{
		NumQueries: info.nq,
		TopK:       info.topK,
		FieldsData: make([]*schemapb.FieldData, len(searchResultData[0].FieldsData)),
		Scores:     make([]float32, 0),
		Ids:        &schemapb.IDs{},
		Topks:      make([]int64, 0),
	}

	resultOffsets := make([][]int64, len(searchResultData))
	for i := 0; i < len(searchResultData); i++ {
		resultOffsets[i] = make([]int64, len(searchResultData[i].Topks))
		for j := int64(1); j < info.nq; j++ {
			resultOffsets[i][j] = resultOffsets[i][j-1] + searchResultData[i].Topks[j-1]
		}
		ret.AllSearchCount += searchResultData[i].GetAllSearchCount()
	}

	var filteredCount int64
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	groupSize := info.groupSize
	if groupSize <= 0 {
		groupSize = 1
	}
	groupBound := info.topK * groupSize

	for i := int64(0); i < info.nq; i++ {
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
			groupByVal := typeutil.GetData(searchResultData[sel].GetGroupByFieldValue(), int(idx))
			score := searchResultData[sel].Scores[idx]
			if _, ok := idSet[id]; !ok {
				if groupByVal == nil {
					return ret, merr.WrapErrParameterMissing("GroupByVal returned from segment cannot be null")
				}

				groupCount := groupByValueMap[groupByVal]
				if groupCount == 0 && int64(len(groupByValueMap)) >= info.topK {
					// exceed the limit for group count, filter this entity
					filteredCount++
				} else if groupCount >= groupSize {
					// exceed the limit for each group, filter this entity
					filteredCount++
				} else {
					retSize += typeutil.AppendFieldData(ret.FieldsData, searchResultData[sel].FieldsData, idx)
					typeutil.AppendPKs(ret.Ids, id)
					ret.Scores = append(ret.Scores, score)
					if err := typeutil.AppendGroupByValue(ret, groupByVal, searchResultData[sel].GetGroupByFieldValue().GetType()); err != nil {
						log.Error("Failed to append groupByValues", zap.Error(err))
						return ret, err
					}
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
	if float64(filteredCount) >= 0.3*float64(groupBound) {
		log.Warn("GroupBy reduce filtered too many results, "+
			"this may influence the final result seriously",
			zap.Int64("filteredCount", filteredCount),
			zap.Int64("groupBound", groupBound))
	}
	log.Debug("skip duplicated search result", zap.Int64("count", filteredCount))
	return ret, nil
}

func InitSearchReducer(info *ReduceInfo) SearchReduce {
	if info.groupByFieldID > 0 {
		return &SearchGroupByReduce{}
	}
	return &SearchCommonReduce{}
}
