/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package rerank

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type PKType interface {
	int64 | string
}

// Data for a single search result for a single query, with multi fields
type columns struct {
	data   []any
	size   int64
	ids    any
	scores []float32

	nqOffset int64
}

type rerankInputs struct {
	// nqs,searchResultsIndex
	data         [][]*columns
	idGroupValue map[any]any
	nq           int64

	// field data need for non-requery rerank
	// multipIdField []map[int64]*schemapb.FieldData
	fieldData []*schemapb.SearchResultData

	// There is only fieldId in schemapb.SearchResultData, but no fieldName
	inputFieldIds []int64
}

func organizeFieldIdData(multipSearchResultData []*schemapb.SearchResultData, inputFieldIds []int64) ([]map[int64]*schemapb.FieldData, error) {
	multipIdField := []map[int64]*schemapb.FieldData{}
	for _, searchData := range multipSearchResultData {
		idField := map[int64]*schemapb.FieldData{}
		if searchData != nil && typeutil.GetSizeOfIDs(searchData.Ids) != 0 && len(searchData.FieldsData) != 0 {
			for _, field := range searchData.FieldsData {
				for _, fieldid := range inputFieldIds {
					if fieldid == field.FieldId {
						idField[field.FieldId] = field
					}
				}
			}
			if len(idField) != len(inputFieldIds) {
				return nil, fmt.Errorf("Search reaults mismatch rerank inputs")
			}
		}
		multipIdField = append(multipIdField, idField)
	}
	return multipIdField, nil
}

func newRerankInputs(multipSearchResultData []*schemapb.SearchResultData, inputFieldIds []int64, isGrouping bool) (*rerankInputs, error) {
	if len(multipSearchResultData) == 0 {
		return &rerankInputs{}, nil
	}

	multipIdField, err := organizeFieldIdData(multipSearchResultData, inputFieldIds)
	if err != nil {
		return nil, err
	}
	nq := multipSearchResultData[0].NumQueries
	cols := make([][]*columns, nq)
	for i := range cols {
		cols[i] = make([]*columns, len(multipSearchResultData))
	}
	for retIdx, searchResult := range multipSearchResultData {
		start := int64(0)
		for i := int64(0); i < nq; i++ {
			size := searchResult.Topks[i]
			if cols[i][retIdx] == nil {
				cols[i][retIdx] = &columns{}
				cols[i][retIdx].size = size
				cols[i][retIdx].ids = getIds(searchResult.Ids, start, size)
				cols[i][retIdx].scores = searchResult.Scores[start : start+size]
				cols[i][retIdx].nqOffset = start
			}
			for _, fieldId := range inputFieldIds {
				fieldData, exist := multipIdField[retIdx][fieldId]
				if !exist {
					continue
				}
				d, err := getField(fieldData, start, size)
				if err != nil {
					return nil, err
				}
				cols[i][retIdx].data = append(cols[i][retIdx].data, d)
			}
			start += size
		}
	}
	if isGrouping {
		idGroup, err := genIdGroupingMap(multipSearchResultData)
		if err != nil {
			return nil, err
		}
		return &rerankInputs{cols, idGroup, nq, multipSearchResultData, inputFieldIds}, nil
	}
	return &rerankInputs{cols, nil, nq, multipSearchResultData, inputFieldIds}, nil
}

func (inputs *rerankInputs) numOfQueries() int64 {
	return inputs.nq
}

type rerankOutputs struct {
	searchResultData *schemapb.SearchResultData
}

func newRerankOutputs(inputs *rerankInputs, searchParams *SearchParams) *rerankOutputs {
	topk := searchParams.limit
	if searchParams.isGrouping() {
		topk = topk * searchParams.groupSize
	}
	ret := &schemapb.SearchResultData{
		NumQueries: searchParams.nq,
		TopK:       topk,
		FieldsData: make([]*schemapb.FieldData, 0),
		Scores:     []float32{},
		Ids:        &schemapb.IDs{},
		Topks:      []int64{},
	}
	// Find the first non-empty fieldData and prepare result fields
	for _, fieldData := range inputs.fieldData {
		if fieldData != nil && len(fieldData.GetFieldsData()) > 0 {
			ret.FieldsData = typeutil.PrepareResultFieldData(fieldData.GetFieldsData(), searchParams.limit)
			break
		}
	}
	return &rerankOutputs{ret}
}

func appendResult[T PKType](inputs *rerankInputs, outputs *rerankOutputs, idScores *IDScores[T]) {
	ids := idScores.ids
	scores := idScores.scores
	outputs.searchResultData.Topks = append(outputs.searchResultData.Topks, int64(len(ids)))
	outputs.searchResultData.Scores = append(outputs.searchResultData.Scores, scores...)
	if len(inputs.fieldData) > 0 && len(outputs.searchResultData.FieldsData) > 0 {
		for idx := range ids {
			loc := idScores.locations[idx]
			typeutil.AppendFieldData(outputs.searchResultData.FieldsData, inputs.fieldData[loc.batchIdx].GetFieldsData(), int64(loc.offset))
		}
	}
	switch any(ids).(type) {
	case []int64:
		if outputs.searchResultData.Ids.GetIntId() == nil {
			outputs.searchResultData.Ids.IdField = &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: make([]int64, 0),
				},
			}
		}
		outputs.searchResultData.Ids.GetIntId().Data = append(outputs.searchResultData.Ids.GetIntId().Data, any(ids).([]int64)...)
	case []string:
		if outputs.searchResultData.Ids.GetStrId() == nil {
			outputs.searchResultData.Ids.IdField = &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: make([]string, 0),
				},
			}
		}
		outputs.searchResultData.Ids.GetStrId().Data = append(outputs.searchResultData.Ids.GetStrId().Data, any(ids).([]string)...)
	}
}

type IDScores[T PKType] struct {
	ids       []T
	scores    []float32
	size      int64
	locations []IDLoc
}

type IDLoc struct {
	batchIdx int
	offset   int
}

func newIDScores[T PKType](idScores map[T]float32, idLocs map[T]IDLoc, searchParams *SearchParams, descendingOrder bool) *IDScores[T] {
	ids := make([]T, 0, len(idScores))
	for id := range idScores {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		if idScores[ids[i]] == idScores[ids[j]] {
			return ids[i] < ids[j]
		}
		if descendingOrder {
			return idScores[ids[i]] > idScores[ids[j]]
		} else {
			return idScores[ids[i]] < idScores[ids[j]]
		}
	})
	topk := searchParams.offset + searchParams.limit
	if int64(len(ids)) > topk {
		ids = ids[:topk]
	}
	ret := IDScores[T]{
		make([]T, 0, searchParams.limit),
		make([]float32, 0, searchParams.limit),
		0,
		make([]IDLoc, 0, searchParams.limit),
	}
	for index := searchParams.offset; index < int64(len(ids)); index++ {
		score := idScores[ids[index]]
		if searchParams.roundDecimal != -1 {
			multiplier := math.Pow(10.0, float64(searchParams.roundDecimal))
			score = float32(math.Floor(float64(score)*multiplier+0.5) / multiplier)
		}
		ret.ids = append(ret.ids, ids[index])
		ret.scores = append(ret.scores, score)
		ret.locations = append(ret.locations, idLocs[ids[index]])
	}
	ret.size = int64(len(ret.ids))
	return &ret
}

func groupScore[T PKType](group *Group[T], scorerType string) (float32, error) {
	switch scorerType {
	case maxScorer:
		return group.maxScore, nil
	case sumScorer:
		return group.sumScore, nil
	case avgScorer:
		if len(group.idList) == 0 {
			return 0, merr.WrapErrParameterInvalid(1, len(group.idList),
				"input group for score must have at least one id, must be sth wrong within code")
		}
		return group.sumScore / float32(len(group.idList)), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("input group scorer type: %s is not supported!", scorerType)
	}
}

type Group[T PKType] struct {
	idList     []T
	scoreList  []float32
	groupVal   any
	maxScore   float32
	sumScore   float32
	finalScore float32
}

func newGroupingIDScores[T PKType](idScores map[T]float32, idLocations map[T]IDLoc, searchParams *SearchParams, idGroup map[any]any) (*IDScores[T], error) {
	ids := make([]T, 0, len(idScores))
	for id := range idScores {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		if idScores[ids[i]] == idScores[ids[j]] {
			return ids[i] < ids[j]
		}
		return idScores[ids[i]] > idScores[ids[j]]
	})

	buckets := make(map[interface{}]*Group[T])
	for _, id := range ids {
		score := idScores[id]
		groupVal := idGroup[id]
		if buckets[groupVal] == nil {
			buckets[groupVal] = &Group[T]{
				idList:    make([]T, 0),
				scoreList: make([]float32, 0),
				groupVal:  groupVal,
			}
		}
		if int64(len(buckets[groupVal].idList)) >= searchParams.groupSize {
			continue
		}
		buckets[groupVal].idList = append(buckets[groupVal].idList, id)
		buckets[groupVal].scoreList = append(buckets[groupVal].scoreList, idScores[id])
		if score > buckets[groupVal].maxScore {
			buckets[groupVal].maxScore = score
		}
		buckets[groupVal].sumScore += score
	}

	groupList := make([]*Group[T], len(buckets))
	idx := 0
	var err error
	for _, group := range buckets {
		if group.finalScore, err = groupScore(group, searchParams.groupScore); err != nil {
			return nil, err
		}
		groupList[idx] = group
		idx += 1
	}
	sort.Slice(groupList, func(i, j int) bool {
		if groupList[i].finalScore == groupList[j].finalScore {
			if len(groupList[i].idList) == len(groupList[j].idList) {
				// if final score and size of group are both equal
				// choose the group with smaller first key
				// here, it's guaranteed all group having at least one id in the idList
				return groupList[i].idList[0] < groupList[j].idList[0]
			}
			// choose the larger group when scores are equal
			return len(groupList[i].idList) > len(groupList[j].idList)
		}
		return groupList[i].finalScore > groupList[j].finalScore
	})

	if int64(len(groupList)) > searchParams.limit+searchParams.offset {
		groupList = groupList[:searchParams.limit+searchParams.offset]
	}

	ret := IDScores[T]{
		make([]T, 0, searchParams.limit),
		make([]float32, 0, searchParams.limit),
		0,
		make([]IDLoc, 0, searchParams.limit),
	}
	for index := int(searchParams.offset); index < len(groupList); index++ {
		group := groupList[index]
		for i, score := range group.scoreList {
			// idList and scoreList must have same length
			if searchParams.roundDecimal != -1 {
				multiplier := math.Pow(10.0, float64(searchParams.roundDecimal))
				score = float32(math.Floor(float64(score)*multiplier+0.5) / multiplier)
			}
			ret.scores = append(ret.scores, score)
			ret.ids = append(ret.ids, group.idList[i])
			ret.locations = append(ret.locations, idLocations[group.idList[i]])
		}
	}
	ret.size = int64(len(ret.ids))
	return &ret, nil
}

func getField(inputField *schemapb.FieldData, start int64, size int64) (any, error) {
	switch inputField.Type {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetIntData() != nil {
			return inputField.GetScalars().GetIntData().Data[start : start+size], nil
		}
		return []int32{}, nil
	case schemapb.DataType_Int64:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetLongData() != nil {
			return inputField.GetScalars().GetLongData().Data[start : start+size], nil
		}
		return []int64{}, nil
	case schemapb.DataType_Float:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetFloatData() != nil {
			return inputField.GetScalars().GetFloatData().Data[start : start+size], nil
		}
		return []float32{}, nil
	case schemapb.DataType_Double:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetDoubleData() != nil {
			return inputField.GetScalars().GetDoubleData().Data[start : start+size], nil
		}
		return []float64{}, nil
	case schemapb.DataType_Timestamptz:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetTimestamptzData() != nil {
			return inputField.GetScalars().GetTimestamptzData().Data[start : start+size], nil
		}
		return []int64{}, nil
	case schemapb.DataType_Bool:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetBoolData() != nil {
			return inputField.GetScalars().GetBoolData().Data[start : start+size], nil
		}
		return []bool{}, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetStringData() != nil {
			return inputField.GetScalars().GetStringData().Data[start : start+size], nil
		}
		return []string{}, nil
	default:
		return nil, fmt.Errorf("Unsupported field type:%s", inputField.Type.String())
	}
}

func getIds(ids *schemapb.IDs, start int64, size int64) any {
	if ids == nil {
		return nil
	}
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		if ids.GetIntId() != nil && ids.GetIntId().GetData() != nil {
			return ids.GetIntId().GetData()[start : start+size]
		}
		return []int64{}
	case *schemapb.IDs_StrId:
		if ids.GetStrId() != nil && ids.GetStrId().GetData() != nil {
			return ids.GetStrId().GetData()[start : start+size]
		}
		return []string{}
	}
	return nil
}

type scoreMergeFunc[T PKType] func(cols []*columns) map[T]float32

func getMergeFunc[T PKType](name string) (scoreMergeFunc[T], error) {
	switch strings.ToLower(name) {
	case "max":
		return maxMerge[T], nil
	case "avg":
		return avgMerge[T], nil
	case "sum":
		return sumMerge[T], nil
	default:
		return nil, fmt.Errorf("Unsupport score mode: [%s], only supports: [max, avg, sum]", name)
	}
}

func maxMerge[T PKType](cols []*columns) map[T]float32 {
	srcScores := make(map[T]float32)

	for _, col := range cols {
		if col.size == 0 {
			continue
		}
		scores := col.scores
		ids := col.ids.([]T)

		for idx, id := range ids {
			if score, ok := srcScores[id]; !ok {
				srcScores[id] = scores[idx]
			} else {
				srcScores[id] = max(score, scores[idx])
			}
		}
	}
	return srcScores
}

func avgMerge[T PKType](cols []*columns) map[T]float32 {
	srcScores := make(map[T]*typeutil.Pair[float32, int32])

	for _, col := range cols {
		if col.size == 0 {
			continue
		}
		scores := col.scores
		ids := col.ids.([]T)

		for idx, id := range ids {
			if _, ok := srcScores[id]; !ok {
				p := typeutil.NewPair[float32, int32](scores[idx], 1)
				srcScores[id] = &p
			} else {
				srcScores[id].A += scores[idx]
				srcScores[id].B += 1
			}
		}
	}
	retScores := make(map[T]float32, len(srcScores))
	for id, item := range srcScores {
		retScores[id] = item.A / float32(item.B)
	}
	return retScores
}

func sumMerge[T PKType](cols []*columns) map[T]float32 {
	srcScores := make(map[T]float32)

	for _, col := range cols {
		if col.size == 0 {
			continue
		}
		scores := col.scores
		ids := col.ids.([]T)

		for idx, id := range ids {
			if _, ok := srcScores[id]; !ok {
				srcScores[id] = scores[idx]
			} else {
				srcScores[id] += scores[idx]
			}
		}
	}
	return srcScores
}

func getPKType(collSchema *schemapb.CollectionSchema) (schemapb.DataType, error) {
	pkType := schemapb.DataType_None
	for _, field := range collSchema.Fields {
		if field.IsPrimaryKey {
			pkType = field.DataType
		}
	}

	if pkType == schemapb.DataType_None {
		return pkType, fmt.Errorf("Collection %s can not found pk field", collSchema.Name)
	}
	return pkType, nil
}

func genIdGroupingMap(multipSearchResultData []*schemapb.SearchResultData) (map[any]any, error) {
	idGroupValue := map[any]any{}
	for _, result := range multipSearchResultData {
		if result.GetGroupByFieldValue() == nil {
			log.Warn("Group value is nil, this is due to empty results in search reduce phase")
			continue
		}
		size := typeutil.GetSizeOfIDs(result.Ids)
		groupIter := typeutil.GetDataIterator(result.GetGroupByFieldValue())
		for i := 0; i < size; i++ {
			groupByVal := groupIter(i)
			id := typeutil.GetPK(result.Ids, int64(i))
			if _, exist := idGroupValue[id]; !exist {
				idGroupValue[id] = groupByVal
			}
		}
	}
	return idGroupValue, nil
}

type normalizeFunc func(float32) float32

func getNormalizeFunc(normScore bool, metrics string, toGreater bool) normalizeFunc {
	if !normScore {
		if !toGreater {
			return func(distance float32) float32 {
				return distance
			}
		}
		switch strings.ToUpper(metrics) {
		case metric.COSINE, metric.IP, metric.BM25:
			return func(distance float32) float32 {
				return distance
			}
		default:
			return func(distance float32) float32 {
				return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
			}
		}
	}
	switch strings.ToUpper(metrics) {
	case metric.COSINE:
		return func(distance float32) float32 {
			return (1 + distance) * 0.5
		}
	case metric.IP:
		return func(distance float32) float32 {
			return 0.5 + float32(math.Atan(float64(distance)))/math.Pi
		}
	case metric.BM25:
		return func(distance float32) float32 {
			return 2 * float32(math.Atan(float64(distance))) / math.Pi
		}
	default:
		return func(distance float32) float32 {
			return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
		}
	}
}

// analyzeMetricsType inspects the given metrics and determines
// whether they contain mixed types and what the sorting order should be.
//
// Parameters:
//
//	metrics - A list of metric names (e.g., COSINE, IP, BM25).
//
// Returns:
//
//	mixed          - true if the input contains both "larger-is-more-similar"
//	                 and "smaller-is-more-similar" metrics; false otherwise.
//	sortDescending - true if results should be sorted in descending order
//	                 (larger value = more similar, e.g., COSINE, IP, BM25);
//	                 false if results should be sorted in ascending order
//	                 (smaller value = more similar, e.g., L2 distance).
func classifyMetricsOrder(metrics []string) (mixed bool, sortDescending bool) {
	countLargerIsBetter := 0  // Larger value = more similar
	countSmallerIsBetter := 0 // Smaller value = more similar
	for _, m := range metrics {
		switch strings.ToUpper(m) {
		case metric.COSINE, metric.IP, metric.BM25:
			countLargerIsBetter++
		default:
			countSmallerIsBetter++
		}
	}
	if countLargerIsBetter > 0 && countSmallerIsBetter > 0 {
		return true, true
	}
	return false, countSmallerIsBetter == 0
}
