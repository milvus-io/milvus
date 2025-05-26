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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
}

type rerankInputs struct {
	// nqs,searchResultsIndex
	data [][]*columns

	nq int64

	// There is only fieldId in schemapb.SearchResultData, but no fieldName
	inputFieldIds []int64
}

func organizeFieldIdData(multipSearchResultData []*schemapb.SearchResultData, inputFieldIds []int64) ([]map[int64]*schemapb.FieldData, error) {
	multipIdField := []map[int64]*schemapb.FieldData{}
	for _, searchData := range multipSearchResultData {
		idField := map[int64]*schemapb.FieldData{}
		if len(searchData.FieldsData) != 0 {
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

func newRerankInputs(multipSearchResultData []*schemapb.SearchResultData, inputFieldIds []int64) (*rerankInputs, error) {
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
		for _, fieldId := range inputFieldIds {
			fieldData := multipIdField[retIdx][fieldId]
			start := int64(0)
			for i := int64(0); i < nq; i++ {
				size := searchResult.Topks[i]
				d, err := getField(fieldData, start, size)
				if err != nil {
					return nil, err
				}
				if cols[i][retIdx] == nil {
					cols[i][retIdx] = &columns{}
					cols[i][retIdx].size = size
					cols[i][retIdx].ids = getIds(searchResult.Ids, start, size)
					cols[i][retIdx].scores = searchResult.Scores[start : start+size]
				}
				cols[i][retIdx].data = append(cols[i][retIdx].data, d)
				start += size
			}
		}
	}
	return &rerankInputs{cols, nq, inputFieldIds}, nil
}

func (inputs *rerankInputs) numOfQueries() int64 {
	return inputs.nq
}

type rerankOutputs struct {
	searchResultData *schemapb.SearchResultData
}

func newRerankOutputs(searchParams *SearchParams) *rerankOutputs {
	ret := &schemapb.SearchResultData{
		NumQueries: searchParams.nq,
		TopK:       searchParams.limit,
		FieldsData: make([]*schemapb.FieldData, 0),
		Scores:     []float32{},
		Ids:        &schemapb.IDs{},
		Topks:      []int64{},
	}
	return &rerankOutputs{ret}
}

func appendResult[T PKType](outputs *rerankOutputs, ids []T, scores []float32) {
	outputs.searchResultData.Topks = append(outputs.searchResultData.Topks, int64(len(ids)))
	outputs.searchResultData.Scores = append(outputs.searchResultData.Scores, scores...)
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
	// idScores map[T]float32
	ids    []T
	scores []float32
	size   int64
}

// func (s *IDScores[T]) GetSortedIdScores() ([]T, []float32) {
// 	ids := make([]T, 0, s.size)
// 	big := func(i, j int) bool {
// 		if s.idScores[ids[i]] == s.idScores[ids[j]] {
// 			return ids[i] < ids[j]
// 		}
// 		return s.idScores[ids[i]] > s.idScores[ids[j]]
// 	}
// 	sort.Slice(ids, big)
// 	scores := make([]float32, 0, s.size)
// 	for _, id := range ids {
// 		scores = append(scores, s.idScores[id])
// 	}
// 	return ids, scores
// }

func newIDScores[T PKType](idScores map[T]float32, searchParams *SearchParams) *IDScores[T] {
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
	topk := searchParams.offset + searchParams.limit
	if int64(len(ids)) > topk {
		ids = ids[:topk]
	}
	ret := IDScores[T]{
		make([]T, 0, searchParams.limit),
		make([]float32, 0, searchParams.limit),
		0,
	}
	for index := searchParams.offset; index < int64(len(ids)); index++ {
		score := idScores[ids[index]]
		if searchParams.roundDecimal != -1 {
			multiplier := math.Pow(10.0, float64(searchParams.roundDecimal))
			score = float32(math.Floor(float64(score)*multiplier+0.5) / multiplier)
		}
		ret.ids = append(ret.ids, ids[index])
		ret.scores = append(ret.scores, score)
	}
	ret.size = int64(len(ret.ids))
	return &ret
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
	case schemapb.DataType_Bool:
		if inputField.GetScalars() != nil && inputField.GetScalars().GetBoolData() != nil {
			return inputField.GetScalars().GetBoolData().Data[start : start+size], nil
		}
		return []bool{}, nil
	case schemapb.DataType_String:
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
