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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type milvusIDs[T int64 | string] struct {
	data []T
}

func newMilvusIDs(ids *schemapb.IDs, pkType schemapb.DataType) any {
	switch pkType {
	case schemapb.DataType_Int64:
		return milvusIDs[int64]{ids.GetIntId().Data}
	case schemapb.DataType_String:
		return milvusIDs[string]{ids.GetStrId().Data}
	}
	return nil
}

type idSocres[T int64 | string] struct {
	idScoreMap map[T]float32
}

func newIdScores[T int64 | string]() *idSocres[T] {
	return &idSocres[T]{idScoreMap: make(map[T]float32)}
}

func (ids *idSocres[T]) exist(id T) bool {
	_, exists := ids.idScoreMap[id]
	return exists
}

func (ids *idSocres[T]) set(id T, scores float32) {
	ids.idScoreMap[id] = scores
}

func (ids *idSocres[T]) get(id T) float32 {
	return ids.idScoreMap[id]
}

type numberField[T int32 | int64 | float32 | float64] struct {
	data []T
}

func (n *numberField[T]) GetFloat64(idx int) (float64, error) {
	if len(n.data) <= idx {
		return 0.0, fmt.Errorf("Get field err, idx:%d is larger than data size:%d", idx, len(n.data))
	}
	return float64(n.data[idx]), nil
}

func getNumberic(inputField *schemapb.FieldData) (any, error) {
	switch inputField.Type {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return &numberField[int32]{inputField.GetScalars().GetIntData().Data}, nil
	case schemapb.DataType_Int64:
		return &numberField[int64]{inputField.GetScalars().GetLongData().Data}, nil
	case schemapb.DataType_Float:
		return &numberField[float32]{inputField.GetScalars().GetFloatData().Data}, nil
	case schemapb.DataType_Double:
		return &numberField[float64]{inputField.GetScalars().GetDoubleData().Data}, nil
	default:
		return nil, fmt.Errorf("Unsupported field type:%s, only support numberic field", inputField.Type.String())
	}
}
