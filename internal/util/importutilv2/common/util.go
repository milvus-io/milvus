// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func FillDynamicData(data *storage.InsertData, schema *schemapb.CollectionSchema) error {
	if !schema.GetEnableDynamicField() {
		return nil
	}
	dynamicField := typeutil.GetDynamicField(schema)
	if dynamicField == nil {
		return nil
	}
	totalRowNum := getInsertDataRowNum(data, schema)
	dynamicData := data.Data[dynamicField.GetFieldID()]
	jsonFD := dynamicData.(*storage.JSONFieldData)
	bs := []byte("{}")
	existedRowNum := dynamicData.RowNum()
	for i := 0; i < totalRowNum-existedRowNum; i++ {
		jsonFD.Data = append(jsonFD.Data, bs)
	}
	data.Data[dynamicField.GetFieldID()] = dynamicData
	return nil
}

func getInsertDataRowNum(data *storage.InsertData, schema *schemapb.CollectionSchema) int {
	fields := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	for fieldID, fd := range data.Data {
		if fields[fieldID].GetIsDynamic() {
			continue
		}
		if fd.RowNum() != 0 {
			return fd.RowNum()
		}
	}
	return 0
}

func CheckVarcharLength(str string, maxLength int64, field *schemapb.FieldSchema) error {
	if (int64)(len(str)) > maxLength {
		return fmt.Errorf("value length(%d) for field %s exceeds max_length(%d)", len(str), field.GetName(), maxLength)
	}
	return nil
}

func CheckArrayCapacity(arrLength int, maxCapacity int64, field *schemapb.FieldSchema) error {
	if (int64)(arrLength) > maxCapacity {
		return fmt.Errorf("array capacity(%d) for field %s exceeds max_capacity(%d)", arrLength, field.GetName(), maxCapacity)
	}
	return nil
}

func EstimateReadCountPerBatch(bufferSize int, schema *schemapb.CollectionSchema) (int64, error) {
	sizePerRecord, err := typeutil.EstimateMaxSizePerRecord(schema)
	if err != nil {
		return 0, err
	}
	if sizePerRecord <= 0 || bufferSize <= 0 {
		return 0, fmt.Errorf("invalid size, sizePerRecord=%d, bufferSize=%d", sizePerRecord, bufferSize)
	}
	if 1000*sizePerRecord <= bufferSize {
		return 1000, nil
	}
	ret := int64(bufferSize) / int64(sizePerRecord)
	if ret <= 0 {
		return 1, nil
	}
	return ret, nil
}

func CheckValidUTF8(s string, field *schemapb.FieldSchema) error {
	if !typeutil.IsUTF8(s) {
		return fmt.Errorf("field %s contains invalid UTF-8 data, value=%s", field.GetName(), s)
	}
	return nil
}
