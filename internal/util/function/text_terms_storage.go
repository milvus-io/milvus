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

package function

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// CollectInsertData analyzes the final row set of one bulk-writer batch.
func (c *TextTermCollector) CollectInsertData(data *storage.InsertData) error {
	if !c.Enabled() || data == nil {
		return nil
	}
	inputs := make(map[int64][]string)
	for _, entry := range c.runners {
		for _, fieldID := range entry.inputFieldIDs {
			if _, ok := inputs[fieldID]; ok {
				continue
			}
			fieldData, ok := data.Data[fieldID]
			if !ok {
				return merr.WrapErrFunctionFailedMsg("text term collector input field %d is missing from insert data", fieldID)
			}
			strings, ok := fieldData.(*storage.StringFieldData)
			if !ok {
				return merr.WrapErrFunctionFailedMsg("text term collector input field %d has unexpected type %T", fieldID, fieldData)
			}
			values := strings.Data
			if strings.Nullable {
				if len(strings.ValidData) != len(strings.Data) {
					return merr.WrapErrFunctionFailedMsg("nullable text term collector input field %d has %d values but %d validity entries", fieldID, len(strings.Data), len(strings.ValidData))
				}
				values = append([]string(nil), strings.Data...)
				for i, valid := range strings.ValidData {
					if !valid {
						values[i] = ""
					}
				}
			}
			inputs[fieldID] = values
		}
	}
	return c.Collect(inputs)
}

// CollectRecord analyzes projected string columns from a storage Record.
func (c *TextTermCollector) CollectRecord(record storage.Record) error {
	if !c.Enabled() || record == nil {
		return nil
	}
	inputs := make(map[int64][]string)
	for _, fieldID := range c.InputFieldIDs() {
		column := record.Column(fieldID)
		if column == nil {
			return merr.WrapErrFunctionFailedMsg("text term collector input field %d is missing from record", fieldID)
		}
		values, err := stringValuesFromArrow(column, fieldID)
		if err != nil {
			return err
		}
		inputs[fieldID] = values
	}
	return c.Collect(inputs)
}

// CollectArrowRecord analyzes an Arrow record whose column positions are
// mapped by input field ID. It is used by the StorageV3 LOB-aware reader,
// which returns resolved TEXT columns as UTF-8 arrays.
func (c *TextTermCollector) CollectArrowRecord(record arrow.Record, fieldColumns map[int64]int) error {
	if !c.Enabled() || record == nil {
		return nil
	}
	inputs := make(map[int64][]string)
	for _, fieldID := range c.InputFieldIDs() {
		columnIndex, ok := fieldColumns[fieldID]
		if !ok || columnIndex < 0 || columnIndex >= int(record.NumCols()) {
			return merr.WrapErrFunctionFailedMsg("text term collector input field %d is missing from arrow record", fieldID)
		}
		values, err := stringValuesFromArrow(record.Column(columnIndex), fieldID)
		if err != nil {
			return err
		}
		inputs[fieldID] = values
	}
	return c.Collect(inputs)
}

func stringValuesFromArrow(column arrow.Array, fieldID int64) ([]string, error) {
	strings, ok := column.(*array.String)
	if !ok {
		return nil, merr.WrapErrFunctionFailedMsg("text term collector input field %d has unexpected arrow type %T", fieldID, column)
	}
	values := make([]string, strings.Len())
	for i := 0; i < strings.Len(); i++ {
		if strings.IsNull(i) {
			continue
		}
		values[i] = strings.Value(i)
	}
	return values, nil
}
