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

package json

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/samber/lo"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Validator interface {
	Validate(raw any) (Row, error)
}

type validator struct {
	name2FieldID map[string]int64
	pkField      *schemapb.FieldSchema
	dynamicField *schemapb.FieldSchema
}

func NewValidator(schema *schemapb.CollectionSchema) (Validator, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	name2FieldID := lo.SliceToMap(schema.GetFields(),
		func(field *schemapb.FieldSchema) (string, int64) {
			return field.GetName(), field.GetFieldID()
		})

	if pkField.GetAutoID() {
		delete(name2FieldID, pkField.GetName())
	}

	dynamicField, _ := typeutil.GetDynamicField(schema)
	if dynamicField != nil {
		delete(name2FieldID, dynamicField.GetName())
	}

	return &validator{
		name2FieldID: name2FieldID,
		pkField:      pkField,
		dynamicField: dynamicField,
	}, nil
}

func (v *validator) Validate(raw any) (Row, error) {
	stringMap, ok := raw.(map[string]any)
	if !ok {
		return nil, merr.WrapErrImportFailed("invalid JSON format, each row should be a key-value map")
	}
	if _, ok = stringMap[v.pkField.GetName()]; ok && v.pkField.GetAutoID() {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", v.pkField.GetName()))
	}
	dynamicValues := make(map[string]any)
	row := make(Row)
	for key, value := range stringMap {
		if fieldID, ok := v.name2FieldID[key]; ok {
			row[fieldID] = value
		} else if v.dynamicField != nil {
			// has dynamic field, put redundant pair to dynamicValues
			dynamicValues[key] = value
		} else {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the field '%s' is not defined in schema", key))
		}
	}
	for fieldName, fieldID := range v.name2FieldID {
		if _, ok = row[fieldID]; !ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("value of field '%s' is missed", fieldName))
		}
	}
	if v.dynamicField == nil {
		return row, nil
	}
	// combine the redundant pairs into dynamic field(if it has)
	err := v.combineDynamicRow(dynamicValues, row)
	if err != nil {
		return nil, err
	}
	return row, err
}

func (v *validator) combineDynamicRow(dynamicValues map[string]any, row Row) error {
	// Combine the dynamic field value
	// valid inputs:
	// case 1: {"id": 1, "vector": [], "x": 8, "$meta": "{\"y\": 8}"} ==>> {"id": 1, "vector": [], "$meta": "{\"y\": 8, \"x\": 8}"}
	// case 2: {"id": 1, "vector": [], "x": 8, "$meta": {}} ==>> {"id": 1, "vector": [], "$meta": {\"x\": 8}}
	// case 3: {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 4: {"id": 1, "vector": [], "$meta": {"x": 8}}
	// case 5: {"id": 1, "vector": [], "$meta": {}}
	// case 6: {"id": 1, "vector": [], "x": 8} ==>> {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 7: {"id": 1, "vector": []}
	dynamicFieldID := v.dynamicField.GetFieldID()
	obj, ok := row[dynamicFieldID]
	if ok {
		if len(dynamicValues) == 0 {
			return nil
		}
		if value, ok := obj.(string); ok {
			// case 1
			mp := make(map[string]any)
			desc := json.NewDecoder(strings.NewReader(value))
			desc.UseNumber()
			err := desc.Decode(&mp)
			if err != nil {
				return merr.WrapErrImportFailed(fmt.Sprintf("illegal dynamic field value, not JSON format, value:%s", value))
			}
			maps.Copy(dynamicValues, mp)
		} else if mp, ok := obj.(map[string]any); ok {
			// case 2
			maps.Copy(dynamicValues, mp)
		} else {
			return merr.WrapErrImportFailed(fmt.Sprintf("illegal dynamic field value, not JSON format, value:%s", value))
		}
		row[dynamicFieldID] = dynamicValues
		// else case 3/4/5
		return nil
	}
	if len(dynamicValues) > 0 {
		// case 6
		row[dynamicFieldID] = dynamicValues
	} else {
		// case 7
		row[dynamicFieldID] = "{}"
	}
	return nil
}
