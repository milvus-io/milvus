// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func isAlpha(c uint8) bool {
	if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
		return false
	}
	return true
}

func isNumber(c uint8) bool {
	if c < '0' || c > '9' {
		return false
	}
	return true
}

func validateCollectionNameOrAlias(entity, entityType string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return fmt.Errorf("Collection %s should not be empty", entityType)
	}

	invalidMsg := fmt.Sprintf("Invalid collection %s: %s. ", entityType, entity)
	if int64(len(entity)) > Params.MaxNameLength {
		msg := invalidMsg + fmt.Sprintf("The length of a collection %s must be less than ", entityType) +
			strconv.FormatInt(Params.MaxNameLength, 10) + " characters."
		return errors.New(msg)
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + fmt.Sprintf("The first character of a collection %s must be an underscore or letter.", entityType)
		return errors.New(msg)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + fmt.Sprintf("Collection %s can only contain numbers, letters, dollars and underscores.", entityType)
			return errors.New(msg)
		}
	}
	return nil
}

func ValidateCollectionAlias(collAlias string) error {
	return validateCollectionNameOrAlias(collAlias, "alias")
}

func ValidateCollectionName(collName string) error {
	return validateCollectionNameOrAlias(collName, "name")
}

func ValidatePartitionTag(partitionTag string, strictCheck bool) error {
	partitionTag = strings.TrimSpace(partitionTag)

	invalidMsg := "Invalid partition name: " + partitionTag + ". "
	if partitionTag == "" {
		msg := invalidMsg + "Partition name should not be empty."
		return errors.New(msg)
	}

	if int64(len(partitionTag)) > Params.MaxNameLength {
		msg := invalidMsg + "The length of a partition tag must be less than " +
			strconv.FormatInt(Params.MaxNameLength, 10) + " characters."
		return errors.New(msg)
	}

	if strictCheck {
		firstChar := partitionTag[0]
		if firstChar != '_' && !isAlpha(firstChar) && !isNumber(firstChar) {
			msg := invalidMsg + "The first character of a partition tag must be an underscore or letter."
			return errors.New(msg)
		}

		tagSize := len(partitionTag)
		for i := 1; i < tagSize; i++ {
			c := partitionTag[i]
			if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
				msg := invalidMsg + "Partition tag can only contain numbers, letters, dollars and underscores."
				return errors.New(msg)
			}
		}
	}

	return nil
}

func ValidateFieldName(fieldName string) error {
	fieldName = strings.TrimSpace(fieldName)

	if fieldName == "" {
		return errors.New("Field name should not be empty")
	}

	invalidMsg := "Invalid field name: " + fieldName + ". "
	if int64(len(fieldName)) > Params.MaxNameLength {
		msg := invalidMsg + "The length of a field name must be less than " +
			strconv.FormatInt(Params.MaxNameLength, 10) + " characters."
		return errors.New(msg)
	}

	firstChar := fieldName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of a field name must be an underscore or letter."
		return errors.New(msg)
	}

	fieldNameSize := len(fieldName)
	for i := 1; i < fieldNameSize; i++ {
		c := fieldName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Field name cannot only contain numbers, letters, and underscores."
			return errors.New(msg)
		}
	}
	return nil
}

func ValidateDimension(dim int64, isBinary bool) error {
	if dim <= 0 || dim > Params.MaxDimension {
		return fmt.Errorf("invalid dimension: %d. should be in range 1 ~ %d", dim, Params.MaxDimension)
	}
	if isBinary && dim%8 != 0 {
		return fmt.Errorf("invalid dimension: %d. should be multiple of 8. ", dim)
	}
	return nil
}

func ValidateVectorFieldMetricType(field *schemapb.FieldSchema) error {
	if (field.DataType != schemapb.DataType_FloatVector) && (field.DataType != schemapb.DataType_BinaryVector) {
		return nil
	}
	for _, params := range field.IndexParams {
		if params.Key == "metric_type" {
			return nil
		}
	}
	return errors.New("vector float without metric_type")
}

func ValidateDuplicatedFieldName(fields []*schemapb.FieldSchema) error {
	names := make(map[string]bool)
	for _, field := range fields {
		_, ok := names[field.Name]
		if ok {
			return errors.New("duplicated field name")
		}
		names[field.Name] = true
	}
	return nil
}

//ValidateFieldAutoID call after ValidatePrimaryKey
func ValidateFieldAutoID(coll *schemapb.CollectionSchema) error {
	var idx = -1
	for i, field := range coll.Fields {
		if field.AutoID {
			if idx != -1 {
				return fmt.Errorf("only one field can speficy AutoID with true, field name = %s, %s", coll.Fields[idx].Name, field.Name)
			}
			idx = i
			if !field.IsPrimaryKey {
				return fmt.Errorf("only primary field can speficy AutoID with true, field name = %s", field.Name)
			}
		}
	}
	return nil
}

func ValidatePrimaryKey(coll *schemapb.CollectionSchema) error {
	idx := -1
	for i, field := range coll.Fields {
		if field.IsPrimaryKey {
			if idx != -1 {
				return fmt.Errorf("there are more than one primary key, field name = %s, %s", coll.Fields[idx].Name, field.Name)
			}
			if field.DataType != schemapb.DataType_Int64 {
				return errors.New("the data type of primary key should be int64")
			}
			idx = i
		}
	}
	if idx == -1 {
		return errors.New("primary key is not specified")
	}
	return nil
}

func RepeatedKeyValToMap(kvPairs []*commonpb.KeyValuePair) (map[string]string, error) {
	resMap := make(map[string]string)
	for _, kv := range kvPairs {
		_, ok := resMap[kv.Key]
		if ok {
			return nil, fmt.Errorf("duplicated param key: %s", kv.Key)
		}
		resMap[kv.Key] = kv.Value
	}
	return resMap, nil
}

func isVector(dataType schemapb.DataType) (bool, error) {
	switch dataType {
	case schemapb.DataType_Bool, schemapb.DataType_Int8,
		schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float, schemapb.DataType_Double:
		return false, nil

	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector:
		return true, nil
	}

	return false, fmt.Errorf("invalid data type: %d", dataType)
}

func ValidateMetricType(dataType schemapb.DataType, metricTypeStrRaw string) error {
	metricTypeStr := strings.ToUpper(metricTypeStrRaw)
	switch metricTypeStr {
	case "L2", "IP":
		if dataType == schemapb.DataType_FloatVector {
			return nil
		}
	case "JACCARD", "HAMMING", "TANIMOTO", "SUBSTRUCTURE", "SUBPERSTURCTURE":
		if dataType == schemapb.DataType_BinaryVector {
			return nil
		}
	}
	return fmt.Errorf("data_type %s mismatch with metric_type %s", dataType.String(), metricTypeStrRaw)
}

func ValidateSchema(coll *schemapb.CollectionSchema) error {
	autoID := coll.AutoID
	primaryIdx := -1
	idMap := make(map[int64]int)    // fieldId -> idx
	nameMap := make(map[string]int) // name -> idx
	for idx, field := range coll.Fields {
		// check system field
		if field.FieldID < 100 {
			// System Fields, not injected yet
			return fmt.Errorf("FieldID(%d) that is less than 100 is reserved for system fields: %s", field.FieldID, field.Name)
		}

		// primary key detector
		if field.IsPrimaryKey {
			if autoID {
				return fmt.Errorf("autoId forbids primary key")
			} else if primaryIdx != -1 {
				return fmt.Errorf("there are more than one primary key, field name = %s, %s", coll.Fields[primaryIdx].Name, field.Name)
			}
			if field.DataType != schemapb.DataType_Int64 {
				return fmt.Errorf("type of primary key shoule be int64")
			}
			primaryIdx = idx
		}
		// check unique
		elemIdx, ok := idMap[field.FieldID]
		if ok {
			return fmt.Errorf("duplicate field ids: %d", coll.Fields[elemIdx].FieldID)
		}
		idMap[field.FieldID] = idx
		elemIdx, ok = nameMap[field.Name]
		if ok {
			return fmt.Errorf("duplicate field names: %s", coll.Fields[elemIdx].Name)
		}
		nameMap[field.Name] = idx

		isVec, err3 := isVector(field.DataType)
		if err3 != nil {
			return err3
		}

		if isVec {
			indexKv, err1 := RepeatedKeyValToMap(field.IndexParams)
			if err1 != nil {
				return err1
			}
			typeKv, err2 := RepeatedKeyValToMap(field.TypeParams)
			if err2 != nil {
				return err2
			}
			dimStr, ok := typeKv["dim"]
			if !ok {
				return fmt.Errorf("dim not found in type_params for vector field %s(%d)", field.Name, field.FieldID)
			}
			dim, err := strconv.Atoi(dimStr)
			if err != nil || dim < 0 {
				return fmt.Errorf("invalid dim; %s", dimStr)
			}

			metricTypeStr, ok := indexKv["metric_type"]
			if ok {
				err4 := ValidateMetricType(field.DataType, metricTypeStr)
				if err4 != nil {
					return err4
				}
			} else {
				// in C++, default type will be specified
				// do nothing
			}
		} else {
			if len(field.IndexParams) != 0 {
				return fmt.Errorf("index params is not empty for scalar field: %s(%d)", field.Name, field.FieldID)
			}
			if len(field.TypeParams) != 0 {
				return fmt.Errorf("type params is not empty for scalar field: %s(%d)", field.Name, field.FieldID)
			}
		}
	}

	if !autoID && primaryIdx == -1 {
		return fmt.Errorf("primary key is required for non autoid mode")
	}

	return nil
}
