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

package rootcoord

import (
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func getCollectionDescription(props ...*commonpb.KeyValuePair) (bool, string, []*commonpb.KeyValuePair) {
	hasDesc := false
	desc := ""
	newProperties := make([]*commonpb.KeyValuePair, 0, len(props))
	for _, p := range props {
		if p.GetKey() == common.CollectionDescription {
			hasDesc = true
			desc = p.GetValue()
		} else {
			newProperties = append(newProperties, p)
		}
	}
	return hasDesc, desc, newProperties
}

func getConsistencyLevel(props ...*commonpb.KeyValuePair) (bool, commonpb.ConsistencyLevel) {
	for _, p := range props {
		if p.GetKey() == common.ConsistencyLevel {
			value := p.GetValue()
			if lv, ok := unmarshalConsistencyLevel(value); ok {
				return true, lv
			}
		}
	}
	return false, commonpb.ConsistencyLevel(0)
}

// unmarshalConsistencyLevel unmarshals the consistency level from the value.
func unmarshalConsistencyLevel(value string) (commonpb.ConsistencyLevel, bool) {
	if level, err := strconv.ParseInt(value, 10, 32); err == nil {
		if _, ok := commonpb.ConsistencyLevel_name[int32(level)]; ok {
			return commonpb.ConsistencyLevel(level), true
		}
	} else {
		if level, ok := commonpb.ConsistencyLevel_value[value]; ok {
			return commonpb.ConsistencyLevel(level), true
		}
	}
	return commonpb.ConsistencyLevel_Strong, false
}

func DeleteProperties(oldProps []*commonpb.KeyValuePair, deleteKeys []string) []*commonpb.KeyValuePair {
	propsMap := make(map[string]string)
	for _, prop := range oldProps {
		propsMap[prop.Key] = prop.Value
	}
	for _, key := range deleteKeys {
		delete(propsMap, key)
	}
	propKV := make([]*commonpb.KeyValuePair, 0, len(propsMap))
	for key, value := range propsMap {
		propKV = append(propKV, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	return propKV
}

func ResetFieldProperties(coll *model.Collection, fieldName string, newProps []*commonpb.KeyValuePair) error {
	for i, field := range coll.Fields {
		if field.Name == fieldName {
			coll.Fields[i].TypeParams = newProps
			return nil
		}
	}
	for i, structField := range coll.StructArrayFields {
		if structField.Name == fieldName {
			coll.StructArrayFields[i].TypeParams = newProps
			return nil
		}
		for j, field := range structField.Fields {
			if field.Name == fieldName {
				coll.StructArrayFields[i].Fields[j].TypeParams = newProps
				return nil
			}
		}
	}
	return merr.WrapErrParameterInvalidMsg("field %s does not exist in collection", fieldName)
}

func GetFieldProperties(coll *model.Collection, fieldName string) ([]*commonpb.KeyValuePair, error) {
	for _, field := range coll.Fields {
		if field.Name == fieldName {
			return field.TypeParams, nil
		}
	}
	for _, structField := range coll.StructArrayFields {
		if structField.Name == fieldName {
			return structField.TypeParams, nil
		}
		for _, field := range structField.Fields {
			if field.Name == fieldName {
				return field.TypeParams, nil
			}
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("field %s does not exist in collection", fieldName)
}

func UpdateFieldPropertyParams(oldProps, updatedProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	props := make(map[string]string)
	for _, prop := range oldProps {
		props[prop.Key] = prop.Value
	}
	log.Info("UpdateFieldPropertyParams", zap.Any("oldprops", props), zap.Any("newprops", updatedProps))
	for _, prop := range updatedProps {
		props[prop.Key] = prop.Value
	}
	log.Info("UpdateFieldPropertyParams", zap.Any("newprops", props))
	propKV := make([]*commonpb.KeyValuePair, 0)
	for key, value := range props {
		propKV = append(propKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	return propKV
}
