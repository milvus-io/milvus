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

package utils

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

// ApplyCollectionMmapSetting applies collection and struct mmap defaults in place,
// preserving explicit field settings. Callers must pass an owned schema.
func ApplyCollectionMmapSetting(schema *schemapb.CollectionSchema,
	collectionProperties []*commonpb.KeyValuePair,
) *schemapb.CollectionSchema {
	// field mmap enabled if collection-level mmap enabled or the field mmap enabled
	collectionMmapEnabled, exist := common.IsMmapDataEnabled(collectionProperties...)
	for _, field := range schema.GetFields() {
		if exist &&
			// field-level mmap setting has higher priority than collection-level mmap setting, skip if field-level mmap enabled
			!common.FieldHasMmapKey(schema, field.GetFieldID()) {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.MmapEnabledKey,
				Value: strconv.FormatBool(collectionMmapEnabled),
			})
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		structTypeParams := structField.GetTypeParams()
		structMmapEnabled, structExist := common.IsMmapDataEnabled(structTypeParams...)

		// If struct field itself doesn't have mmap setting, inherit from collection
		if !structExist && exist &&
			!common.FieldHasMmapKey(schema, structField.GetFieldID()) {
			structField.TypeParams = append(structField.TypeParams, &commonpb.KeyValuePair{
				Key:   common.MmapEnabledKey,
				Value: strconv.FormatBool(collectionMmapEnabled),
			})
			// Update struct's mmap state since we just set it
			structMmapEnabled = collectionMmapEnabled
			structExist = true
		}

		// Apply mmap setting to fields inside struct
		for _, field := range structField.GetFields() {
			// Skip if field already has mmap setting
			if common.FieldHasMmapKey(schema, field.GetFieldID()) {
				continue
			}

			// Priority: struct field setting > collection setting
			if structExist {
				field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
					Key:   common.MmapEnabledKey,
					Value: strconv.FormatBool(structMmapEnabled),
				})
			} else if exist {
				field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
					Key:   common.MmapEnabledKey,
					Value: strconv.FormatBool(collectionMmapEnabled),
				})
			}
		}
	}
	return schema
}
