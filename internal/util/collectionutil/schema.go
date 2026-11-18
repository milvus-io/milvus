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

package collectionutil

import (
	"sort"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ApplySettings applies collection-level mmap and warmup settings to all fields.
// It returns an owned schema, preserving explicit field and struct overrides.
// Both QueryCoord loads and QueryNode raw WAL/sync schemas must use this path.
// Schema properties take precedence over separately supplied collection properties.
func ApplySettings(schema *schemapb.CollectionSchema,
	collectionProperties []*commonpb.KeyValuePair,
) *schemapb.CollectionSchema {
	// first clone the schema then apply some settings to it
	schemaCloned := typeutil.Clone(schema)
	schemaCloned.Properties = mergeCollectionProps(schemaCloned.Properties, collectionProperties)

	schemaCloned = ApplyMmapSetting(schemaCloned, schemaCloned.GetProperties())
	schemaCloned = ApplyWarmupSetting(schemaCloned, schemaCloned.GetProperties())

	// Carry auto-warmup to segcore even when a vector has no IndexInfo.
	if _, exist := common.GetWarmupPolicyByKey(common.WarmupVectorIndexKey, schemaCloned.GetProperties()...); !exist &&
		AutoWarmupEnabled(schemaCloned.GetProperties()) {
		schemaCloned.Properties = append(schemaCloned.Properties, &commonpb.KeyValuePair{
			Key:   common.WarmupVectorIndexKey,
			Value: common.WarmupSync,
		})
	}
	sort.Slice(schemaCloned.Properties, func(i, j int) bool {
		return schemaCloned.Properties[i].GetKey() < schemaCloned.Properties[j].GetKey()
	})
	return schemaCloned
}

// ApplyMmapSetting applies collection and struct mmap defaults in place,
// preserving explicit field settings. Callers must pass an owned schema.
func ApplyMmapSetting(schema *schemapb.CollectionSchema,
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

// AutoWarmupEnabled reports whether the collection should use automatic warmup.
// Returns true when the AutoWarmupForNonPKIsolationCollection config is enabled AND the collection
// does NOT have partition key isolation.
func AutoWarmupEnabled(collectionProperties []*commonpb.KeyValuePair) bool {
	if !paramtable.Get().QueryCoordCfg.AutoWarmupForNonPKIsolationCollection.GetAsBool() {
		return false
	}
	isPKI, err := common.IsPartitionKeyIsolationKvEnabled(collectionProperties...)
	return err == nil && !isPKI
}

// ApplyWarmupSetting applies collection-level warmup setting to all fields
// and propagates struct-level warmup to nested fields.
// Priority: field-level > struct-level > collection-level > automatic warmup (scalar only)
// Collection-level granular keys: warmup.scalarField, warmup.vectorField
func ApplyWarmupSetting(schema *schemapb.CollectionSchema,
	collectionProperties []*commonpb.KeyValuePair,
) *schemapb.CollectionSchema {
	// Get collection-level granular warmup policies
	scalarFieldWarmup, scalarFieldExist := common.GetWarmupPolicyByKey(common.WarmupScalarFieldKey, collectionProperties...)
	vectorFieldWarmup, vectorFieldExist := common.GetWarmupPolicyByKey(common.WarmupVectorFieldKey, collectionProperties...)
	autoWarmup := AutoWarmupEnabled(collectionProperties)

	// Apply collection-level warmup to regular fields
	for _, field := range schema.GetFields() {
		// field-level warmup setting has higher priority, skip if field already has warmup setting
		if common.FieldHasWarmupKey(schema, field.GetFieldID()) {
			continue
		}

		isVector := typeutil.IsVectorType(field.GetDataType())
		if isVector && vectorFieldExist {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.WarmupKey,
				Value: vectorFieldWarmup,
			})
		} else if !isVector && scalarFieldExist {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.WarmupKey,
				Value: scalarFieldWarmup,
			})
		} else if autoWarmup && !isVector {
			// AutoWarmupEnabled fallback: force sync warmup for scalar fields (not vector fields)
			// vector fields are excluded from autowarmup due to its large size
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.WarmupKey,
				Value: common.WarmupSync,
			})
		}
	}

	// Apply warmup to struct array fields and their nested fields
	for _, structField := range schema.GetStructArrayFields() {
		// Get struct field's warmup setting
		structFieldWarmup, structHasWarmup := common.GetWarmupPolicy(structField.GetTypeParams()...)

		// Apply warmup setting to fields inside struct
		for _, field := range structField.GetFields() {
			// Skip if field already has warmup setting
			if common.FieldHasWarmupKey(schema, field.GetFieldID()) {
				continue
			}

			// Priority: struct field setting > collection setting > AutoWarmupEnabled
			if structHasWarmup {
				field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
					Key:   common.WarmupKey,
					Value: structFieldWarmup,
				})
			} else {
				isVector := typeutil.IsVectorType(field.GetDataType())
				if isVector && vectorFieldExist {
					field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
						Key:   common.WarmupKey,
						Value: vectorFieldWarmup,
					})
				} else if !isVector && scalarFieldExist {
					field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
						Key:   common.WarmupKey,
						Value: scalarFieldWarmup,
					})
				} else if autoWarmup && !isVector {
					// AutoWarmupEnabled fallback for struct nested scalar fields
					// vector fields are excluded from autowarmup due to its large size
					field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
						Key:   common.WarmupKey,
						Value: common.WarmupSync,
					})
				}
			}
		}
	}
	return schema
}

func mergeCollectionProps(schemaProps []*commonpb.KeyValuePair, collectionProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	// Merge the collectionProps and schemaProps maps, giving priority to the values in schemaProps if there are duplicate keys.
	props := make(map[string]string)
	for _, p := range collectionProps {
		props[p.GetKey()] = p.GetValue()
	}
	for _, p := range schemaProps {
		props[p.GetKey()] = p.GetValue()
	}
	var ret []*commonpb.KeyValuePair
	for k, v := range props {
		ret = append(ret, &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		})
	}
	return ret
}
