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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A collection created with preserved field IDs (the preserve_field_ids
// property or restore from a snapshot) keeps the IDs of its schema instead of
// allocating new ones. Restore tools rely on it to map stored columns to fields
// by ID, so the IDs must be a valid allocation: the checks below reject a
// schema that no collection could have produced. Every violation is caused by
// the request content and is an input error.

// hasCreateCollectionPartitionKey reports whether schema uses partition-key
// partitioning once RootCoord finishes preparing it. With preserve_field_ids=true,
// system fields are recreated by RootCoord, so caller-supplied flags on them
// are ignored and a namespace collection in partition_key mode gets its
// partition-key role even when the minimal $namespace_id entry omits
// IsPartitionKey=true.
func hasCreateCollectionPartitionKey(schema *schemapb.CollectionSchema, properties []*commonpb.KeyValuePair, preserveFieldIDs bool) bool {
	if !preserveFieldIDs {
		_, err := typeutil.GetPartitionKeyFieldSchema(schema)
		return err == nil
	}
	for _, field := range schema.GetFields() {
		switch field.GetName() {
		case RowIDFieldName, TimeStampFieldName, MetaFieldName, NamespaceFieldName:
			continue
		}
		if field.GetIsPartitionKey() {
			return true
		}
	}
	return schema.GetEnableNamespace() && !typeutil.IsExternalCollection(schema) && common.IsNamespaceModePartitionKey(properties...)
}

// stripCreateOnlyProperties removes request-scoped CreateCollection directives
// (such as preserve_field_ids) before writing the WAL message and collection
// metadata.
//
// Design & compatibility notes:
//   - External entry point: preserve_field_ids stays in CreateCollectionRequest.properties
//     (introduced in v3.0.0 via #44361 and exposed by SDKs/REST via properties)
//     rather than a separate proto field, avoiding a dual-track contract and
//     preventing silent protobuf unknown-field dropping (which would fall back
//     to sequential ID reallocation and misalign columns) when newer clients
//     talk to older patch-level servers.
//   - Why strip before the WAL message: preserve_field_ids specifies how this
//     CreateCollection call assigns IDs, not a persistent property of the
//     resulting collection. Once prepareSchema materializes the preserved
//     field IDs, function field IDs, and max_field_id onto CollectionSchema,
//     subsequent DDLs, WAL recovery replay, and CDC secondaries
//     (createCollectionV1AckCallback) never read preserve_field_ids. Terminating it
//     here prevents DescribeCollection property reuse on new collections from
//     failing preserved-ID validation, avoids stale leakage into re-backups or
//     duplicate KV accumulation across repeated RestoreSnapshot rounds, and
//     keeps WAL payloads, etcd metadata, snapshots, and featureusage CustomKey
//     metrics clean.
//   - Pre-existing collections: collections persisted on v3.0.0–v3.0.6 before
//     this change retain their stored model.Collection.Properties in etcd, while
//     read projections (convertModelToDesc) also run
//     stripCreateOnlyProperties so DescribeCollection and newly created
//     snapshots never return legacy preserve_field_ids entries either; see
//     matchesPreUpgradePreservedCollection for rolling-upgrade retry handling.
func stripCreateOnlyProperties(properties []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	if len(properties) == 0 {
		return nil
	}
	hasCreateOnly := false
	for _, kv := range properties {
		if kv.GetKey() == util.PreserveFieldIdsKey {
			hasCreateOnly = true
			break
		}
	}
	if !hasCreateOnly {
		return properties
	}
	filtered := make([]*commonpb.KeyValuePair, 0, len(properties)-1)
	for _, kv := range properties {
		if kv.GetKey() == util.PreserveFieldIdsKey {
			continue
		}
		filtered = append(filtered, kv)
	}
	return filtered
}

// matchesPreUpgradePreservedCollection keeps CreateCollection retries
// idempotent across rolling upgrades when a pre-upgrade coordinator (v3.0.0–v3.0.6)
// persisted the create-only preserve_field_ids directive (whether true or
// false) in model.Collection.Properties.
func (t *createCollectionTask) matchesPreUpgradePreservedCollection(existing, candidate *model.Collection) bool {
	existingValue, found := funcutil.TryGetAttrByKeyFromRepeatedKV(util.PreserveFieldIdsKey, existing.Properties)
	if !found {
		return false
	}
	if (existingValue == "true") != t.preserveFieldID {
		return false
	}
	normalized := *existing
	normalized.Properties = stripCreateOnlyProperties(existing.Properties)
	return normalized.Equal(*candidate)
}

// validatePreservedFieldIDs checks the field IDs of a schema whose IDs are
// preserved. RowID and Timestamp are skipped; RootCoord re-creates them.
func validatePreservedFieldIDs(schema *schemapb.CollectionSchema) error {
	owners := make(map[int64]string)
	check := func(name string, id int64) error {
		if id < StartOfUserFieldID {
			return merr.WrapErrParameterInvalidMsg(
				"field %s has field ID %d; with %s=true every field must carry its source field ID, which is at least %d",
				name, id, util.PreserveFieldIdsKey, StartOfUserFieldID)
		}
		if owner, ok := owners[id]; ok {
			return merr.WrapErrParameterInvalidMsg(
				"fields %s and %s have the same field ID %d; with %s=true field IDs must be unique",
				owner, name, id, util.PreserveFieldIdsKey)
		}
		owners[id] = name
		return nil
	}

	metaFields, namespaceFields := 0, 0
	for _, field := range schema.GetFields() {
		switch field.GetName() {
		case RowIDFieldName, TimeStampFieldName:
			continue
		case MetaFieldName:
			metaFields++
		case NamespaceFieldName:
			namespaceFields++
		}
		if err := check(field.GetName(), field.GetFieldID()); err != nil {
			return err
		}
	}
	for _, structField := range schema.GetStructArrayFields() {
		if err := check(structField.GetName(), structField.GetFieldID()); err != nil {
			return err
		}
		for _, subField := range structField.GetFields() {
			if err := check(subField.GetName(), subField.GetFieldID()); err != nil {
				return err
			}
			// IDs are allocated in increasing order and a struct's sub-fields
			// always get IDs above the struct's own ID. Readers that fill a
			// sub-field missing from old segments depend on this order.
			if subField.GetFieldID() <= structField.GetFieldID() {
				return merr.WrapErrParameterInvalidMsg(
					"sub-field %s has field ID %d, which is not greater than the field ID %d of its struct %s",
					subField.GetName(), subField.GetFieldID(), structField.GetFieldID(), structField.GetName())
			}
		}
	}

	if metaFields > 1 {
		return merr.WrapErrParameterInvalidMsg("the schema contains more than one %s field", MetaFieldName)
	}
	if namespaceFields > 1 {
		return merr.WrapErrParameterInvalidMsg("the schema contains more than one %s field", NamespaceFieldName)
	}
	if schema.GetEnableDynamicField() && metaFields == 0 {
		return merr.WrapErrParameterInvalidMsg(
			"collection %s enables the dynamic field, so with %s=true the schema fields must include the dynamic field: "+
				"name %s, is_dynamic=true, data type JSON, and the source field ID. Only its field ID is used; "+
				"Milvus creates the field with its standard attributes",
			schema.GetName(), util.PreserveFieldIdsKey, MetaFieldName)
	}
	if !schema.GetEnableDynamicField() && metaFields > 0 {
		return merr.WrapErrParameterInvalidMsg(
			"the schema contains the dynamic field %s but collection %s does not enable the dynamic field",
			MetaFieldName, schema.GetName())
	}
	return nil
}

// checkPreservedNamespaceFieldID runs after RootCoord decided whether the
// collection gets a namespace field: the request must carry its source ID
// exactly when it does.
func checkPreservedNamespaceFieldID(created bool, preservedID int64) error {
	if created && preservedID < 0 {
		return merr.WrapErrParameterInvalidMsg(
			"the collection gets a namespace field, so with %s=true the schema fields must include it: "+
				"name %s, data type VarChar, and the source field ID. Only its field ID is used; "+
				"Milvus creates the field with its standard attributes",
			util.PreserveFieldIdsKey, NamespaceFieldName)
	}
	if !created && preservedID >= 0 {
		return merr.WrapErrParameterInvalidMsg(
			"the schema contains the namespace field %s but the collection does not get a namespace field",
			NamespaceFieldName)
	}
	return nil
}

// validatePreservedMaxFieldID rejects a max_field_id property that is not a
// non-negative integer. With preserved IDs the property is the only record of
// the IDs that dropped fields used, so a value that cannot be read must not be
// ignored.
func validatePreservedMaxFieldID(properties []*commonpb.KeyValuePair) error {
	value, ok := funcutil.TryGetAttrByKeyFromRepeatedKV(common.MaxFieldIDKey, properties)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return merr.WrapErrParameterInvalidMsg("property %s must be a non-negative integer, got %q", common.MaxFieldIDKey, value)
	}
	return nil
}

// resolvePreservedFunctionFieldIDs keeps every function ID and resolves the
// input and output field IDs from the field names against the preserved
// field IDs. IDs sent with the request must equal the resolved ones.
func resolvePreservedFunctionFieldIDs(schema *schemapb.CollectionSchema) error {
	name2id := make(map[string]int64)
	for _, field := range schema.GetFields() {
		name2id[field.GetName()] = field.GetFieldID()
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			name2id[field.GetName()] = field.GetFieldID()
		}
	}
	resolve := func(function *schemapb.FunctionSchema, kind string, names []string, sent []int64) ([]int64, error) {
		if len(sent) != 0 && len(sent) != len(names) {
			return nil, merr.WrapErrParameterInvalidMsg(
				"function %s has %d %s field names but %d %s field IDs",
				function.GetName(), len(names), kind, len(sent), kind)
		}
		ids := make([]int64, len(names))
		for i, name := range names {
			id, ok := name2id[name]
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("%s field %s of function %s not found", kind, name, function.GetName())
			}
			if len(sent) != 0 && sent[i] != id {
				return nil, merr.WrapErrParameterInvalidMsg(
					"function %s refers to %s field %s by field ID %d, but the field has field ID %d",
					function.GetName(), kind, name, sent[i], id)
			}
			ids[i] = id
		}
		return ids, nil
	}

	functionOwners := make(map[int64]string)
	for _, function := range schema.GetFunctions() {
		if function.GetId() < StartOfUserFunctionID {
			return merr.WrapErrParameterInvalidMsg(
				"function %s has ID %d; with %s=true every function must carry its source function ID, which is at least %d",
				function.GetName(), function.GetId(), util.PreserveFieldIdsKey, StartOfUserFunctionID)
		}
		if owner, ok := functionOwners[function.GetId()]; ok {
			return merr.WrapErrParameterInvalidMsg(
				"functions %s and %s have the same ID %d", owner, function.GetName(), function.GetId())
		}
		functionOwners[function.GetId()] = function.GetName()

		inputs, err := resolve(function, "input", function.GetInputFieldNames(), function.GetInputFieldIds())
		if err != nil {
			return err
		}
		outputs, err := resolve(function, "output", function.GetOutputFieldNames(), function.GetOutputFieldIds())
		if err != nil {
			return err
		}
		function.InputFieldIds = inputs
		function.OutputFieldIds = outputs
	}
	return nil
}
