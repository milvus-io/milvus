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

package schemautil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Origin of struct sub-fields.
//
// A segment written before a sub-field was added to a struct has no column for
// it. Readers synthesize that column from a stored sibling: a null struct row
// stays null, and a non-null row with N elements gets N null elements. The N
// and the row validity can only come from a sibling column that is stored in
// the old segment and still exists in the schema.
//
// An "original" sub-field is one that has been in the schema since the struct
// itself appeared: it was declared when the collection was created, or it came
// with the whole struct through AddCollectionStructField. Every segment that
// holds a row written after the struct appeared stores every original
// sub-field, so a remaining original sub-field is always a usable sibling.
//
// A sub-field added later through AlterCollectionSchema (FieldInfo.struct_path)
// is marked with common.StructSubFieldAddedKey in its type_params. Old
// collections are not migrated: a sub-field without the marker is original.
//
// Drop rule: after dropping sub-fields of a struct, at least one original
// sub-field must remain. Otherwise the drop is rejected as an input error,
// because old segments would be left with no stored sibling for the added
// sub-fields. Dropping the whole struct is always allowed.
//
// The marker is internal: rootcoord writes it, users cannot set, alter or
// delete it, and DescribeCollection does not return it.

const structSubFieldAddedValue = "true"

// IsAddedStructSubField reports whether a struct sub-field was added after its
// struct appeared, i.e. it is not an original sub-field.
func IsAddedStructSubField(field *schemapb.FieldSchema) bool {
	for _, kv := range field.GetTypeParams() {
		if kv.GetKey() == common.StructSubFieldAddedKey {
			return true
		}
	}
	return false
}

// MarkAddedStructSubField marks a sub-field that is being added to an existing
// struct. Only rootcoord calls it, when it prepares the add.
func MarkAddedStructSubField(field *schemapb.FieldSchema) {
	if IsAddedStructSubField(field) {
		return
	}
	field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
		Key:   common.StructSubFieldAddedKey,
		Value: structSubFieldAddedValue,
	})
}

// StripStructSubFieldOriginMarker returns params without the internal marker.
// It never modifies params; it returns params itself when there is nothing to
// remove.
func StripStructSubFieldOriginMarker(params []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	found := false
	for _, kv := range params {
		if kv.GetKey() == common.StructSubFieldAddedKey {
			found = true
			break
		}
	}
	if !found {
		return params
	}
	result := make([]*commonpb.KeyValuePair, 0, len(params)-1)
	for _, kv := range params {
		if kv.GetKey() != common.StructSubFieldAddedKey {
			result = append(result, kv)
		}
	}
	return result
}

// RejectStructSubFieldOriginMarker rejects a field schema supplied by a user
// that carries the internal marker.
func RejectStructSubFieldOriginMarker(field *schemapb.FieldSchema) error {
	for _, kv := range field.GetTypeParams() {
		if kv.GetKey() == common.StructSubFieldAddedKey {
			return merr.WrapErrParameterInvalidMsg("type param %s of field %q is reserved", common.StructSubFieldAddedKey, field.GetName())
		}
	}
	return nil
}

// RejectStructSubFieldOriginMarkerKeys rejects a request that sets or deletes
// the internal marker as a field property.
func RejectStructSubFieldOriginMarkerKeys(fieldName string, keys ...string) error {
	for _, key := range keys {
		if key == common.StructSubFieldAddedKey {
			return merr.WrapErrParameterInvalidMsg("property %s of field %q is reserved", common.StructSubFieldAddedKey, fieldName)
		}
	}
	return nil
}

// CheckStructKeepsOriginalSubField enforces the drop rule: remaining are the
// sub-fields of structName left after dropping droppedName, and at least one
// of them must be original. It returns an input error otherwise.
func CheckStructKeepsOriginalSubField(structName, droppedName string, remaining []*schemapb.FieldSchema) error {
	for _, field := range remaining {
		if !IsAddedStructSubField(field) {
			return nil
		}
	}
	return merr.WrapErrParameterInvalidMsg(
		"cannot drop sub-field %q of struct field %q: every remaining sub-field was added after the struct was created, and segments written before that store no data for them; drop the struct field instead",
		droppedName, structName)
}
