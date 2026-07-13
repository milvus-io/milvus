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
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ValidateSchemaEvolution is the single structural admission gate for online schema change.
//
// The streamingnode write path accepts any write at or behind the current schema version
// and lets the pre-WAL transform + segcore reconcile the gap (backfill a missing
// nullable/default column, skip a column carried for a since-dropped field, skip a function
// whose input the write predates). That reconciliation is loss-free ONLY for changes that
// are pure add / pure drop / a relaxed bound AND that keep the schema graph intact. This
// gate is the one authoritative place -- independent of the proxy, which mixed-version
// nodes, internal calls and future entry points can bypass -- that rejects everything else.
//
// It rejects, deriving every rule from (oldSchema, newSchema):
//   - in-place reinterpretation of a live field id: changed data/element type, changed
//     nullability, a shrunk or removed max_length/max_capacity, a changed dim, or an
//     existing field turned into a function output;
//   - an added field that cannot be backfilled into existing rows (not nullable, no default,
//     and not a function output / dynamic field);
//   - a drop that breaks the schema graph: primary / partition / clustering key, a system
//     field, the last vector field, or a field still referenced by a surviving function;
//   - an in-place change to a surviving function (its output was produced by the old
//     model/params and cannot be reinterpreted);
//   - a newly-added function whose input field already existed: its existing rows cannot be
//     given the function output online (there is no backfill yet), so its output would be
//     silently incomplete; the input field must be added fresh alongside the function.
//
// It ALLOWS: adding a backfillable field, adding a function whose input and output fields are
// both brand new, dropping a field or a function (including disabling the dynamic field, which
// is a safe drop of $meta), and growing a bound. A dropped field id is never reused
// (max_field_id is monotonic), so a drop can never silently reinterpret old data.
//
// Note: proxy keeps its own richer request-level checks (limits, storage-version/compaction
// prerequisites, "cannot alter if loaded", TTL-property references, API routing) as fast,
// user-facing validation. This gate is the structural backstop the node's trust rests on.
func ValidateSchemaEvolution(oldSchema, newSchema *schemapb.CollectionSchema) error {
	if oldSchema == nil || newSchema == nil {
		// Collection creation, or no committed baseline to compare against.
		return nil
	}

	oldFields := fieldsByID(oldSchema)
	newFields := fieldsByID(newSchema)

	// (A) A field id present before and after must not be reinterpreted in place.
	for id, oldF := range oldFields {
		if newF, kept := newFields[id]; kept {
			if err := validateKeptFieldUnchanged(oldF, newF); err != nil {
				return err
			}
		}
	}

	// (B) A newly introduced top-level field id must be deterministically backfillable into
	// old rows. Struct-array sub-fields are skipped here: they are backfilled through their
	// nullable container (whose nullability the add-struct-field path enforces), so requiring
	// each sub-field to be individually nullable would wrongly reject a valid struct add.
	subFieldIDs := structSubFieldIDs(newSchema)
	for id, newF := range newFields {
		if _, existed := oldFields[id]; existed {
			continue
		}
		if _, isSub := subFieldIDs[id]; isSub {
			continue
		}
		if err := validateAddedField(newF); err != nil {
			return err
		}
	}

	// (C) A dropped field id must not leave the surviving schema graph dangling.
	for id, oldF := range oldFields {
		if _, kept := newFields[id]; !kept {
			if err := validateDroppedField(oldF, newSchema); err != nil {
				return err
			}
		}
	}

	// (D) A surviving function must be byte-identical; a newly-added function may reference only
	// newly-added input fields (existing rows cannot be given its output online); dropping a
	// function is fine.
	if err := validateFunctions(oldSchema, newSchema, oldFields); err != nil {
		return err
	}

	// (E) The resulting schema must be an internally consistent field/function graph. This is
	// the backstop for entry points -- notably AddCollectionField -- that can otherwise persist
	// a field with an inconsistent role (an orphan function-output field, a stray dynamic field)
	// the proxy did not reject.
	return validateSchemaGraph(newSchema)
}

func fieldsByID(schema *schemapb.CollectionSchema) map[int64]*schemapb.FieldSchema {
	all := typeutil.GetAllFieldSchemas(schema)
	m := make(map[int64]*schemapb.FieldSchema, len(all))
	for _, f := range all {
		m[f.GetFieldID()] = f
	}
	return m
}

// structSubFieldIDs returns the ids of every field nested inside a struct-array field.
func structSubFieldIDs(schema *schemapb.CollectionSchema) map[int64]struct{} {
	m := make(map[int64]struct{})
	for _, sf := range schema.GetStructArrayFields() {
		for _, f := range sf.GetFields() {
			m[f.GetFieldID()] = struct{}{}
		}
	}
	return m
}

// validateKeptFieldUnchanged rejects any in-place semantic reinterpretation of a live field.
func validateKeptFieldUnchanged(oldF, newF *schemapb.FieldSchema) error {
	name := oldF.GetName()
	if oldF.GetDataType() != newF.GetDataType() {
		return merr.WrapErrParameterInvalidMsg(
			"cannot change the data type of field %q in place; add a new field, backfill, then drop the old one", name)
	}
	if oldF.GetElementType() != newF.GetElementType() {
		return merr.WrapErrParameterInvalidMsg("cannot change the element type of field %q in place", name)
	}
	if oldF.GetNullable() != newF.GetNullable() {
		return merr.WrapErrParameterInvalidMsg("cannot change the nullability of field %q in place", name)
	}
	// Reject only false->true: pointing a function at an already-populated live field would
	// reinterpret its existing data (the closed P0). The reverse, true->false, is a detach --
	// dropping the function while keeping its output field as a plain field holding the data
	// it already produced -- which is exactly how DropCollectionFunction works, and is safe.
	if !oldF.GetIsFunctionOutput() && newF.GetIsFunctionOutput() {
		return merr.WrapErrParameterInvalidMsg(
			"cannot turn existing field %q into a function output; a function output must be a brand-new empty field", name)
	}
	return validateBoundNotTightened(name, oldF.GetTypeParams(), newF.GetTypeParams())
}

// validateAddedField rejects a new field that cannot be backfilled into existing rows, or
// that introduces a role which cannot be added online.
func validateAddedField(f *schemapb.FieldSchema) error {
	name := f.GetName()
	if f.GetIsPrimaryKey() {
		return merr.WrapErrParameterInvalidMsg("cannot add a primary key field %q online", name)
	}
	if f.GetFieldID() < common.StartOfUserFieldID {
		return merr.WrapErrParameterInvalidMsg("cannot add a system field %q online", name)
	}
	// A function output is filled by the function/backfill; the dynamic $meta field is
	// backfilled with {} by the system. Every other added field must carry its own way to
	// backfill existing rows: be nullable (backfilled null) or carry a default value.
	if f.GetIsFunctionOutput() || f.GetIsDynamic() {
		return nil
	}
	if !f.GetNullable() && f.GetDefaultValue() == nil {
		return merr.WrapErrParameterInvalidMsg(
			"cannot add non-nullable field %q without a default value: existing rows cannot be backfilled", name)
	}
	return nil
}

// validateDroppedField rejects a drop that would break the schema graph that survives.
func validateDroppedField(f *schemapb.FieldSchema, newSchema *schemapb.CollectionSchema) error {
	name := f.GetName()
	switch {
	case f.GetIsPrimaryKey():
		return merr.WrapErrParameterInvalidMsg("cannot drop primary key field %q", name)
	case f.GetIsPartitionKey():
		return merr.WrapErrParameterInvalidMsg("cannot drop partition key field %q", name)
	case f.GetIsClusteringKey():
		return merr.WrapErrParameterInvalidMsg("cannot drop clustering key field %q", name)
	case f.GetFieldID() < common.StartOfUserFieldID:
		return merr.WrapErrParameterInvalidMsg("cannot drop system field %q", name)
	}
	if fn := functionReferencing(newSchema, f.GetFieldID()); fn != "" {
		return merr.WrapErrParameterInvalidMsg(
			"cannot drop field %q: it is still referenced by function %q; drop the function first", name, fn)
	}
	if typeutil.IsVectorType(f.GetDataType()) && !hasAnyVectorField(newSchema) {
		return merr.WrapErrParameterInvalidMsg("cannot drop the last vector field %q", name)
	}
	return nil
}

// validateFunctions enforces two rules on the function set:
//   - a surviving function (same id before and after) must be byte-identical: its existing
//     output was produced by the old model/params and cannot be reinterpreted in place;
//   - a newly-added function may reference only newly-added input fields. If its input field
//     already existed, the collection may already hold rows for that field that the function
//     was never run over, and there is no online backfill to fill their output -- so the new
//     function's output would be silently incomplete on existing data. Requiring a brand-new
//     input keeps it consistent: existing rows carry neither the input nor the output, and only
//     rows written after the change carry both. Deriving this from the schema alone (an input
//     that did not exist before) avoids querying row counts; it is intentionally conservative
//     (it also rejects adding a function onto a pre-existing but empty field). Backfilling onto
//     existing data is deferred to the online-backfill work.
func validateFunctions(oldSchema, newSchema *schemapb.CollectionSchema, oldFields map[int64]*schemapb.FieldSchema) error {
	oldFnByID := make(map[int64]*schemapb.FunctionSchema)
	for _, fn := range oldSchema.GetFunctions() {
		oldFnByID[fn.GetId()] = fn
	}
	for _, newFn := range newSchema.GetFunctions() {
		if oldFn, survived := oldFnByID[newFn.GetId()]; survived {
			if !proto.Equal(oldFn, newFn) {
				return merr.WrapErrParameterInvalidMsg(
					"cannot alter function %q in place: its existing output was produced by the old model/params and cannot be reinterpreted; add a new function with a fresh output field, backfill it, then drop the old function", oldFn.GetName())
			}
			continue // surviving and unchanged -- allowed
		}
		// Newly-added function: every input field must be brand new.
		for _, inID := range newFn.GetInputFieldIds() {
			if pre, existed := oldFields[inID]; existed {
				return merr.WrapErrParameterInvalidMsg(
					"cannot add function %q onto existing field %q: its existing rows cannot be given the function output online (no backfill yet); add the input field together with the function as new, or define the function at collection creation",
					newFn.GetName(), pre.GetName())
			}
		}
	}
	return nil
}

// validateSchemaGraph checks that schema is an internally consistent field/function graph. These
// are invariants every well-formed schema already holds (create-time validation enforces them);
// enforcing them at the admission gate closes the entry points that can otherwise persist an
// inconsistent schema the proxy did not vet -- most concretely AddCollectionField, which will
// happily store a field carrying IsFunctionOutput or IsDynamic with nothing to back it. It checks:
//   - every function input / output field id exists in the schema;
//   - no field is the output of more than one function;
//   - a field marked IsFunctionOutput is actually produced by some function (no orphan output
//     field, which the user cannot write and no function fills, so it stays permanently empty);
//   - the dynamic field is unique, named $meta, JSON typed, and present exactly when
//     EnableDynamicField is set.
//
// It deliberately does NOT flag a function-output field left un-marked (the reverse direction):
// whether the un-bypassable pre-broadcast paths always stamp IsFunctionOutput on the new output
// field is not guaranteed here, and rejecting that would risk a false positive; the orphan
// direction above is the one AddCollectionField can actually reach.
func validateSchemaGraph(schema *schemapb.CollectionSchema) error {
	allFields := fieldsByID(schema)
	outputOwner := make(map[int64]string)
	for _, fn := range schema.GetFunctions() {
		for _, id := range fn.GetInputFieldIds() {
			if _, ok := allFields[id]; !ok {
				return merr.WrapErrParameterInvalidMsg("function %q references a non-existent input field id %d", fn.GetName(), id)
			}
		}
		for _, id := range fn.GetOutputFieldIds() {
			if _, ok := allFields[id]; !ok {
				return merr.WrapErrParameterInvalidMsg("function %q references a non-existent output field id %d", fn.GetName(), id)
			}
			if owner, dup := outputOwner[id]; dup {
				return merr.WrapErrParameterInvalidMsg("field id %d is an output of more than one function (%q and %q)", id, owner, fn.GetName())
			}
			outputOwner[id] = fn.GetName()
		}
	}

	for _, f := range schema.GetFields() {
		if f.GetIsFunctionOutput() {
			if _, produced := outputOwner[f.GetFieldID()]; !produced {
				return merr.WrapErrParameterInvalidMsg(
					"field %q is marked as a function output but no function produces it", f.GetName())
			}
		}
	}

	var dynamic *schemapb.FieldSchema
	for _, f := range schema.GetFields() {
		if !f.GetIsDynamic() {
			continue
		}
		if dynamic != nil {
			return merr.WrapErrParameterInvalidMsg("more than one dynamic field: %q and %q", dynamic.GetName(), f.GetName())
		}
		dynamic = f
	}
	if dynamic != nil {
		if !schema.GetEnableDynamicField() {
			return merr.WrapErrParameterInvalidMsg("field %q is marked dynamic but the collection has the dynamic field disabled", dynamic.GetName())
		}
		if dynamic.GetName() != common.MetaFieldName {
			return merr.WrapErrParameterInvalidMsg("the dynamic field must be named %q, got %q", common.MetaFieldName, dynamic.GetName())
		}
		if dynamic.GetDataType() != schemapb.DataType_JSON {
			return merr.WrapErrParameterInvalidMsg("the dynamic field %q must be JSON typed, got %s", dynamic.GetName(), dynamic.GetDataType())
		}
	} else if schema.GetEnableDynamicField() {
		return merr.WrapErrParameterInvalidMsg("the collection has the dynamic field enabled but no %s field", common.MetaFieldName)
	}

	return nil
}

// functionReferencing returns the name of a function in schema that uses fieldID as an input
// or output, or "" if none does.
func functionReferencing(schema *schemapb.CollectionSchema, fieldID int64) string {
	for _, fn := range schema.GetFunctions() {
		for _, id := range fn.GetInputFieldIds() {
			if id == fieldID {
				return fn.GetName()
			}
		}
		for _, id := range fn.GetOutputFieldIds() {
			if id == fieldID {
				return fn.GetName()
			}
		}
	}
	return ""
}

func hasAnyVectorField(schema *schemapb.CollectionSchema) bool {
	for _, f := range typeutil.GetAllFieldSchemas(schema) {
		if typeutil.IsVectorType(f.GetDataType()) {
			return true
		}
	}
	return false
}

// validateBoundNotTightened rejects a shrunk, removed, or now-unparseable numeric bound
// (max_length / max_capacity) and any change to dim; growing a bound and non-bound
// type-param edits (e.g. mmap) are allowed. It fails CLOSED: if the old schema carried a
// bound and the new one no longer parses it, that is a rejection, not a pass -- a deleted or
// corrupted bound must not silently widen what the node accepts.
func validateBoundNotTightened(field string, oldParams, newParams []*commonpb.KeyValuePair) error {
	for _, key := range []string{common.MaxLengthKey, common.MaxCapacityKey} {
		o, ok1 := getIntParam(oldParams, key)
		if !ok1 {
			continue // no bound before -> nothing to protect
		}
		n, ok2 := getIntParam(newParams, key)
		if !ok2 {
			return merr.WrapErrParameterInvalidMsg(
				"cannot remove or invalidate %s of field %q; a bound can only be increased", key, field)
		}
		if n < o {
			return merr.WrapErrParameterInvalidMsg(
				"cannot shrink %s of field %q (%d -> %d); it can only be increased", key, field, o, n)
		}
	}
	if o, ok1 := getIntParam(oldParams, common.DimKey); ok1 {
		n, ok2 := getIntParam(newParams, common.DimKey)
		if !ok2 || o != n {
			return merr.WrapErrParameterInvalidMsg("cannot change or remove the dim of field %q", field)
		}
	}
	return nil
}

func getIntParam(kvs []*commonpb.KeyValuePair, key string) (int64, bool) {
	for _, kv := range kvs {
		if kv.GetKey() == key {
			v, err := strconv.ParseInt(kv.GetValue(), 10, 64)
			if err != nil {
				return 0, false
			}
			return v, true
		}
	}
	return 0, false
}
