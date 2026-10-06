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

// Package arrowconv converts between the Arrow records segcore exports and
// schemapb.FieldData.
//
// It is deliberately cgo-free. Producing the record needs cgo and stays in
// internal/util/segcore; everything downstream of the C Data Interface is
// ordinary Go over arrow.Record, and keeping it separable is what lets the
// reduce (internal/util/queryutil) materialize a selection without pulling a
// built libmilvus_core into its tests -- and what will let the delegator and
// proxy reuse this if the wire format ever carries Arrow.
package arrowconv

import (
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// fieldOrderMetadataKey is the Arrow schema-level metadata key carrying the
// field ids of the original fields_data in their original order, comma
// separated. The C++ export records the order as observed rather than
// recomputing it: the retrieve fill branches disagree on an order derivable
// from the plan (plain retrieve follows plan->field_ids_, ORDER BY follows
// pipeline_field_ids_ and appends system fields last).
const fieldOrderMetadataKey = "milvus.field_order"

// validDataFieldsMetadataKey is the Arrow schema-level metadata key listing
// the field ids whose source DataArray carried a valid_data bitmap, comma
// separated. See kRetrieveValidDataFieldsKey in segment_c.cpp for why the
// distinction cannot be recovered from the Arrow array or the schema.
const validDataFieldsMetadataKey = "milvus.valid_data_fields"

// FieldOrder returns the field ids recorded by the C++ export, in the order
// the protobuf path would have emitted them.
func FieldOrder(record arrow.Record) ([]int64, error) {
	md := record.Schema().Metadata()
	idx := md.FindKey(fieldOrderMetadataKey)
	if idx < 0 {
		return nil, merr.WrapErrServiceInternal(
			"arrow retrieve record is missing " + fieldOrderMetadataKey +
				" metadata; column order cannot be reconstructed")
	}
	return parseFieldOrder(md.Values()[idx])
}

// SchemaMetadataFingerprint returns the two retrieve metadata values as a
// single comparable string, for checking that every record in a selection
// agrees with the template.
//
// This is DEFENSE IN DEPTH, not a fix for a reachable divergence. No segment of
// one request can currently disagree with another on either key:
//
//   - field_order is built from plan->field_ids_, which every emitter walks in
//     order (SegmentInterface's FillTargetEntry and the sealed take path alike);
//   - valid_data_fields is effectively "the nullable output fields". Both
//     emission paths key on field_meta.is_nullable() -- the take path populates
//     it in ArrowToDataArray, and the non-take path in
//     CreateEmptyScalarDataArray -- so the set is schema-derived and identical
//     per segment. The take fallback is whole-result too
//     (clear_fields_data + clear_ids + return false), not per column, so a
//     segment cannot be half-taken.
//
// It is asserted anyway because the materializer reads BOTH keys from the
// template -- whichever record happens to be first -- and then applies that one
// reading to the rows of every record. That assumption is invisible at the call
// site and would fail silently if it ever broke: the wrong reading strips a real
// bitmap or invents an all-true one, which is a byte-identity violation rather
// than an error. Column count agreement, which is checked separately, does not
// imply it.
//
// A missing key yields an error from the caller's own FieldOrder/validDataFields
// call, so this only needs to compare what is there.
func SchemaMetadataFingerprint(record arrow.Record) string {
	md := record.Schema().Metadata()
	get := func(key string) string {
		if i := md.FindKey(key); i >= 0 {
			return md.Values()[i]
		}
		return "\x00missing"
	}
	return get(fieldOrderMetadataKey) + "\x01" + get(validDataFieldsMetadataKey)
}

// validDataFields returns the set of field ids whose source DataArray carried
// a valid_data bitmap. A missing key is an error rather than an empty set: it
// would otherwise be indistinguishable from "no column had one", which is a
// legal value, and silently strip every bitmap.
func validDataFields(record arrow.Record) (map[int64]struct{}, error) {
	md := record.Schema().Metadata()
	idx := md.FindKey(validDataFieldsMetadataKey)
	if idx < 0 {
		return nil, merr.WrapErrServiceInternal(
			"arrow retrieve record is missing " + validDataFieldsMetadataKey +
				" metadata; valid_data cannot be reconstructed")
	}
	ids, err := parseFieldOrder(md.Values()[idx])
	if err != nil {
		return nil, err
	}
	set := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set, nil
}

// UserColumnIDs returns the field ids of the Arrow columns, in Arrow column
// order: everything in order that the protobuf header did not retain.
//
// The Arrow columns carry no field-id metadata (see
// ArrowFieldsToProtoOrdered for why), so this is how they are identified.
// order lists every column of the original fields_data; headerFields are the
// columns that stayed in the protobuf header (the system ones), so the set
// difference is
// exactly the Arrow columns and field_order preserves their relative order.
//
// Partitioning by "what the header kept" rather than by a Go-side system-field
// predicate is deliberate: C++ SystemProperty::Instance().IsSystem() made the
// split, and reading its result back keeps one source of truth. A second
// predicate here could drift from it silently.
//
// Callers must reject duplicate ids in order first. Aggregation columns all
// carry field id 0 and so does RowID, so without that check an aggregation
// result's columns would be misread as system columns and dropped.
func UserColumnIDs(order []int64, headerFields []*schemapb.FieldData) []int64 {
	retained := make(map[int64]struct{}, len(headerFields))
	for _, fd := range headerFields {
		retained[fd.GetFieldId()] = struct{}{}
	}
	out := make([]int64, 0, len(order)-len(retained))
	for _, id := range order {
		if _, isSystem := retained[id]; !isSystem {
			out = append(out, id)
		}
	}
	return out
}

// parseFieldOrder parses the comma-separated field id list written by the C++
// export. An empty value means the result carried no columns at all, which is
// legal (a zero-match query).
func parseFieldOrder(v string) ([]int64, error) {
	if v == "" {
		return nil, nil
	}
	parts := strings.Split(v, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		id, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, merr.WrapErrServiceInternalErr(err,
				"malformed %s metadata: %s", fieldOrderMetadataKey, v)
		}
		out = append(out, id)
	}
	return out, nil
}

// ReconcileValidData makes the reconstructed columns carry a valid_data
// bitmap exactly when the protobuf path would have.
//
// The LIVE reason is the one-pass writer: queryutil's materializeColumn never
// writes ValidData, for any type. A nullable column with no nulls in this batch
// passes selectedSources and takes that writer, so it comes back with no bitmap
// where the protobuf path would have an all-true one -- which the `want && !got`
// arm below repairs. (A nullable column that DOES have nulls is declined to the
// gather, which goes through setArrowValidData and gets its bitmap there.)
//
// Secondarily, schema nullability is the wrong authority in general: the ORDER BY
// pipeline allocates valid_data for every scalar column whether or not the field
// is nullable, so a non-nullable field would come back with a bitmap on the
// protobuf path and without one on the Arrow path. That divergence is NOT
// reachable today -- CreateScalarDataArray's nullable=true callers are both on
// the columnar path, which shouldUseArrowTransport excludes -- so it is recorded
// as why the export's own record is the right authority, not as a case this
// repairs today.
//
// Why it matters either way: results from the two paths meet in one reduce when
// the config differs ACROSS NODES, and AppendFieldData appends a validity bit
// only for sources that have one (schema.go:1367-1371), so the mix yields a
// ValidData shorter than the row count. There is no per-segment fallback within
// a single QueryNode -- RetrieveArrow has no fallback and the routing decision
// is made once per request -- so the cross-node mix is the only real one.
//
// The C++ export records what the source actually had; that is the authority,
// in both directions.
// numRows is the row count of the RESULT, which is not record.NumRows() when
// the record is only a metadata template for a lazily materialized selection.
func ReconcileValidData(record arrow.Record, cols []*schemapb.FieldData, numRows int) error {
	hadValidData, err := validDataFields(record)
	if err != nil {
		return err
	}
	for _, fd := range cols {
		_, want := hadValidData[fd.GetFieldId()]
		got := len(typeutil.GetFieldDataValidData(fd)) > 0
		switch {
		case want && !got:
			// The source had an all-true bitmap: no nulls, or
			// ArrowFieldsToProto would already have emitted one.
			valid := make([]bool, numRows)
			for i := range valid {
				valid[i] = true
			}
			typeutil.SetFieldDataValidData(fd, valid)
		case !want && got:
			typeutil.SetFieldDataValidData(fd, nil)
		}
	}
	return nil
}
