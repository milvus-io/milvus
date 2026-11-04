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

package meta

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// isFixedWidthType tells whether the schema alone fully determines the size of
// a field, i.e. the estimation cannot be off. Variable length types only get a
// configured guess out of the schema (see typeutil.EstimateSizePerRecord), so
// they are attributed from the measured data instead whenever possible.
//
// Bool is deliberately absent: the schema charges it 1 byte per row while the
// measured group size accounts it bit-packed (~1/8 byte per row, see
// ActualSizeInBytes handling of arrow.BOOL), and external sampling can round
// it down to 0. Treating it as exact would over-deduct from the residual and
// understate the variable length columns sharing its group.
func IsFixedWidthType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float,
		schemapb.DataType_Double, schemapb.DataType_Timestamptz,
		schemapb.DataType_BinaryVector, schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector:
		return true
	default:
		return false
	}
}

// fieldSizeEstimate is the schema-derived size of one field, qualified by how
// far it can be trusted. The two flags serve the two distinct uses of a size:
//
//   - exact: the stored size is fully determined by the schema (fixed width,
//     not nullable). Safe to deduct from the measured group budget — a
//     deduction must never exceed the field's real bytes, or the remaining
//     variable length fields get understated.
//   - fixedWidth: the schema width bounds the stored data from above even when
//     the field is nullable (null rows only shrink it). Safe to charge the
//     requested field with, never to deduct.
type fieldSizeEstimate struct {
	sizePerRecord int64
	exact         bool
	fixedWidth    bool
}

// fieldSizePerRecord returns the per row size of a field derived from the
// schema.
//
// A nullable field is never exact, whatever its data type: rows holding null
// store less than the full width (nullable vectors are even stored with a
// variable length encoding, see add_vector_payload in
// internal/core/src/storage/Util.cpp), so its real size is data dependent.
// For a fixed width type the schema width still bounds it from above, which
// fixedWidth records.
func fieldSizePerRecord(schema *schemapb.CollectionSchema, fieldID int64) (fieldSizeEstimate, error) {
	field := typeutil.GetFieldByID(schema, fieldID)
	if field == nil {
		return fieldSizeEstimate{}, merr.WrapErrFieldNotFound(fieldID)
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{field},
	})
	if err != nil {
		return fieldSizeEstimate{}, err
	}
	if sizePerRecord <= 0 {
		// An unrecognized data type estimates to zero, which would understate
		// the slot usage. Let the caller keep the conservative binlog size.
		return fieldSizeEstimate{}, merr.WrapErrParameterInvalidMsg("cannot estimate size of field %d with data type %s",
			fieldID, field.GetDataType().String())
	}
	fixedWidth := IsFixedWidthType(field.GetDataType())
	return fieldSizeEstimate{
		sizePerRecord: int64(sizePerRecord),
		exact:         fixedWidth && !field.GetNullable(),
		fixedWidth:    fixedWidth,
	}, nil
}

// columnGroup is a set of fields stored together, which is what a binlog entry
// describes for storage v3 segments. Storage v1 binlogs hold a single field and
// therefore form a group of one.
type columnGroup struct {
	fields []int64
	size   int64
}

// segmentColumnGroups returns the column groups of a segment, keyed by every
// field they hold.
func segmentColumnGroups(segment *SegmentInfo) map[int64]*columnGroup {
	groups := make(map[int64]*columnGroup)
	for _, fieldBinlog := range segment.GetBinlogs() {
		fields := fieldBinlog.GetChildFields()
		if len(fields) == 0 {
			fields = []int64{fieldBinlog.GetFieldID()}
		}
		var size int64
		for _, binlog := range fieldBinlog.GetBinlogs() {
			size += binlog.GetMemorySize()
		}
		group := &columnGroup{fields: fields, size: size}
		for _, fieldID := range fields {
			groups[fieldID] = group
		}
	}
	return groups
}

// estimateFieldsReadSize estimates how much data a task reads for fieldIDs of a
// segment.
//
// It works off segment.GetBinlogs(), which for manifest-backed StorageV3
// segments only exists in memory: the catalog does not persist their
// FieldBinlog arrays (see isV3Segment), so after a DataCoord restart such
// segments carry no binlogs and every field errors out here, letting the
// callers keep their conservative whole-segment fallback until the segment is
// rewritten.
//
// Both the index build and the json/text stats tasks request a single column at
// a time. Per-field attribution is safe only for a StorageV3 segment with a
// manifest: GetFieldDatasFromManifest asks the reader for the target column,
// while GetFieldDatasFromStorageV2 materializes every column of the selected
// column group. The binlog size however is recorded per column group, so a
// projected field sharing a group with others otherwise gets charged for all of
// them. This is worst for external collections, whose segments carry one
// synthetic group holding every column at once (see buildFakeBinlogs in
// internal/datanode/external).
//
// The group size is the measured truth for the group as a whole, so it is used
// as the budget: fields whose schema gives an exact size take exactly that,
// nullable fixed width fields take their schema width plus the nullable
// encoding overhead (an upper bound — nulls only shrink the stored data), and
// whatever remains is charged to the variable length fields. That keeps a 128
// dim vector at (close to) its real size — including on non milvus-table
// external collections, where every user field is forced nullable at create
// time — while a json column is charged what the data really contains instead
// of the configured per row guess (common.dynamicFieldLengthAvg), which can be
// an order of magnitude off. The residual is not split proportionally between
// several variable length fields: the schema weights are guesses, and
// splitting by them could understate a fat column, i.e. over-admit tasks.
// Charging it once keeps the result an upper bound, which is what a slot must
// be. The flip side is that a variable length column of a group with nothing
// exact to deduct (e.g. any non milvus-table external group, all nullable) is
// charged the whole group.
func EstimateFieldsReadSize(schema *schemapb.CollectionSchema, segment *SegmentInfo, fieldIDs []int64) (int64, error) {
	if len(fieldIDs) == 0 {
		return 0, merr.WrapErrParameterInvalidMsg("no field to estimate size for")
	}
	numRows := segment.GetNumOfRows()
	if numRows <= 0 {
		return 0, nil
	}
	if !SupportsFieldProjection(segment) {
		// Callers gate this optimization on supportsFieldProjection. Reaching
		// this branch means an internal contract was violated, not bad input.
		return 0, merr.WrapErrServiceInternalMsg(
			"segment %d does not have a StorageV3 manifest for projected field reads", segment.GetID())
	}

	groups := segmentColumnGroups(segment)
	// requested fields grouped by the column group holding them
	requested := make(map[*columnGroup][]int64)
	for _, fieldID := range fieldIDs {
		group, ok := groups[fieldID]
		if !ok {
			return 0, merr.WrapErrFieldNotFound(fieldID, "field has no binlog in segment")
		}
		requested[group] = append(requested[group], fieldID)
	}

	var total int64
	for group, groupFieldIDs := range requested {
		size, err := estimateGroupFieldsSize(schema, group, groupFieldIDs, numRows)
		if err != nil {
			return 0, err
		}
		total += size
	}
	return total, nil
}

// supportsFieldProjection matches the actual reader selection in the index and
// stats workers. A non-empty manifest selects GetFieldDatasFromManifest, which
// projects the requested column. StorageV2 files without a manifest are read by
// GetFieldDatasFromStorageV2, which materializes the whole column group.
func SupportsFieldProjection(segment *SegmentInfo) bool {
	return segment.GetStorageVersion() == storage.StorageV3 && segment.GetManifestPath() != ""
}

// nullableFixedWidthPadPerRow bounds the per row encoding overhead a nullable
// fixed width column adds on top of its schema width: nullable vectors are
// binary encoded (4 byte offsets, see add_vector_payload in
// internal/core/src/storage/Util.cpp), and every nullable column carries a
// validity bitmap (1/8 byte per row).
const NullableFixedWidthPadPerRow = 8

// estimateGroupFieldsSize attributes the measured size of one column group to
// the requested fields of that group.
//
// Each requested field is charged by the strongest bound available: an exact
// field its schema size, a nullable fixed width field its schema width plus
// the nullable encoding overhead (nulls only shrink the stored data), and a
// variable length field the measured residual of the group — the group size
// minus the exact fields, which only exact fields may reduce (deducting a
// nullable field's full width could overstate its real bytes and understate
// everyone else's).
func estimateGroupFieldsSize(schema *schemapb.CollectionSchema, group *columnGroup, fieldIDs []int64, numRows int64) (int64, error) {
	if group.size <= 0 {
		// nothing measured to attribute, e.g. binlogs written before the size
		// was recorded. Let the caller keep its own conservative fallback.
		return 0, merr.WrapErrParameterInvalidMsg("column group of fields %v has no recorded size", group.fields)
	}
	if len(group.fields) <= 1 {
		// the group holds nothing but the requested field, its size is exact
		return group.size, nil
	}

	requestedFields := make(map[int64]struct{}, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		requestedFields[fieldID] = struct{}{}
	}

	estimates := make(map[int64]fieldSizeEstimate, len(group.fields))
	var fixedPerRecord int64
	var hasVariableField bool
	var externalColumnResolver *typeutil.StorageColumnResolver
	if typeutil.IsExternalCollection(schema) {
		externalColumnResolver = typeutil.NewStorageColumnResolver(schema)
	}
	for _, fieldID := range group.fields {
		estimate, err := fieldSizePerRecord(schema, fieldID)
		if err != nil {
			if _, requested := requestedFields[fieldID]; requested {
				return 0, err
			}
			// A group member the schema cannot resolve, e.g. a field dropped
			// after this segment was flushed and before compaction rewrote it.
			// Skipping it leaves its bytes in the residual instead of giving up
			// on the whole group.
			continue
		}
		estimates[fieldID] = estimate
		if estimate.exact {
			measured := true
			if externalColumnResolver != nil {
				field := typeutil.GetFieldByID(schema, fieldID)
				_, measured = externalColumnResolver.SourceDataColumnName(field)
				// Refresh adds generated function outputs to MemorySize after
				// sampling the source columns, so they are measured too.
				measured = measured || typeutil.IsFunctionOutputField(schema, field)
			}
			if measured {
				fixedPerRecord += estimate.sizePerRecord
			}
			continue
		}
		hasVariableField = true
	}

	// bytes the variable length fields of this group take in total, measured.
	// Kept in whole bytes rather than per record: a per record truncation
	// could lose up to numRows bytes and drop the estimate into a lower slot
	// bucket.
	residualBytes := group.size - fixedPerRecord*numRows
	if residualBytes < 0 || !hasVariableField {
		// the schema does not add up to the measured data, e.g. because some
		// field of the group is not materialized in it. Fall back to the schema
		// estimation alone, still bounded by the group size.
		residualBytes = 0
	}

	var total int64
	var residualCharged bool
	for _, fieldID := range fieldIDs {
		estimate := estimates[fieldID]
		switch {
		case estimate.exact:
			total += estimate.sizePerRecord * numRows
		case estimate.fixedWidth:
			// Nullable fixed width: null rows only shrink the stored data, so
			// the schema width plus the nullable encoding overhead bounds it
			// from above; the measured residual it lives in caps it further.
			// This branch is what keeps the attribution alive on non
			// milvus-table external collections, where create time forces
			// every user field nullable (see Pass 2 of
			// NormalizeAndValidateExternalCollectionSchema) and nothing is
			// left to deduct from the group budget.
			charge := (estimate.sizePerRecord + NullableFixedWidthPadPerRow) * numRows
			if residualBytes > 0 && charge > residualBytes {
				charge = residualBytes
			}
			total += charge
		case residualBytes > 0:
			// every variable length field of the group shares the same
			// residual, so it is charged once no matter how many of them are
			// requested
			if !residualCharged {
				total += residualBytes
				residualCharged = true
			}
		default:
			total += estimate.sizePerRecord * numRows
		}
	}
	if total > group.size {
		return group.size, nil
	}
	return total, nil
}
