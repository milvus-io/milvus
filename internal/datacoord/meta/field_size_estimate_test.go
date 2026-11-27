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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type FieldSizeEstimateSuite struct {
	suite.Suite
}

func TestFieldSizeEstimate(t *testing.T) {
	suite.Run(t, new(FieldSizeEstimateSuite))
}

func (suite *FieldSizeEstimateSuite) TestEstimateFieldsReadSize() {
	const (
		numRows          = int64(1000)
		pkField          = int64(100)
		vecField         = int64(101)
		jsonField        = int64(102)
		varcharField     = int64(103)
		badField         = int64(104)
		nullableVecField = int64(105)
	)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: pkField, Name: "pk", DataType: schemapb.DataType_Int64},
			{
				FieldID:  vecField,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
			{FieldID: jsonField, Name: "json", DataType: schemapb.DataType_JSON},
			{
				FieldID:  varcharField,
				Name:     "var",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "256"},
				},
			},
			// VarChar without max_length cannot be estimated
			{FieldID: badField, Name: "bad", DataType: schemapb.DataType_VarChar},
			{
				FieldID:  nullableVecField,
				Name:     "nullable_vec",
				DataType: schemapb.DataType_FloatVector,
				Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}

	// per row schema estimates
	vecSize := int64(128 * 4)
	pkSize := int64(8)
	jsonEstimate, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{schema.Fields[2]},
	})
	suite.NoError(err)
	jsonSize := int64(jsonEstimate)
	varcharEstimate, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{schema.Fields[3]},
	})
	suite.NoError(err)
	varcharSize := int64(varcharEstimate)

	newSegment := func(groups ...*datapb.FieldBinlog) *SegmentInfo {
		return &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				NumOfRows:      numRows,
				StorageVersion: storage.StorageV3,
				ManifestPath:   "manifest.json",
				Binlogs:        groups,
			},
		}
	}
	group := func(size int64, fields ...int64) *datapb.FieldBinlog {
		fieldBinlog := &datapb.FieldBinlog{
			FieldID: fields[0],
			Binlogs: []*datapb.Binlog{{EntriesNum: numRows, MemorySize: size}},
		}
		if len(fields) > 1 {
			fieldBinlog.FieldID = 0
			fieldBinlog.ChildFields = fields
		}
		return fieldBinlog
	}

	suite.Run("single field group is measured", func() {
		// a field owning its column group, e.g. a vector in storage v3
		segment := newSegment(group(vecSize*numRows, vecField))
		size, err := EstimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(vecSize*numRows, size)
	})

	suite.Run("fixed width field of a shared group takes its exact size", func() {
		groupSize := (pkSize + vecSize + jsonSize) * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField))

		size, err := EstimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(vecSize*numRows, size)
		suite.Less(size, groupSize)
	})

	suite.Run("variable width field takes the residual of the group", func() {
		// json rows are 10x bigger than the schema guesses, the residual of the
		// measured group size must land on the json column, not on the vector
		measuredJSONSize := jsonSize * 10
		groupSize := (pkSize + vecSize + measuredJSONSize) * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField))

		size, err := EstimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(measuredJSONSize*numRows, size)

		size, err = EstimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(vecSize*numRows, size)
	})

	suite.Run("every variable width field is charged the whole residual", func() {
		// the schema weights are guesses, so a single variable width column is
		// charged all the measured variable bytes of its group instead of a
		// share of them: under-charging a fat column would over-admit tasks
		residualPerRow := (jsonSize + varcharSize) * 4
		groupSize := (pkSize + residualPerRow) * numRows
		segment := newSegment(group(groupSize, pkField, jsonField, varcharField))

		jsonRead, err := EstimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, jsonRead)

		varcharRead, err := EstimateFieldsReadSize(schema, segment, []int64{varcharField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, varcharRead)

		// requesting both does not charge the residual twice
		bothRead, err := EstimateFieldsReadSize(schema, segment, []int64{jsonField, varcharField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, bothRead)

		// and the fixed width column of the same group is unaffected
		pkRead, err := EstimateFieldsReadSize(schema, segment, []int64{pkField})
		suite.NoError(err)
		suite.Equal(pkSize*numRows, pkRead)
	})

	suite.Run("residual keeps sub-row remainder bytes", func() {
		// a group size that is not a multiple of the row count must not lose
		// its remainder to a per-record truncation: near a slot bucket
		// boundary that loss could drop the task into a lower bucket
		jsonBytes := jsonSize*numRows + 777
		segment := newSegment(group(vecSize*numRows+jsonBytes, vecField, jsonField))
		size, err := EstimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(jsonBytes, size)
	})

	suite.Run("bool group member does not reduce the measured residual", func() {
		// bool is measured bit-packed (~1/8 B per row) but the schema charges
		// 1 B per row: treating it as exact would over-deduct from the
		// variable width columns of its group
		boolSchema := &schemapb.CollectionSchema{
			Fields: append([]*schemapb.FieldSchema{
				{FieldID: 106, Name: "flag", DataType: schemapb.DataType_Bool},
			}, schema.Fields...),
		}
		boolBytes := numRows / 8
		jsonBytes := jsonSize * numRows
		segment := newSegment(group(pkSize*numRows+boolBytes+jsonBytes, 106, pkField, jsonField))

		size, err := EstimateFieldsReadSize(boolSchema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(boolBytes+jsonBytes, size)
	})

	suite.Run("nullable fixed width field is charged its bounded width", func() {
		// a nullable vector is stored with a variable length encoding and rows
		// holding null store less than the full width, so the schema size is
		// not exact. It is still bounded above by the width plus the nullable
		// encoding overhead, and by the measured residual of its group —
		// whichever is smaller.

		// half the rows are null: the measured residual is the tighter bound
		nullableVecBytes := vecSize * numRows / 2
		segment := newSegment(group(pkSize*numRows+nullableVecBytes, pkField, nullableVecField))

		size, err := EstimateFieldsReadSize(schema, segment, []int64{nullableVecField})
		suite.NoError(err)
		suite.Equal(nullableVecBytes, size)

		// and it does not consume the fixed width budget of the group
		size, err = EstimateFieldsReadSize(schema, segment, []int64{pkField})
		suite.NoError(err)
		suite.Equal(pkSize*numRows, size)

		// no nulls and a fat neighbor: the width bound is the tighter one, the
		// nullable vector must not absorb the whole residual
		fatGroupSize := (pkSize+vecSize)*numRows + 10*vecSize*numRows
		segment = newSegment(group(fatGroupSize, pkField, nullableVecField, jsonField))
		size, err = EstimateFieldsReadSize(schema, segment, []int64{nullableVecField})
		suite.NoError(err)
		suite.Equal((vecSize+nullableFixedWidthPadPerRow)*numRows, size)
	})

	suite.Run("all nullable external group still attributes fixed width columns", func() {
		// A non milvus-table external collection forces every user field
		// nullable at create time (Pass 2 of
		// NormalizeAndValidateExternalCollectionSchema), so nothing in its one
		// synthetic column group is exact. The indexed vector must still be
		// charged its bounded width, not the whole 100x bigger group.
		externalSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: pkField, Name: "pk", DataType: schemapb.DataType_Int64, Nullable: true, ExternalField: "pk_col"},
				{
					FieldID:       vecField,
					Name:          "vec",
					DataType:      schemapb.DataType_FloatVector,
					Nullable:      true,
					ExternalField: "vec_col",
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "128"},
					},
				},
				{FieldID: jsonField, Name: "json", DataType: schemapb.DataType_JSON, Nullable: true, ExternalField: "json_col"},
			},
		}
		groupSize := 100 * vecSize * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField))

		size, err := EstimateFieldsReadSize(externalSchema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal((vecSize+nullableFixedWidthPadPerRow)*numRows, size)

		// a variable length column has no schema bound and nothing exact is
		// deductible, so it conservatively keeps the whole group
		size, err = EstimateFieldsReadSize(externalSchema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(groupSize, size)
	})

	suite.Run("schema estimate is used when the group has no residual", func() {
		// measured data smaller than the fixed width fields alone
		segment := newSegment(group(pkSize*numRows/2, pkField, jsonField))
		size, err := EstimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		// bounded by the group size
		suite.Equal(pkSize*numRows/2, size)
	})

	suite.Run("fields of several groups are summed", func() {
		segment := newSegment(
			group(vecSize*numRows, vecField),
			group((pkSize+jsonSize)*numRows, pkField, jsonField),
		)
		size, err := EstimateFieldsReadSize(schema, segment, []int64{vecField, pkField})
		suite.NoError(err)
		suite.Equal((vecSize+pkSize)*numRows, size)
	})

	suite.Run("segment without a projecting reader is rejected", func() {
		for name, configure := range map[string]func(*SegmentInfo){
			"storage v2": func(segment *SegmentInfo) {
				segment.StorageVersion = storage.StorageV2
				segment.ManifestPath = ""
			},
			"storage v3 without manifest": func(segment *SegmentInfo) {
				segment.StorageVersion = storage.StorageV3
				segment.ManifestPath = ""
			},
		} {
			suite.Run(name, func() {
				segment := newSegment(group((pkSize+jsonSize)*numRows, pkField, jsonField))
				configure(segment)
				_, err := EstimateFieldsReadSize(schema, segment, []int64{jsonField})
				suite.Error(err)
			})
		}
	})

	suite.Run("empty segment", func() {
		segment := newSegment(group(vecSize*numRows, vecField))
		segment.NumOfRows = 0
		size, err := EstimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(int64(0), size)
	})

	suite.Run("column group without recorded size", func() {
		segment := newSegment(group(0, vecField))
		_, err := EstimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.Error(err)
	})

	suite.Run("no field requested", func() {
		segment := newSegment(group(vecSize*numRows, vecField))
		_, err := EstimateFieldsReadSize(schema, segment, nil)
		suite.Error(err)
	})

	suite.Run("field without binlog", func() {
		segment := newSegment(group(vecSize*numRows, vecField))
		_, err := EstimateFieldsReadSize(schema, segment, []int64{pkField})
		suite.Error(err)
	})

	suite.Run("requested field missing from schema", func() {
		segment := newSegment(group(vecSize*numRows, vecField, 999))
		_, err := EstimateFieldsReadSize(schema, segment, []int64{999})
		suite.Error(err)
	})

	suite.Run("requested field size not estimable", func() {
		segment := newSegment(group(vecSize*numRows, vecField, badField))
		_, err := EstimateFieldsReadSize(schema, segment, []int64{badField})
		suite.Error(err)
	})

	suite.Run("group member missing from schema is skipped", func() {
		// a field dropped after the segment was flushed still shows up in
		// ChildFields until compaction rewrites the segment: its bytes stay in
		// the residual instead of aborting the whole group
		groupSize := (pkSize + vecSize + jsonSize) * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField, 999))
		size, err := EstimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(vecSize*numRows, size)
	})

	suite.Run("group member of unestimable size is skipped", func() {
		groupSize := (pkSize + vecSize + jsonSize) * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField, badField))
		size, err := EstimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(vecSize*numRows, size)
	})

	suite.Run("regular system fields of a group are charged as fixed width", func() {
		sysSchema := &schemapb.CollectionSchema{
			Fields: append([]*schemapb.FieldSchema{
				{FieldID: 0, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
				{FieldID: 1, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			}, schema.Fields...),
		}
		residualPerRow := jsonSize * 4
		groupSize := (8 + 8 + pkSize + residualPerRow) * numRows
		segment := newSegment(group(groupSize, 0, 1, pkField, jsonField))

		size, err := EstimateFieldsReadSize(sysSchema, segment, []int64{0})
		suite.NoError(err)
		suite.Equal(int64(8)*numRows, size)

		// Regular StorageV3 manifests measure these materialized columns, so
		// they consume 16B/row of the group budget.
		size, err = EstimateFieldsReadSize(sysSchema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, size)
	})

	suite.Run("external system and virtual fields do not reduce measured residual", func() {
		const (
			virtualPKField      = int64(105)
			functionOutputField = int64(106)
		)
		// production shape: user fields mapped from the source are nullable
		// (Pass 2 of NormalizeAndValidateExternalCollectionSchema), while
		// system, virtual, and function output fields keep Nullable=false
		externalSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 0, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
				{FieldID: 1, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
				{FieldID: virtualPKField, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: pkField, Name: "pk", DataType: schemapb.DataType_Int64, Nullable: true, ExternalField: "pk_col"},
				{FieldID: functionOutputField, Name: "generated", DataType: schemapb.DataType_Int64, IsFunctionOutput: true},
				{FieldID: jsonField, Name: "json", DataType: schemapb.DataType_JSON, Nullable: true, ExternalField: "json_col"},
			},
			Functions: []*schemapb.FunctionSchema{
				{OutputFieldIds: []int64{functionOutputField}},
			},
		}
		residualPerRow := jsonSize * 4
		// External refresh samples pk_col and json_col, then explicitly adds
		// generated function outputs. RowID, Timestamp, and VirtualPK still
		// appear in ChildFields but are absent from the measured group size.
		groupSize := (pkSize + 8 + residualPerRow) * numRows
		segment := newSegment(group(groupSize, 0, 1, virtualPKField, pkField, functionOutputField, jsonField))

		// only the function output is deductible; the nullable pk stays in
		// the residual the json column is charged with
		size, err := EstimateFieldsReadSize(externalSchema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal((pkSize+residualPerRow)*numRows, size)

		// the nullable pk itself is charged its bounded width
		size, err = EstimateFieldsReadSize(externalSchema, segment, []int64{pkField})
		suite.NoError(err)
		suite.Equal((pkSize+nullableFixedWidthPadPerRow)*numRows, size)

		// Unmeasured generated fields retain their schema-derived estimate when
		// requested, but do not consume the sampled source-column budget.
		size, err = EstimateFieldsReadSize(externalSchema, segment, []int64{virtualPKField})
		suite.NoError(err)
		suite.Equal(int64(8)*numRows, size)
	})

	suite.Run("unrecognized data type", func() {
		unknownSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: pkField, Name: "pk", DataType: schemapb.DataType_Int64},
				{FieldID: vecField, Name: "unknown", DataType: schemapb.DataType_None},
			},
		}
		segment := newSegment(group(pkSize*numRows, pkField, vecField))
		_, err := EstimateFieldsReadSize(unknownSchema, segment, []int64{vecField})
		suite.Error(err)
	})
}

func (suite *FieldSizeEstimateSuite) TestIsFixedWidthType() {
	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_Int64, schemapb.DataType_Double,
		schemapb.DataType_FloatVector, schemapb.DataType_Int8Vector,
	} {
		suite.True(isFixedWidthType(dataType), dataType.String())
	}
	for _, dataType := range []schemapb.DataType{
		// Bool is stored bit-packed, so the 1 byte per row schema estimate is
		// not exact and it must be attributed from the measured residual
		schemapb.DataType_Bool,
		schemapb.DataType_VarChar, schemapb.DataType_Text, schemapb.DataType_JSON,
		schemapb.DataType_Array, schemapb.DataType_Geometry,
		schemapb.DataType_SparseFloatVector, schemapb.DataType_ArrayOfVector,
	} {
		suite.False(isFixedWidthType(dataType), dataType.String())
	}
}

func (suite *FieldSizeEstimateSuite) TestSupportsFieldProjection() {
	for name, tc := range map[string]struct {
		storageVersion int64
		manifestPath   string
		expected       bool
	}{
		"storage v3 with manifest":    {storage.StorageV3, "manifest.json", true},
		"storage v3 without manifest": {storage.StorageV3, "", false},
		"storage v2":                  {storage.StorageV2, "", false},
	} {
		suite.Run(name, func() {
			segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				StorageVersion: tc.storageVersion,
				ManifestPath:   tc.manifestPath,
			}}
			suite.Equal(tc.expected, SupportsFieldProjection(segment))
			suite.Equal(tc.expected, CanBuildExternalJSONKeyIndex(segment))
		})
	}
}
