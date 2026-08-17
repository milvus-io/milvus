// licensed to the lf ai & data foundation under one
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

package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UtilSuite struct {
	suite.Suite
}

func (suite *UtilSuite) TestVerifyResponse() {
	type testCase struct {
		resp       interface{}
		err        error
		expected   error
		equalValue bool
	}
	cases := []testCase{
		{
			resp:       nil,
			err:        errors.New("boom"),
			expected:   errors.New("boom"),
			equalValue: true,
		},
		{
			resp:       nil,
			err:        nil,
			expected:   errNilResponse,
			equalValue: false,
		},
		{
			resp:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			err:        nil,
			expected:   nil,
			equalValue: false,
		},
		{
			resp:       &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "r1"},
			err:        nil,
			expected:   errors.New("r1"),
			equalValue: true,
		},
		{
			resp:       (*commonpb.Status)(nil),
			err:        nil,
			expected:   errNilResponse,
			equalValue: false,
		},
		{
			resp: &rootcoordpb.AllocIDResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			},
			err:        nil,
			expected:   nil,
			equalValue: false,
		},
		{
			resp: &rootcoordpb.AllocIDResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: "r2"},
			},
			err:        nil,
			expected:   errors.New("r2"),
			equalValue: true,
		},
		{
			resp:       &rootcoordpb.AllocIDResponse{},
			err:        nil,
			expected:   errNilStatusResponse,
			equalValue: true,
		},
		{
			resp:       (*rootcoordpb.AllocIDResponse)(nil),
			err:        nil,
			expected:   errNilStatusResponse,
			equalValue: true,
		},
		{
			resp:       struct{}{},
			err:        nil,
			expected:   errUnknownResponseType,
			equalValue: false,
		},
	}
	for _, c := range cases {
		r := VerifyResponse(c.resp, c.err)
		if c.equalValue {
			suite.Contains(r.Error(), c.expected.Error())
		} else {
			suite.Equal(c.expected, r)
		}
	}
}

func TestUtil(t *testing.T) {
	suite.Run(t, new(UtilSuite))
}

func (suite *UtilSuite) TestEstimateFieldsReadSize() {
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
		size, err := estimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(vecSize*numRows, size)
	})

	suite.Run("fixed width field of a shared group takes its exact size", func() {
		groupSize := (pkSize + vecSize + jsonSize) * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField))

		size, err := estimateFieldsReadSize(schema, segment, []int64{vecField})
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

		size, err := estimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(measuredJSONSize*numRows, size)

		size, err = estimateFieldsReadSize(schema, segment, []int64{vecField})
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

		jsonRead, err := estimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, jsonRead)

		varcharRead, err := estimateFieldsReadSize(schema, segment, []int64{varcharField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, varcharRead)

		// requesting both does not charge the residual twice
		bothRead, err := estimateFieldsReadSize(schema, segment, []int64{jsonField, varcharField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, bothRead)

		// and the fixed width column of the same group is unaffected
		pkRead, err := estimateFieldsReadSize(schema, segment, []int64{pkField})
		suite.NoError(err)
		suite.Equal(pkSize*numRows, pkRead)
	})

	suite.Run("residual keeps sub-row remainder bytes", func() {
		// a group size that is not a multiple of the row count must not lose
		// its remainder to a per-record truncation: near a slot bucket
		// boundary that loss could drop the task into a lower bucket
		jsonBytes := jsonSize*numRows + 777
		segment := newSegment(group(vecSize*numRows+jsonBytes, vecField, jsonField))
		size, err := estimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(jsonBytes, size)
	})

	suite.Run("nullable fixed width field is charged from the residual", func() {
		// a nullable vector is stored with a variable length encoding and rows
		// holding null store less than the full width, so the schema size is
		// not exact: the field takes the measured residual of its group
		nullableVecBytes := vecSize * numRows / 2
		segment := newSegment(group(pkSize*numRows+nullableVecBytes, pkField, nullableVecField))

		size, err := estimateFieldsReadSize(schema, segment, []int64{nullableVecField})
		suite.NoError(err)
		suite.Equal(nullableVecBytes, size)

		// and it does not consume the fixed width budget of the group
		size, err = estimateFieldsReadSize(schema, segment, []int64{pkField})
		suite.NoError(err)
		suite.Equal(pkSize*numRows, size)
	})

	suite.Run("schema estimate is used when the group has no residual", func() {
		// measured data smaller than the fixed width fields alone
		segment := newSegment(group(pkSize*numRows/2, pkField, jsonField))
		size, err := estimateFieldsReadSize(schema, segment, []int64{jsonField})
		suite.NoError(err)
		// bounded by the group size
		suite.Equal(pkSize*numRows/2, size)
	})

	suite.Run("fields of several groups are summed", func() {
		segment := newSegment(
			group(vecSize*numRows, vecField),
			group((pkSize+jsonSize)*numRows, pkField, jsonField),
		)
		size, err := estimateFieldsReadSize(schema, segment, []int64{vecField, pkField})
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
				_, err := estimateFieldsReadSize(schema, segment, []int64{jsonField})
				suite.Error(err)
			})
		}
	})

	suite.Run("empty segment", func() {
		segment := newSegment(group(vecSize*numRows, vecField))
		segment.NumOfRows = 0
		size, err := estimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(int64(0), size)
	})

	suite.Run("column group without recorded size", func() {
		segment := newSegment(group(0, vecField))
		_, err := estimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.Error(err)
	})

	suite.Run("no field requested", func() {
		segment := newSegment(group(vecSize*numRows, vecField))
		_, err := estimateFieldsReadSize(schema, segment, nil)
		suite.Error(err)
	})

	suite.Run("field without binlog", func() {
		segment := newSegment(group(vecSize*numRows, vecField))
		_, err := estimateFieldsReadSize(schema, segment, []int64{pkField})
		suite.Error(err)
	})

	suite.Run("requested field missing from schema", func() {
		segment := newSegment(group(vecSize*numRows, vecField, 999))
		_, err := estimateFieldsReadSize(schema, segment, []int64{999})
		suite.Error(err)
	})

	suite.Run("requested field size not estimable", func() {
		segment := newSegment(group(vecSize*numRows, vecField, badField))
		_, err := estimateFieldsReadSize(schema, segment, []int64{badField})
		suite.Error(err)
	})

	suite.Run("group member missing from schema is skipped", func() {
		// a field dropped after the segment was flushed still shows up in
		// ChildFields until compaction rewrites the segment: its bytes stay in
		// the residual instead of aborting the whole group
		groupSize := (pkSize + vecSize + jsonSize) * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField, 999))
		size, err := estimateFieldsReadSize(schema, segment, []int64{vecField})
		suite.NoError(err)
		suite.Equal(vecSize*numRows, size)
	})

	suite.Run("group member of unestimable size is skipped", func() {
		groupSize := (pkSize + vecSize + jsonSize) * numRows
		segment := newSegment(group(groupSize, pkField, vecField, jsonField, badField))
		size, err := estimateFieldsReadSize(schema, segment, []int64{vecField})
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

		size, err := estimateFieldsReadSize(sysSchema, segment, []int64{0})
		suite.NoError(err)
		suite.Equal(int64(8)*numRows, size)

		// Regular StorageV3 manifests measure these materialized columns, so
		// they consume 16B/row of the group budget.
		size, err = estimateFieldsReadSize(sysSchema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, size)
	})

	suite.Run("external system and virtual fields do not reduce measured residual", func() {
		const (
			virtualPKField      = int64(105)
			functionOutputField = int64(106)
		)
		externalSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 0, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
				{FieldID: 1, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
				{FieldID: virtualPKField, Name: common.VirtualPKFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: pkField, Name: "pk", DataType: schemapb.DataType_Int64, ExternalField: "pk_col"},
				{FieldID: functionOutputField, Name: "generated", DataType: schemapb.DataType_Int64, IsFunctionOutput: true},
				{FieldID: jsonField, Name: "json", DataType: schemapb.DataType_JSON, ExternalField: "json_col"},
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

		size, err := estimateFieldsReadSize(externalSchema, segment, []int64{jsonField})
		suite.NoError(err)
		suite.Equal(residualPerRow*numRows, size)

		// Unmeasured generated fields retain their schema-derived estimate when
		// requested, but do not consume the sampled source-column budget.
		size, err = estimateFieldsReadSize(externalSchema, segment, []int64{virtualPKField})
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
		_, err := estimateFieldsReadSize(unknownSchema, segment, []int64{vecField})
		suite.Error(err)
	})
}

func (suite *UtilSuite) TestIsFixedWidthType() {
	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int64, schemapb.DataType_Double,
		schemapb.DataType_FloatVector, schemapb.DataType_Int8Vector,
	} {
		suite.True(isFixedWidthType(dataType), dataType.String())
	}
	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_VarChar, schemapb.DataType_Text, schemapb.DataType_JSON,
		schemapb.DataType_Array, schemapb.DataType_Geometry,
		schemapb.DataType_SparseFloatVector, schemapb.DataType_ArrayOfVector,
	} {
		suite.False(isFixedWidthType(dataType), dataType.String())
	}
}

type fixedTSOAllocator struct {
	fixedTime time.Time
}

func (f *fixedTSOAllocator) AllocTimestamp(_ context.Context) (Timestamp, error) {
	return tsoutil.ComposeTS(f.fixedTime.UnixNano()/int64(time.Millisecond), 0), nil
}

func (f *fixedTSOAllocator) AllocID(_ context.Context) (UniqueID, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fixedTSOAllocator) AllocN(_ context.Context, _ int64) (UniqueID, UniqueID, error) {
	panic("not implemented") // TODO: Implement
}

func (suite *UtilSuite) TestGetZeroTime() {
	n := 10
	for i := 0; i < n; i++ {
		timeGot := getZeroTime()
		suite.True(timeGot.IsZero())
	}
}

func (suite *UtilSuite) TestGetCollectionAutoCompactionEnabled() {
	properties := map[string]string{
		common.CollectionAutoCompactionKey: "true",
	}

	enabled, err := getCollectionAutoCompactionEnabled(properties)
	suite.NoError(err)
	suite.True(enabled)

	properties = map[string]string{
		common.CollectionAutoCompactionKey: "bad_value",
	}

	_, err = getCollectionAutoCompactionEnabled(properties)
	suite.Error(err)

	enabled, err = getCollectionAutoCompactionEnabled(map[string]string{})
	suite.NoError(err)
	suite.Equal(Params.DataCoordCfg.EnableAutoCompaction.GetAsBool(), enabled)
}

func (suite *UtilSuite) TestCreateStorageConfig() {
	suite.Run("local", func() {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "local")
		paramtable.Get().Save(Params.LocalStorageCfg.Path.Key, "/tmp/milvus-local")
		paramtable.Get().Save(Params.MinioCfg.MaxConnections.Key, "237")
		defer paramtable.Get().Reset(Params.CommonCfg.StorageType.Key)
		defer paramtable.Get().Reset(Params.LocalStorageCfg.Path.Key)
		defer paramtable.Get().Reset(Params.MinioCfg.MaxConnections.Key)

		config := createStorageConfig()
		suite.Equal("local", config.StorageType)
		suite.Equal("/tmp/milvus-local", config.RootPath)
		// An external collection can still read from s3:// while the primary
		// storage is local, so the connection cap must survive this branch.
		suite.Equal(uint32(237), config.MaxConnections)
	})

	suite.Run("remote", func() {
		paramtable.Get().Save(Params.CommonCfg.StorageType.Key, "minio")
		paramtable.Get().Save(Params.MinioCfg.SslTLSMinVersion.Key, "1.2")
		paramtable.Get().Save(Params.MinioCfg.UseCRC32C.Key, "true")
		paramtable.Get().Save(Params.MinioCfg.MaxConnections.Key, "237")
		defer paramtable.Get().Reset(Params.CommonCfg.StorageType.Key)
		defer paramtable.Get().Reset(Params.MinioCfg.SslTLSMinVersion.Key)
		defer paramtable.Get().Reset(Params.MinioCfg.UseCRC32C.Key)
		defer paramtable.Get().Reset(Params.MinioCfg.MaxConnections.Key)

		config := createStorageConfig()
		suite.Equal("minio", config.StorageType)
		suite.Equal(Params.MinioCfg.Address.GetValue(), config.Address)
		suite.Equal("1.2", config.SslTlsMinVersion)
		suite.True(config.UseCrc32CChecksum)
		suite.Equal(uint32(237), config.MaxConnections)
	})
}

func (suite *UtilSuite) TestCalculateL0SegmentSize() {
	logsize := int64(100)
	fields := []*datapb.FieldBinlog{{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{LogSize: logsize, MemorySize: logsize}},
	}}

	suite.Equal(calculateL0SegmentSize(fields), float64(logsize))
}

func (suite *UtilSuite) TestCalculateIndexTaskSlot() {
	pt := paramtable.Get()
	heavyKey := pt.DataCoordCfg.IndexTaskSlotUsage.Key
	scalarKey := pt.DataCoordCfg.ScalarIndexTaskSlotUsage.Key
	workerSlotKey := pt.DataNodeCfg.WorkerSlotUnit.Key
	buildParallelKey := pt.DataNodeCfg.BuildParallel.Key
	suite.NoError(pt.Save(heavyKey, "64"))
	suite.NoError(pt.Save(scalarKey, "16"))
	suite.NoError(pt.Save(workerSlotKey, "16"))
	suite.NoError(pt.Save(buildParallelKey, "1"))
	defer pt.Reset(heavyKey)
	defer pt.Reset(scalarKey)
	defer pt.Reset(workerSlotKey)
	defer pt.Reset(buildParallelKey)

	const mib = int64(1024 * 1024)
	fmIndexParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}
	invertedParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED}}
	testCases := []struct {
		name         string
		fieldSize    int64
		wantFMIndex  int64
		wantInverted int64
	}{
		{name: "small", fieldSize: 5 * mib, wantFMIndex: 1, wantInverted: 1},
		{name: "medium", fieldSize: 50 * mib, wantFMIndex: 1, wantInverted: 1},
		{name: "large_below_512mb", fieldSize: 200 * mib, wantFMIndex: 4, wantInverted: 4},
		{name: "exactly_512mb", fieldSize: 512 * mib, wantFMIndex: 10, wantInverted: 4},
		{name: "above_512mb", fieldSize: 512*mib + 1, wantFMIndex: 10, wantInverted: 16},
		{name: "one_gib", fieldSize: 1024 * mib, wantFMIndex: 20, wantInverted: 32},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			suite.Equal(tc.wantFMIndex, calculateIndexTaskSlot(tc.fieldSize, 1, fmIndexParams))
			suite.Equal(tc.wantInverted, calculateIndexTaskSlot(tc.fieldSize, 1, invertedParams))
		})
	}

	// Existing vector indexes must keep using the same heavy curve after the
	// helper started accepting the complete parameter set.
	hnswParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "HNSW"}}
	suite.Equal(int64(16), calculateIndexTaskSlot(200*mib, 1, hnswParams))
}

func (suite *UtilSuite) TestEstimateFMIndexBuildPeakBytes() {
	const mib = int64(1024 * 1024)
	defaultParams := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}

	// For a single long row the compact-SA peak is ~9.66x payload: source data,
	// int32 text + SA, sampled bitmap/rank directory, and 1/8 sampled SA values.
	peak := estimateFMIndexBuildPeakBytes(100*mib, 1, defaultParams)
	suite.Greater(peak, int64(float64(100*mib)*9.65))
	suite.Less(peak, int64(float64(100*mib)*9.68))

	// More rows add separators and the actual std::string/string_view/boundary
	// allocations even when payload bytes are identical.
	manyRowsPeak := estimateFMIndexBuildPeakBytes(100*mib, 1_000_000, defaultParams)
	suite.Greater(manyRowsPeak, peak)

	// The sampled-SA rate is a real build-memory knob and must affect admission.
	rate4 := append(defaultParams, &commonpb.KeyValuePair{Key: indexparamcheck.FmSaSampleRateKey, Value: "4"})
	rate64 := append(defaultParams, &commonpb.KeyValuePair{Key: indexparamcheck.FmSaSampleRateKey, Value: "64"})
	suite.Greater(
		estimateFMIndexBuildPeakBytes(100*mib, 1, rate4),
		estimateFMIndexBuildPeakBytes(100*mib, 1, rate64),
	)

	// Crossing INT32_MAX symbols selects the int64 text + SA path and creates a
	// visible discontinuity that the estimator must preserve.
	compactPeak := estimateFMIndexBuildPeakBytes(int64(^uint32(0)>>1)-1, 0, defaultParams)
	widePeak := estimateFMIndexBuildPeakBytes(int64(^uint32(0)>>1), 0, defaultParams)
	suite.Greater(widePeak, compactPeak)
}

func (suite *UtilSuite) TestFMIndexBuildTaskSlotsStandaloneRatio() {
	pt := paramtable.Get()
	workerSlotKey := pt.DataNodeCfg.WorkerSlotUnit.Key
	buildParallelKey := pt.DataNodeCfg.BuildParallel.Key
	standaloneRatioKey := pt.DataNodeCfg.StandaloneSlotRatio.Key
	suite.NoError(pt.Save(workerSlotKey, "16"))
	suite.NoError(pt.Save(buildParallelKey, "1"))
	suite.NoError(pt.Save(standaloneRatioKey, "0.25"))
	defer pt.Reset(workerSlotKey)
	defer pt.Reset(buildParallelKey)
	defer pt.Reset(standaloneRatioKey)

	oldRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.StandaloneRole)
	defer paramtable.SetRole(oldRole)

	params := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexparamcheck.IndexFMINDEX}}
	// A ~9.66 GiB peak consumes five standalone slots when the 0.25 factor
	// exposes four slots per 8 GiB memory unit.
	suite.Equal(int64(5), fmIndexBuildTaskSlots(1024*1024*1024, 1, params))
}

func (suite *UtilSuite) TestFilterDuplicateFieldBinlogs() {
	suite.Run("empty existing returns new unchanged", func() {
		newLogs := []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}},
		}}
		result := filterDuplicateFieldBinlogs(nil, newLogs)
		suite.Equal(newLogs, result)
	})

	suite.Run("empty new returns empty", func() {
		existing := []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogID: 1}},
		}}
		result := filterDuplicateFieldBinlogs(existing, nil)
		suite.Empty(result)
	})

	suite.Run("partial overlap same field", func() {
		existing := []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}},
		}}
		newLogs := []*datapb.FieldBinlog{{
			FieldID:     102,
			ChildFields: []int64{102, 103},
			Format:      "parquet",
			Binlogs:     []*datapb.Binlog{{LogID: 2}, {LogID: 3}}, // 2 dup, 3 new
		}}
		result := filterDuplicateFieldBinlogs(existing, newLogs)
		suite.Equal(1, len(result))
		suite.Equal(int64(102), result[0].FieldID)
		suite.ElementsMatch([]int64{102, 103}, result[0].GetChildFields())
		suite.Equal("parquet", result[0].GetFormat())
		suite.Equal(1, len(result[0].Binlogs))
		suite.Equal(int64(3), result[0].Binlogs[0].LogID)
	})

	suite.Run("full overlap returns empty", func() {
		existing := []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}},
		}}
		newLogs := []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}},
		}}
		result := filterDuplicateFieldBinlogs(existing, newLogs)
		suite.Empty(result)
	})

	suite.Run("different fieldIDs no filtering", func() {
		existing := []*datapb.FieldBinlog{{
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogID: 1}},
		}}
		newLogs := []*datapb.FieldBinlog{{
			FieldID: 103,
			Binlogs: []*datapb.Binlog{{LogID: 1}}, // same logID but different field
		}}
		result := filterDuplicateFieldBinlogs(existing, newLogs)
		suite.Equal(1, len(result))
		suite.Equal(int64(103), result[0].FieldID)
		suite.Equal(1, len(result[0].Binlogs))
	})

	suite.Run("mixed fields partial overlap", func() {
		existing := []*datapb.FieldBinlog{
			{FieldID: 102, Binlogs: []*datapb.Binlog{{LogID: 1}}},
			{FieldID: 103, Binlogs: []*datapb.Binlog{{LogID: 5}}},
		}
		newLogs := []*datapb.FieldBinlog{
			{FieldID: 102, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}}}, // 1 dup, 2 new
			{FieldID: 104, Binlogs: []*datapb.Binlog{{LogID: 10}}},            // completely new field
		}
		result := filterDuplicateFieldBinlogs(existing, newLogs)
		suite.Equal(2, len(result))
		// find fieldID 102 in result
		var fb102, fb104 *datapb.FieldBinlog
		for _, fb := range result {
			if fb.FieldID == 102 {
				fb102 = fb
			}
			if fb.FieldID == 104 {
				fb104 = fb
			}
		}
		suite.NotNil(fb102)
		suite.Equal(1, len(fb102.Binlogs))
		suite.Equal(int64(2), fb102.Binlogs[0].LogID)
		suite.NotNil(fb104)
		suite.Equal(1, len(fb104.Binlogs))
	})
}

func (suite *UtilSuite) TestMergeFieldBinlogsPreservesColumnGroupMetadata() {
	current := []*datapb.FieldBinlog{{
		FieldID: 102,
		Binlogs: []*datapb.Binlog{{LogID: 1}},
	}}
	newLogs := []*datapb.FieldBinlog{{
		FieldID:     102,
		ChildFields: []int64{102, 103},
		Format:      "parquet",
		Binlogs:     []*datapb.Binlog{{LogID: 2}},
	}}

	result := mergeFieldBinlogs(current, newLogs)

	suite.Len(result, 1)
	suite.Equal([]int64{102, 103}, result[0].GetChildFields())
	suite.Equal("parquet", result[0].GetFormat())
	suite.Len(result[0].GetBinlogs(), 2)
}
