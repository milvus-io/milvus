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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/taskresource"
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

func TestMemoryToSlots(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	// 8GiB / (8192 x 1) = 1MiB raw per slot, x memoryRatio 1.0.
	oneMiBPerSlot := func(t *testing.T) {
		t.Helper()
		pt.Save(pt.DataNodeCfg.WorkerSlotUnit.Key, "8192")
		t.Cleanup(func() { pt.Reset(pt.DataNodeCfg.WorkerSlotUnit.Key) })
		pt.Save(pt.DataNodeCfg.ResourceMemoryRatio.Key, "1.0")
		t.Cleanup(func() { pt.Reset(pt.DataNodeCfg.ResourceMemoryRatio.Key) })
		require.EqualValues(t, 1024*1024, taskresource.LegacyMemoryPerSlot())
	}

	t.Run("divides by the derived bytes-per-slot", func(t *testing.T) {
		oneMiBPerSlot(t)

		assert.EqualValues(t, 10, memoryToSlots(10*1024*1024))
	})

	t.Run("floors at 1 slot instead of 0", func(t *testing.T) {
		oneMiBPerSlot(t)

		assert.EqualValues(t, 1, memoryToSlots(0))
		assert.EqualValues(t, 1, memoryToSlots(1024))
	})

	t.Run("uses the default derived rate when nothing is configured", func(t *testing.T) {
		// 384MiB per slot at the defaults, so 768MiB is 2 slots.
		assert.EqualValues(t, 2, memoryToSlots(768*1024*1024))
	})
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
