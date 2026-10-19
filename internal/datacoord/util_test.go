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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type UtilSuite struct {
	suite.Suite
}

func (suite *UtilSuite) TestCompactionMergeInfoEnums() {
	types := map[datapb.CompactionType]commonpb.CompactionType{
		datapb.CompactionType_UndefinedCompaction:                  commonpb.CompactionType_CompactionTypeUndefined,
		datapb.CompactionType_MergeCompaction:                      commonpb.CompactionType_CompactionTypeMerge,
		datapb.CompactionType_MixCompaction:                        commonpb.CompactionType_CompactionTypeMix,
		datapb.CompactionType_SingleCompaction:                     commonpb.CompactionType_CompactionTypeSingle,
		datapb.CompactionType_MinorCompaction:                      commonpb.CompactionType_CompactionTypeMinor,
		datapb.CompactionType_MajorCompaction:                      commonpb.CompactionType_CompactionTypeMajor,
		datapb.CompactionType_Level0DeleteCompaction:               commonpb.CompactionType_CompactionTypeLevel0Delete,
		datapb.CompactionType_ClusteringCompaction:                 commonpb.CompactionType_CompactionTypeClustering,
		datapb.CompactionType_SortCompaction:                       commonpb.CompactionType_CompactionTypeSort,
		datapb.CompactionType_PartitionKeySortCompaction:           commonpb.CompactionType_CompactionTypePartitionKeySort,
		datapb.CompactionType_ClusteringPartitionKeySortCompaction: commonpb.CompactionType_CompactionTypeClusteringPartitionKeySort,
		datapb.CompactionType_BumpSchemaVersionCompaction:          commonpb.CompactionType_CompactionTypeBumpSchemaVersion,
	}
	states := map[datapb.CompactionTaskState]commonpb.CompactionTaskState{
		datapb.CompactionTaskState_unknown:    commonpb.CompactionTaskState_CompactionTaskStateUnknown,
		datapb.CompactionTaskState_executing:  commonpb.CompactionTaskState_CompactionTaskStateExecuting,
		datapb.CompactionTaskState_pipelining: commonpb.CompactionTaskState_CompactionTaskStatePipelining,
		datapb.CompactionTaskState_completed:  commonpb.CompactionTaskState_CompactionTaskStateCompleted,
		datapb.CompactionTaskState_failed:     commonpb.CompactionTaskState_CompactionTaskStateFailed,
		datapb.CompactionTaskState_timeout:    commonpb.CompactionTaskState_CompactionTaskStateTimeout,
		datapb.CompactionTaskState_analyzing:  commonpb.CompactionTaskState_CompactionTaskStateAnalyzing,
		datapb.CompactionTaskState_indexing:   commonpb.CompactionTaskState_CompactionTaskStateIndexing,
		datapb.CompactionTaskState_cleaned:    commonpb.CompactionTaskState_CompactionTaskStateCleaned,
		datapb.CompactionTaskState_meta_saved: commonpb.CompactionTaskState_CompactionTaskStateMetaSaved,
		datapb.CompactionTaskState_statistic:  commonpb.CompactionTaskState_CompactionTaskStateStatistic,
	}
	// Adding an internal enum requires verifying its public wire equivalent.
	suite.Len(types, len(datapb.CompactionType_name))
	suite.Len(states, len(datapb.CompactionTaskState_name))
	for internalType, publicType := range types {
		for internalState, publicState := range states {
			info := getCompactionMergeInfo(&datapb.CompactionTask{
				Type: internalType, State: internalState,
				InputSegments: []int64{1, 2}, ResultSegments: []int64{3, 4},
				FailReason: "retained failure reason",
			})
			wire, err := proto.Marshal(info)
			suite.Require().NoError(err)
			decoded := &milvuspb.CompactionMergeInfo{}
			suite.Require().NoError(proto.Unmarshal(wire, decoded))
			suite.Equal(publicType, decoded.GetType(), internalType.String())
			suite.Equal(publicState, decoded.GetState(), internalState.String())
			suite.Equal([]int64{1, 2}, decoded.GetSources())
			suite.Equal([]int64{3, 4}, decoded.GetTargets())
			suite.Equal(int64(3), decoded.GetTarget())
			suite.Equal("retained failure reason", decoded.GetFailureReason())
		}
	}
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
