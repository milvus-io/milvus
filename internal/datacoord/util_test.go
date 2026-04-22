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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
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
			FieldID: 102,
			Binlogs: []*datapb.Binlog{{LogID: 2}, {LogID: 3}}, // 2 dup, 3 new
		}}
		result := filterDuplicateFieldBinlogs(existing, newLogs)
		suite.Equal(1, len(result))
		suite.Equal(int64(102), result[0].FieldID)
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

// allBinlogs adapts a legacy bool-returning proto mutator to a SegmentOperator
// and declares every binlog field on the final segment for side-prefix rewrite.
// Matches the legacy BinlogFamilyAll behavior that tests expected before the
// BinlogIncrement refactor.
func allBinlogs(fn func(*datapb.SegmentInfo) bool) SegmentOperator {
	return func(s *SegmentInfo) (BinlogIncrement, bool) {
		ok := fn(s.SegmentInfo)
		if !ok {
			return BinlogIncrement{}, false
		}
		return BinlogIncrement{
			Binlogs:       s.Binlogs,
			Statslogs:     s.Statslogs,
			Deltalogs:     s.Deltalogs,
			Bm25Statslogs: s.Bm25Statslogs,
		}, true
	}
}

// stateOnlyOp adapts a legacy bool-returning SegmentOperator closure to the
// new (BinlogIncrement, bool) signature for tests that never mutate binlogs.
func stateOnlyOp(fn func(*SegmentInfo) bool) SegmentOperator {
	return func(s *SegmentInfo) (BinlogIncrement, bool) {
		return BinlogIncrement{}, fn(s)
	}
}
