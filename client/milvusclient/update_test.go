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

package milvusclient

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

type UpdateRecordsSuite struct{ MockSuiteBase }

func TestUpdateRecords(t *testing.T) { suite.Run(t, new(UpdateRecordsSuite)) }

func (s *UpdateRecordsSuite) TestLiteralRequestWithoutSchemaRead() {
	for _, stringPK := range []bool{false, true} {
		s.resetMock()
		literal := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{
			ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 0}},
		}}
		array := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ArrayValue{
			ArrayValue: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}},
		}}
		option := NewUpdateRecordsOption("people").WithPartition("p1").
			WithAssignment("age", literal).WithNull("name").WithAssignment("tags", array)
		if stringPK {
			option.WithStringIDs([]string{"a", "a", "b"})
		} else {
			option.WithInt64IDs([]int64{1, 1, 2})
		}
		s.mock.EXPECT().UpdateRecords(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, r *milvuspb.UpdateRecordsRequest) (*milvuspb.MutationResult, error) {
			s.Equal("people", r.GetCollectionName())
			s.Equal("p1", r.GetPartitionName())
			if stringPK {
				s.Equal([]string{"a", "a", "b"}, r.GetPrimaryKeys().GetStrId().GetData())
			} else {
				s.Equal([]int64{1, 1, 2}, r.GetPrimaryKeys().GetIntId().GetData())
			}
			s.Require().Len(r.GetAssignments(), 3)
			s.Equal("age", r.GetAssignments()[0].GetFieldName())
			s.True(proto.Equal(literal, r.GetAssignments()[0].GetLiteral()))
			s.IsType(&schemapb.UpdateLiteral_NullValue{}, r.GetAssignments()[1].GetLiteral().GetValue())
			s.True(proto.Equal(array, r.GetAssignments()[2].GetLiteral()))
			return &milvuspb.MutationResult{Status: merr.Success(), UpdateCnt: 3, Acknowledged: true, Timestamp: 200}, nil
		}).Once()
		// No DescribeCollection/Query/LoadCollection: literal updates use the
		// RPC directly and preserve duplicate PKs for server admission.
		result, err := s.client.UpdateRecords(context.Background(), option)
		s.Require().NoError(err)
		s.EqualValues(3, result.UpdateCount)
		s.True(result.Acknowledged)
		s.EqualValues(200, result.Timestamp)
		s.mock.AssertExpectations(s.T())
	}
}

func (s *UpdateRecordsSuite) TestDoesNotRetryInvalidArgumentOrSchemaMismatch() {
	for _, transport := range []bool{false, true} {
		s.resetMock()
		if transport {
			s.mock.EXPECT().UpdateRecords(mock.Anything, mock.Anything).
				Return(nil, status.Error(codes.InvalidArgument, "invalid assignment")).Once()
		} else {
			// A schema mismatch may follow a successful append to another channel.
			s.mock.EXPECT().UpdateRecords(mock.Anything, mock.Anything).
				Return(&milvuspb.MutationResult{Status: merr.Status(merr.ErrCollectionSchemaMismatch)}, nil).Once()
		}
		result, err := s.client.UpdateRecords(context.Background(), NewUpdateRecordsOption("people").WithInt64IDs([]int64{1}).WithNull("name"))
		s.Require().Error(err)
		s.False(result.Acknowledged)
		s.Zero(result.UpdateCount)
		if !transport {
			s.ErrorIs(err, merr.ErrCollectionSchemaMismatch)
		}
		s.mock.AssertExpectations(s.T())
	}
}

func (s *UpdateRecordsSuite) TestDoesNotReplayAfterLostResponse() {
	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted} {
		s.resetMock()
		var calls atomic.Int32
		var storedValue atomic.Int64
		s.mock.EXPECT().UpdateRecords(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ *milvuspb.UpdateRecordsRequest) (*milvuspb.MutationResult, error) {
			storedValue.Store(18) // This request commits before its response fails.
			if calls.Add(1) == 1 {
				storedValue.Store(35) // Another writer commits after this request.
				return nil, status.Error(code, "response lost after commit")
			}
			// A replay would incorrectly overwrite the later writer with 18.
			return &milvuspb.MutationResult{Status: merr.Success(), Acknowledged: true, UpdateCnt: 1}, nil
		}).Maybe()
		literal := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{
			ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 18}},
		}}
		result, err := s.client.UpdateRecords(context.Background(), NewUpdateRecordsOption("people").WithInt64IDs([]int64{1}).WithAssignment("age", literal))
		s.Require().Error(err)
		s.Equal(code, status.Code(err))
		s.EqualValues(1, calls.Load())
		s.EqualValues(35, storedValue.Load())
		s.False(result.Acknowledged)
	}
}
