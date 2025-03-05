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

package merr

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ErrSuite struct {
	suite.Suite
}

func (s *ErrSuite) SetupSuite() {
	paramtable.Init()
}

func (s *ErrSuite) TestCode() {
	err := WrapErrCollectionNotFound(1)
	errors.Wrap(err, "failed to get collection")
	s.ErrorIs(err, ErrCollectionNotFound)
	s.Equal(Code(ErrCollectionNotFound), Code(err))
	s.Equal(TimeoutCode, Code(context.DeadlineExceeded))
	s.Equal(CanceledCode, Code(context.Canceled))
	s.Equal(errUnexpected.errCode, Code(errUnexpected))

	sameCodeErr := newMilvusError("new error", ErrCollectionNotFound.errCode, false)
	s.True(sameCodeErr.Is(ErrCollectionNotFound))
}

func (s *ErrSuite) TestStatus() {
	err := WrapErrCollectionNotFound(1)
	status := Status(err)
	restoredErr := Error(status)

	s.ErrorIs(err, restoredErr)
	s.Equal(int32(0), Status(nil).Code)
	s.Nil(Error(&commonpb.Status{}))
}

func (s *ErrSuite) TestStatusWithCode() {
	err := WrapErrCollectionNotFound(1)
	status := StatusWithErrorCode(err, commonpb.ErrorCode_CollectionNotExists)
	restoredErr := Error(status)

	s.ErrorIs(err, restoredErr)
	s.Equal(commonpb.ErrorCode_CollectionNotExists, status.ErrorCode)
	s.Equal(int32(0), StatusWithErrorCode(nil, commonpb.ErrorCode_CollectionNotExists).Code)
}

func (s *ErrSuite) TestWrap() {
	// Service related
	s.ErrorIs(WrapErrServiceNotReady("test", 0, "test init..."), ErrServiceNotReady)
	s.ErrorIs(WrapErrServiceUnavailable("test", "test init"), ErrServiceUnavailable)
	s.ErrorIs(WrapErrServiceMemoryLimitExceeded(110, 100, "MLE"), ErrServiceMemoryLimitExceeded)
	s.ErrorIs(WrapErrTooManyRequests(100, "too many requests"), ErrServiceTooManyRequests)
	s.ErrorIs(WrapErrServiceInternal("never throw out"), ErrServiceInternal)
	s.ErrorIs(WrapErrServiceCrossClusterRouting("ins-0", "ins-1"), ErrServiceCrossClusterRouting)
	s.ErrorIs(WrapErrServiceDiskLimitExceeded(110, 100, "DLE"), ErrServiceDiskLimitExceeded)
	s.ErrorIs(WrapErrNodeNotMatch(0, 1, "SIM"), ErrNodeNotMatch)
	s.ErrorIs(WrapErrServiceUnimplemented(errors.New("mock grpc err")), ErrServiceUnimplemented)

	// Collection related
	s.ErrorIs(WrapErrCollectionNotFound("test_collection", "failed to get collection"), ErrCollectionNotFound)
	s.ErrorIs(WrapErrCollectionNotLoaded("test_collection", "failed to query"), ErrCollectionNotLoaded)
	s.ErrorIs(WrapErrCollectionNotFullyLoaded("test_collection", "failed to query"), ErrCollectionNotFullyLoaded)
	s.ErrorIs(WrapErrCollectionNotLoaded("test_collection", "failed to alter index %s", "hnsw"), ErrCollectionNotLoaded)
	s.ErrorIs(WrapErrCollectionOnRecovering("test_collection", "channel lost %s", "dev"), ErrCollectionOnRecovering)
	s.ErrorIs(WrapErrCollectionVectorClusteringKeyNotAllowed("test_collection", "field"), ErrCollectionVectorClusteringKeyNotAllowed)
	s.ErrorIs(WrapErrCollectionSchemaMisMatch("schema mismatch", "field"), ErrCollectionSchemaMismatch)
	// Partition related
	s.ErrorIs(WrapErrPartitionNotFound("test_partition", "failed to get partition"), ErrPartitionNotFound)
	s.ErrorIs(WrapErrPartitionNotLoaded("test_partition", "failed to query"), ErrPartitionNotLoaded)
	s.ErrorIs(WrapErrPartitionNotFullyLoaded("test_partition", "failed to query"), ErrPartitionNotFullyLoaded)

	// ResourceGroup related
	s.ErrorIs(WrapErrResourceGroupNotFound("test_ResourceGroup", "failed to get ResourceGroup"), ErrResourceGroupNotFound)
	s.ErrorIs(WrapErrResourceGroupAlreadyExist("test_ResourceGroup", "failed to get ResourceGroup"), ErrResourceGroupAlreadyExist)
	s.ErrorIs(WrapErrResourceGroupReachLimit("test_ResourceGroup", 1, "failed to get ResourceGroup"), ErrResourceGroupReachLimit)
	s.ErrorIs(WrapErrResourceGroupIllegalConfig("test_ResourceGroup", nil, "failed to get ResourceGroup"), ErrResourceGroupIllegalConfig)
	s.ErrorIs(WrapErrResourceGroupNodeNotEnough("test_ResourceGroup", 1, 2, "failed to get ResourceGroup"), ErrResourceGroupNodeNotEnough)
	s.ErrorIs(WrapErrResourceGroupServiceAvailable("test_ResourceGroup", "failed to get ResourceGroup"), ErrResourceGroupServiceAvailable)

	// Replica related
	s.ErrorIs(WrapErrReplicaNotFound(1, "failed to get replica"), ErrReplicaNotFound)
	s.ErrorIs(WrapErrReplicaNotAvailable(1, "failed to get replica"), ErrReplicaNotAvailable)

	// Channel related
	s.ErrorIs(WrapErrChannelNotFound("test_Channel", "failed to get Channel"), ErrChannelNotFound)
	s.ErrorIs(WrapErrChannelLack("test_Channel", "failed to get Channel"), ErrChannelLack)
	s.ErrorIs(WrapErrChannelReduplicate("test_Channel", "failed to get Channel"), ErrChannelReduplicate)

	// Segment related
	s.ErrorIs(WrapErrSegmentNotFound(1, "failed to get Segment"), ErrSegmentNotFound)
	s.ErrorIs(WrapErrSegmentNotLoaded(1, "failed to query"), ErrSegmentNotLoaded)
	s.ErrorIs(WrapErrSegmentLack(1, "lack of segment"), ErrSegmentLack)
	s.ErrorIs(WrapErrSegmentReduplicate(1, "redundancy of segment"), ErrSegmentReduplicate)

	// Index related
	s.ErrorIs(WrapErrIndexNotFound("failed to get Index"), ErrIndexNotFound)
	s.ErrorIs(WrapErrIndexNotFoundForCollection("milvus_hello", "failed to get collection index"), ErrIndexNotFound)
	s.ErrorIs(WrapErrIndexNotFoundForSegments([]int64{100}, "failed to get collection index"), ErrIndexNotFound)
	s.ErrorIs(WrapErrIndexNotSupported("wsnh", "failed to create index"), ErrIndexNotSupported)

	// Node related
	s.ErrorIs(WrapErrNodeNotFound(1, "failed to get node"), ErrNodeNotFound)
	s.ErrorIs(WrapErrNodeOffline(1, "failed to access node"), ErrNodeOffline)
	s.ErrorIs(WrapErrNodeLack(3, 1, "need more nodes"), ErrNodeLack)
	s.ErrorIs(WrapErrNodeStateUnexpected(1, "Stopping", "failed to suspend node"), ErrNodeStateUnexpected)

	// IO related
	s.ErrorIs(WrapErrIoKeyNotFound("test_key", "failed to read"), ErrIoKeyNotFound)
	s.ErrorIs(WrapErrIoFailed("test_key", os.ErrClosed), ErrIoFailed)
	s.ErrorIs(WrapErrIoUnexpectEOF("test_key", os.ErrClosed), ErrIoUnexpectEOF)

	// Parameter related
	s.ErrorIs(WrapErrParameterInvalid(8, 1, "failed to create"), ErrParameterInvalid)
	s.ErrorIs(WrapErrParameterInvalidRange(1, 1<<16, 0, "topk should be in range"), ErrParameterInvalid)
	s.ErrorIs(WrapErrParameterMissing("alias_name", "no alias parameter"), ErrParameterMissing)
	s.ErrorIs(WrapErrParameterTooLarge("unit test"), ErrParameterTooLarge)

	// Metrics related
	s.ErrorIs(WrapErrMetricNotFound("unknown", "failed to get metric"), ErrMetricNotFound)

	// Message queue related
	s.ErrorIs(WrapErrMqTopicNotFound("unknown", "failed to get topic"), ErrMqTopicNotFound)
	s.ErrorIs(WrapErrMqTopicNotEmpty("unknown", "topic is not empty"), ErrMqTopicNotEmpty)
	s.ErrorIs(WrapErrMqInternal(errors.New("unknown"), "failed to consume"), ErrMqInternal)

	// field related
	s.ErrorIs(WrapErrFieldNotFound("meta", "failed to get field"), ErrFieldNotFound)

	// alias related
	s.ErrorIs(WrapErrAliasNotFound("alias", "failed to get collection id"), ErrAliasNotFound)
	s.ErrorIs(WrapErrCollectionIDOfAliasNotFound(1000, "failed to get collection id"), ErrCollectionIDOfAliasNotFound)

	// Search/Query related
	s.ErrorIs(WrapErrInconsistentRequery("unknown"), ErrInconsistentRequery)
}

func (s *ErrSuite) TestOldCode() {
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_NotReadyServe), ErrServiceNotReady)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_CollectionNotExists), ErrCollectionNotFound)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_IllegalArgument), ErrParameterInvalid)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_NodeIDNotMatch), ErrNodeNotMatch)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_InsufficientMemoryToLoad), ErrServiceMemoryLimitExceeded)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_MemoryQuotaExhausted), ErrServiceMemoryLimitExceeded)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_DiskQuotaExhausted), ErrServiceDiskLimitExceeded)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_RateLimit), ErrServiceRateLimit)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_ForceDeny), ErrServiceQuotaExceeded)
	s.ErrorIs(OldCodeToMerr(commonpb.ErrorCode_UnexpectedError), errUnexpected)
}

func (s *ErrSuite) TestCombine() {
	var (
		errFirst  = errors.New("first")
		errSecond = errors.New("second")
		errThird  = errors.New("third")
	)

	err := Combine(errFirst, errSecond)
	s.True(errors.Is(err, errFirst))
	s.True(errors.Is(err, errSecond))
	s.False(errors.Is(err, errThird))

	s.Equal("first: second", err.Error())
}

func (s *ErrSuite) TestCombineWithNil() {
	err := errors.New("non-nil")

	err = Combine(nil, err)
	s.NotNil(err)
}

func (s *ErrSuite) TestCombineOnlyNil() {
	err := Combine(nil, nil)
	s.Nil(err)
}

func (s *ErrSuite) TestCombineCode() {
	err := Combine(WrapErrPartitionNotFound(10), WrapErrCollectionNotFound(1))
	s.Equal(Code(ErrCollectionNotFound), Code(err))
}

func (s *ErrSuite) TestIsHealthy() {
	type testCase struct {
		code   commonpb.StateCode
		expect bool
	}

	cases := []testCase{
		{commonpb.StateCode_Healthy, true},
		{commonpb.StateCode_Initializing, false},
		{commonpb.StateCode_Abnormal, false},
		{commonpb.StateCode_StandBy, false},
		{commonpb.StateCode_Stopping, false},
	}
	for _, tc := range cases {
		s.Run(tc.code.String(), func() {
			s.Equal(tc.expect, IsHealthy(tc.code) == nil)
		})
	}
}

func (s *ErrSuite) TestIsHealthyOrStopping() {
	type testCase struct {
		code   commonpb.StateCode
		expect bool
	}

	cases := []testCase{
		{commonpb.StateCode_Healthy, true},
		{commonpb.StateCode_Initializing, false},
		{commonpb.StateCode_Abnormal, false},
		{commonpb.StateCode_StandBy, false},
		{commonpb.StateCode_Stopping, true},
	}
	for _, tc := range cases {
		s.Run(tc.code.String(), func() {
			s.Equal(tc.expect, IsHealthyOrStopping(tc.code) == nil)
		})
	}
}

func TestErrors(t *testing.T) {
	suite.Run(t, new(ErrSuite))
}
