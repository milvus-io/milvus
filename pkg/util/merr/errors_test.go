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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
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

func (s *ErrSuite) TestWrap() {
	// Service related
	s.ErrorIs(WrapErrServiceNotReady("init", "test init..."), ErrServiceNotReady)
	s.ErrorIs(WrapErrServiceUnavailable("test", "test init"), ErrServiceUnavailable)
	s.ErrorIs(WrapErrServiceMemoryLimitExceeded(110, 100, "MLE"), ErrServiceMemoryLimitExceeded)
	s.ErrorIs(WrapErrServiceRequestLimitExceeded(100, "too many requests"), ErrServiceRequestLimitExceeded)
	s.ErrorIs(WrapErrServiceInternal("never throw out"), ErrServiceInternal)
	s.ErrorIs(WrapErrCrossClusterRouting("ins-0", "ins-1"), ErrCrossClusterRouting)

	// Collection related
	s.ErrorIs(WrapErrCollectionNotFound("test_collection", "failed to get collection"), ErrCollectionNotFound)
	s.ErrorIs(WrapErrCollectionNotLoaded("test_collection", "failed to query"), ErrCollectionNotLoaded)

	// Partition related
	s.ErrorIs(WrapErrPartitionNotFound("test_Partition", "failed to get Partition"), ErrPartitionNotFound)
	s.ErrorIs(WrapErrPartitionNotLoaded("test_Partition", "failed to query"), ErrPartitionNotLoaded)

	// ResourceGroup related
	s.ErrorIs(WrapErrResourceGroupNotFound("test_ResourceGroup", "failed to get ResourceGroup"), ErrResourceGroupNotFound)

	// Replica related
	s.ErrorIs(WrapErrReplicaNotFound(1, "failed to get Replica"), ErrReplicaNotFound)

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

	// Node related
	s.ErrorIs(WrapErrNodeNotFound(1, "failed to get node"), ErrNodeNotFound)
	s.ErrorIs(WrapErrNodeOffline(1, "failed to access node"), ErrNodeOffline)
	s.ErrorIs(WrapErrNodeLack(3, 1, "need more nodes"), ErrNodeLack)

	// IO related
	s.ErrorIs(WrapErrIoKeyNotFound("test_key", "failed to read"), ErrIoKeyNotFound)
	s.ErrorIs(WrapErrIoFailed("test_key", "failed to read"), ErrIoFailed)

	// Parameter related
	s.ErrorIs(WrapErrParameterInvalid(8, 1, "failed to create"), ErrParameterInvalid)
	s.ErrorIs(WrapErrParameterInvalidRange(1, 1<<16, 0, "topk should be in range"), ErrParameterInvalid)

	// Metrics related
	s.ErrorIs(WrapErrMetricNotFound("unknown", "failed to get metric"), ErrMetricNotFound)

	// Topic related
	s.ErrorIs(WrapErrTopicNotFound("unknown", "failed to get topic"), ErrTopicNotFound)
	s.ErrorIs(WrapErrTopicNotEmpty("unknown", "topic is not empty"), ErrTopicNotEmpty)

	// average related
	s.ErrorIs(WrapErrAverageLabelNotRegister("unknown", "average label not register"), ErrAverageLabelNotRegister)

	// shard delegator related
	s.ErrorIs(WrapErrShardDelegatorNotFound("unknown", "fail to get shard delegator"), ErrShardDelegatorNotFound)

	// task related
	s.ErrorIs(WrapErrTaskQueueFull("test_task_queue", "task queue is full"), ErrTaskQueueFull)
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
	err := Combine(WrapErrIoFailed("test"), WrapErrCollectionNotFound(1))
	s.Equal(Code(ErrCollectionNotFound), Code(err))
}

func TestErrors(t *testing.T) {
	suite.Run(t, new(ErrSuite))
}
