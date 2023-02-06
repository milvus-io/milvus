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

package pipeline

import (
	"errors"
	"fmt"
)

var (
	ErrMsgInvalidType = errors.New("InvalidMessageType")
	ErrMsgNotAligned  = errors.New("CheckAlignedFailed")
	ErrMsgEmpty       = errors.New("EmptyMessage")
	ErrMsgNotTarget   = errors.New("NotTarget")
	ErrMsgExcluded    = errors.New("SegmentExcluded")

	ErrCollectionNotFound     = errors.New("CollectionNotFound")
	ErrShardDelegatorNotFound = errors.New("ShardDelegatorNotFound")

	ErrNewPipelineFailed = errors.New("FailedCreateNewPipeline")
	ErrStartPipeline     = errors.New("PipineStartFailed")
)

func WrapErrMsgNotAligned(err error) error {
	return fmt.Errorf("%w :%s", ErrMsgNotAligned, err)
}

func WrapErrMsgNotTarget(reason string) error {
	return fmt.Errorf("%w%s", ErrMsgNotTarget, reason)
}

func WrapErrMsgExcluded(segmentID int64) error {
	return fmt.Errorf("%w ID:%d", ErrMsgExcluded, segmentID)
}

func WrapErrNewPipelineFailed(err error) error {
	return fmt.Errorf("%w :%s", ErrNewPipelineFailed, err)
}

func WrapErrStartPipeline(reason string) error {
	return fmt.Errorf("%w :%s", ErrStartPipeline, reason)
}

func WrapErrShardDelegatorNotFound(channel string) error {
	return fmt.Errorf("%w channel:%s", ErrShardDelegatorNotFound, channel)
}
