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

package flusherimpl

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func newFlushMsgHandler(wbMgr writebuffer.BufferManager) *flushMsgHandlerImpl {
	return &flushMsgHandlerImpl{
		wbMgr: wbMgr,
	}
}

type flushMsgHandlerImpl struct {
	wbMgr writebuffer.BufferManager
}

func (impl *flushMsgHandlerImpl) HandleFlush(vchannel string, flushMsg message.ImmutableFlushMessageV2) error {
	body, err := flushMsg.Body()
	if err != nil {
		return errors.Wrap(err, "failed to get flush message body")
	}
	if err := impl.wbMgr.SealSegments(context.Background(), vchannel, body.GetSegmentId()); err != nil {
		return errors.Wrap(err, "failed to seal segments")
	}
	return nil
}

func (impl *flushMsgHandlerImpl) HandleManualFlush(vchannel string, flushMsg message.ImmutableManualFlushMessageV2) error {
	if err := impl.wbMgr.FlushChannel(context.Background(), vchannel, flushMsg.Header().GetFlushTs()); err != nil {
		return errors.Wrap(err, "failed to flush channel")
	}
	return nil
}
