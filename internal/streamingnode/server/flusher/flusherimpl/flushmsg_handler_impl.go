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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/pkg/log"
)

// TODO: func(vchannel string, msg FlushMsg)
func flushMsgHandlerImpl(wbMgr writebuffer.BufferManager) func(vchannel string, segmentIDs []int64) {
	return func(vchannel string, segmentIDs []int64) {
		err := wbMgr.SealSegments(context.Background(), vchannel, segmentIDs)
		if err != nil {
			log.Warn("failed to seal segments", zap.Error(err))
		}
	}
}
