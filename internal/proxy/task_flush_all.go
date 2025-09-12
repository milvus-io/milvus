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

package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type flushAllTask struct {
	baseTask
	Condition
	*milvuspb.FlushAllRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.FlushAllResponse
	chMgr    channelsMgr
}

func (t *flushAllTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *flushAllTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *flushAllTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *flushAllTask) Name() string {
	return FlushAllTaskName
}

func (t *flushAllTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *flushAllTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushAllTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushAllTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *flushAllTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Flush
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *flushAllTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *flushAllTask) PostExecute(ctx context.Context) error {
	return nil
}
