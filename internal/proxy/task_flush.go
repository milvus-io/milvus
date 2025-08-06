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

type flushTask struct {
	baseTask
	Condition
	*milvuspb.FlushRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *milvuspb.FlushResponse

	chMgr channelsMgr
}

func (t *flushTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *flushTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *flushTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *flushTask) Name() string {
	return FlushTaskName
}

func (t *flushTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *flushTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *flushTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Flush
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *flushTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *flushTask) PostExecute(ctx context.Context) error {
	return nil
}
