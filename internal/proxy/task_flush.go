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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type flushTask struct {
	baseTask
	Condition
	*milvuspb.FlushRequest
	ctx       context.Context
	dataCoord types.DataCoordClient
	result    *milvuspb.FlushResponse

	replicateMsgStream msgstream.MsgStream
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

func (t *flushTask) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	flushColl2Segments := make(map[string]*schemapb.LongArray)
	coll2SealTimes := make(map[string]int64)
	coll2FlushTs := make(map[string]Timestamp)
	channelCps := make(map[string]*msgpb.MsgPosition)
	for _, collName := range t.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(ctx, t.GetDbName(), collName)
		if err != nil {
			return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
		}
		flushReq := &datapb.FlushRequest{
			Base: commonpbutil.UpdateMsgBase(
				t.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_Flush),
			),
			CollectionID: collID,
		}
		resp, err := t.dataCoord.Flush(ctx, flushReq)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
		}
		coll2Segments[collName] = &schemapb.LongArray{Data: resp.GetSegmentIDs()}
		flushColl2Segments[collName] = &schemapb.LongArray{Data: resp.GetFlushSegmentIDs()}
		coll2SealTimes[collName] = resp.GetTimeOfSeal()
		coll2FlushTs[collName] = resp.GetFlushTs()
		channelCps = resp.GetChannelCps()
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.FlushRequest)
	t.result = &milvuspb.FlushResponse{
		Status:          merr.Success(),
		DbName:          t.GetDbName(),
		CollSegIDs:      coll2Segments,
		FlushCollSegIDs: flushColl2Segments,
		CollSealTimes:   coll2SealTimes,
		CollFlushTs:     coll2FlushTs,
		ChannelCps:      channelCps,
	}
	return nil
}

func (t *flushTask) PostExecute(ctx context.Context) error {
	return nil
}
