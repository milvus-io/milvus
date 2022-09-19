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
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

const (
	FlushTaskName = "FlushTask"
)

type flushTask struct {
	Condition
	*milvuspb.FlushRequest
	ctx       context.Context
	dataCoord types.DataCoord
	result    *milvuspb.FlushResponse
}

func (ft *flushTask) TraceCtx() context.Context {
	return ft.ctx
}

func (ft *flushTask) ID() UniqueID {
	return ft.Base.MsgID
}

func (ft *flushTask) SetID(uid UniqueID) {
	ft.Base.MsgID = uid
}

func (ft *flushTask) Name() string {
	return FlushTaskName
}

func (ft *flushTask) Type() commonpb.MsgType {
	return ft.Base.MsgType
}

func (ft *flushTask) BeginTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *flushTask) EndTs() Timestamp {
	return ft.Base.Timestamp
}

func (ft *flushTask) SetTs(ts Timestamp) {
	ft.Base.Timestamp = ts
}

func (ft *flushTask) OnEnqueue() error {
	ft.Base = &commonpb.MsgBase{}
	return nil
}

func (ft *flushTask) PreExecute(ctx context.Context) error {
	ft.Base.MsgType = commonpb.MsgType_Flush
	ft.Base.SourceID = Params.ProxyCfg.GetNodeID()
	return nil
}

func (ft *flushTask) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	flushColl2Segments := make(map[string]*schemapb.LongArray)
	coll2SealTimes := make(map[string]int64)
	for _, collName := range ft.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(ctx, collName)
		if err != nil {
			return err
		}
		flushReq := &datapb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     ft.Base.MsgID,
				Timestamp: ft.Base.Timestamp,
				SourceID:  ft.Base.SourceID,
			},
			DbID:         0,
			CollectionID: collID,
		}
		resp, err := ft.dataCoord.Flush(ctx, flushReq)
		if err != nil {
			return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
		}
		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}
		coll2Segments[collName] = &schemapb.LongArray{Data: resp.GetSegmentIDs()}
		flushColl2Segments[collName] = &schemapb.LongArray{Data: resp.GetFlushSegmentIDs()}
		coll2SealTimes[collName] = resp.GetTimeOfSeal()
	}
	ft.result = &milvuspb.FlushResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		DbName:          "",
		CollSegIDs:      coll2Segments,
		FlushCollSegIDs: flushColl2Segments,
		CollSealTimes:   coll2SealTimes,
	}
	return nil
}

func (ft *flushTask) PostExecute(ctx context.Context) error {
	return nil
}
