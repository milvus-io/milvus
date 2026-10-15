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

package dml

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type BatchUpdateManifestTask struct {
	baseTask
	Condition
	req      *milvuspb.BatchUpdateManifestRequest
	ctx      context.Context
	mixCoord types.MixCoordClient
	result   *commonpb.Status

	collectionID UniqueID
}

// NewBatchUpdateManifestTask constructs a batch-update-manifest task.
func NewBatchUpdateManifestTask(ctx context.Context, node taskmodel.TaskNode, req *milvuspb.BatchUpdateManifestRequest) *BatchUpdateManifestTask {
	return &BatchUpdateManifestTask{
		baseTask:  baseTask{MetaCache: node.GetMetaCache()},
		ctx:       ctx,
		Condition: NewTaskCondition(ctx),
		req:       req,
		mixCoord:  node.MixCoord(),
	}
}

// Result returns the status after execution.
func (bt *BatchUpdateManifestTask) Result() *commonpb.Status {
	return bt.result
}

func (bt *BatchUpdateManifestTask) TraceCtx() context.Context {
	return bt.ctx
}

func (bt *BatchUpdateManifestTask) ID() UniqueID {
	return bt.req.GetBase().GetMsgID()
}

func (bt *BatchUpdateManifestTask) SetID(uid UniqueID) {
	bt.req.GetBase().MsgID = uid
}

func (bt *BatchUpdateManifestTask) Name() string {
	return "BatchUpdateManifestTask"
}

func (bt *BatchUpdateManifestTask) Type() commonpb.MsgType {
	return bt.req.GetBase().GetMsgType()
}

func (bt *BatchUpdateManifestTask) BeginTs() Timestamp {
	return bt.req.GetBase().GetTimestamp()
}

func (bt *BatchUpdateManifestTask) EndTs() Timestamp {
	return bt.req.GetBase().GetTimestamp()
}

func (bt *BatchUpdateManifestTask) SetTs(ts Timestamp) {
	bt.req.Base.Timestamp = ts
}

func (bt *BatchUpdateManifestTask) OnEnqueue() error {
	if bt.req.Base == nil {
		bt.req.Base = commonpbutil.NewMsgBase()
	}
	bt.req.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (bt *BatchUpdateManifestTask) PreExecute(ctx context.Context) error {
	req := bt.req
	if req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("collection name is empty")
	}
	if len(req.GetItems()) == 0 {
		return merr.WrapErrParameterInvalidMsg("items is empty")
	}

	collectionID, err := bt.GetMetaCache().GetCollectionID(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	bt.collectionID = collectionID

	return nil
}

func (bt *BatchUpdateManifestTask) Execute(ctx context.Context) error {
	mlog.Info(ctx, "proxy batch update manifest",
		mlog.FieldCollectionName(bt.req.GetCollectionName()),
		mlog.FieldCollectionID(bt.collectionID),
		mlog.Int("itemCount", len(bt.req.GetItems())),
	)

	items := make([]*datapb.BatchUpdateManifestItem, 0, len(bt.req.GetItems()))
	for _, item := range bt.req.GetItems() {
		items = append(items, &datapb.BatchUpdateManifestItem{
			SegmentId:       item.GetSegmentId(),
			ManifestVersion: item.GetManifestVersion(),
		})
	}

	var err error
	bt.result, err = bt.mixCoord.BatchUpdateManifest(ctx, &datapb.BatchUpdateManifestRequest{
		Base:         commonpbutil.NewMsgBase(),
		CollectionId: bt.collectionID,
		Items:        items,
	})
	if err = merr.CheckRPCCall(bt.result, err); err != nil {
		return err
	}
	return nil
}

func (bt *BatchUpdateManifestTask) PostExecute(ctx context.Context) error {
	return nil
}
