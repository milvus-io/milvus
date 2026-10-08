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

package dql

import (
	"context"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// HighlightTaskName is the task name for the post-search highlight task.
const HighlightTaskName = "Highlight"

// git highlight after search
type HighlightTask struct {
	baseTask
	Condition
	*querypb.GetHighlightRequest
	ctx            context.Context
	collectionName string
	collectionID   typeutil.UniqueID
	dbName         string
	lb             shardclient.LBPolicy

	result *querypb.GetHighlightResponse
}

func (t *HighlightTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *HighlightTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *HighlightTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *HighlightTask) Name() string {
	return HighlightTaskName
}

func (t *HighlightTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *HighlightTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *HighlightTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *HighlightTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *HighlightTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Undefined
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *HighlightTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *HighlightTask) getHighlightOnShardleader(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	ctx = retry.WithMaxAttemptsContext(ctx, 1)
	t.Channel = channel
	resp, err := qn.GetHighlight(ctx, t.GetHighlightRequest)
	if err != nil {
		return err
	}

	if err := merr.Error(resp.GetStatus()); err != nil {
		return err
	}
	t.result = resp
	return nil
}

func (t *HighlightTask) Execute(ctx context.Context) error {
	err := t.lb.ExecuteOneChannel(ctx, shardclient.CollectionWorkLoad{
		Db:             t.dbName,
		CollectionName: t.collectionName,
		CollectionID:   t.collectionID,
		Nq:             int64(len(t.GetTopks()) * len(t.GetTasks())),
		Exec:           t.getHighlightOnShardleader,
	})

	return err
}

func (t *HighlightTask) PostExecute(ctx context.Context) error {
	return nil
}

// isIgnoreGrowing is used to check if the request should ignore growing
func isIgnoreGrowing(params []*commonpb.KeyValuePair) (bool, error) {
	for _, kv := range params {
		if kv.GetKey() == IgnoreGrowingKey {
			ignoreGrowing, err := strconv.ParseBool(kv.GetValue())
			if err != nil {
				return false, merr.WrapErrParameterInvalidMsg("parse ignore growing field failed")
			}
			return ignoreGrowing, nil
		}
	}
	return false, nil
}
