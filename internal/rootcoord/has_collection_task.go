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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// hasCollectionTask has collection request task
type hasCollectionTask struct {
	baseTask
	Req *milvuspb.HasCollectionRequest
	Rsp *milvuspb.BoolResponse
}

func (t *hasCollectionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_HasCollection); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *hasCollectionTask) Execute(ctx context.Context) error {
	t.Rsp.Status = succStatus()
	ts := getTravelTs(t.Req)
	// TODO: what if err != nil && common.IsCollectionNotExistError == false, should we consider this RPC as failure?
	_, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), ts)
	t.Rsp.Value = err == nil
	return nil
}
