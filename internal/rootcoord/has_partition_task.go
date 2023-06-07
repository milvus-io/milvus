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
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// hasPartitionTask has partition request task
type hasPartitionTask struct {
	baseTask
	Req *milvuspb.HasPartitionRequest
	Rsp *milvuspb.BoolResponse
}

func (t *hasPartitionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_HasPartition); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *hasPartitionTask) Execute(ctx context.Context) error {
	t.Rsp.Status = succStatus()
	t.Rsp.Value = false
	// TODO: why HasPartitionRequest doesn't contain Timestamp but other requests do.
	coll, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.CollectionName, typeutil.MaxTimestamp)
	if err != nil {
		t.Rsp.Status = failStatus(commonpb.ErrorCode_CollectionNotExists, err.Error())
		return err
	}
	for _, part := range coll.Partitions {
		if part.PartitionName == t.Req.PartitionName {
			t.Rsp.Value = true
			break
		}
	}
	return nil
}
