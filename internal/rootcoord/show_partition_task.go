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
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// showPartitionTask show partition request task
type showPartitionTask struct {
	baseTask
	Req              *milvuspb.ShowPartitionsRequest
	Rsp              *milvuspb.ShowPartitionsResponse
	allowUnavailable bool
}

func (t *showPartitionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.Base.MsgType, commonpb.MsgType_ShowPartitions); err != nil {
		return err
	}
	return nil
}

// Execute task execution
func (t *showPartitionTask) Execute(ctx context.Context) error {
	var coll *model.Collection
	var err error
	t.Rsp.Status = succStatus()
	if t.Req.GetCollectionName() == "" {
		coll, err = t.core.meta.GetCollectionByID(ctx, t.Req.GetDbName(), t.Req.GetCollectionID(), typeutil.MaxTimestamp, t.allowUnavailable)
	} else {
		coll, err = t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	}
	if err != nil {
		t.Rsp.Status = failStatus(commonpb.ErrorCode_CollectionNotExists, err.Error())
		return err
	}

	for _, part := range coll.Partitions {
		t.Rsp.PartitionIDs = append(t.Rsp.PartitionIDs, part.PartitionID)
		t.Rsp.PartitionNames = append(t.Rsp.PartitionNames, part.PartitionName)
		t.Rsp.CreatedTimestamps = append(t.Rsp.CreatedTimestamps, part.PartitionCreatedTimestamp)
		physical, _ := tsoutil.ParseHybridTs(part.PartitionCreatedTimestamp)
		t.Rsp.CreatedUtcTimestamps = append(t.Rsp.CreatedUtcTimestamps, uint64(physical))
	}

	return nil
}
