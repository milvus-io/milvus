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

package session

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// SnapshotExportCluster is separate from legacy segment restore operations.
type SnapshotExportCluster interface {
	CreateSnapshotExportCopy(int64, *datapb.SnapshotExportCopyRequest) error
	QuerySnapshotExportCopy(int64, int64) (*datapb.SnapshotExportCopyResponse, error)
	DropSnapshotExportCopy(int64, int64) error
}

func snapshotExportProperties(taskID int64) taskcommon.Properties {
	p := taskcommon.NewProperties(nil)
	p.AppendClusterID(paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
	p.AppendTaskID(taskID)
	p.AppendType(taskcommon.SnapshotExportCopy)
	return p
}

func (c *cluster) CreateSnapshotExportCopy(nodeID int64, req *datapb.SnapshotExportCopyRequest) error {
	p := snapshotExportProperties(req.GetTaskId())
	// Use the frozen weight, including the legacy one-slot default, on both hops.
	p.AppendTaskSlot(max(req.GetTaskSlot(), 1))
	return c.createTask(nodeID, req, p)
}

func (c *cluster) QuerySnapshotExportCopy(nodeID, taskID int64) (*datapb.SnapshotExportCopyResponse, error) {
	response, err := c.queryTask(nodeID, snapshotExportProperties(taskID))
	if err != nil {
		return nil, err
	}
	properties := taskcommon.NewProperties(response.GetProperties())
	state, err := properties.GetTaskState()
	if err != nil {
		return nil, err
	}
	// Shared-pool managers can queue/retry tasks before a worker has a payload.
	// Such responses must retain the requested identity and the outer state.
	if state == taskcommon.None || state == taskcommon.Init || state == taskcommon.Retry || len(response.GetPayload()) == 0 {
		if state == taskcommon.Finished {
			return nil, merr.WrapErrServiceInternalMsg("snapshot export task %d finished without a result payload", taskID)
		}
		return &datapb.SnapshotExportCopyResponse{
			Status: merr.Success(), TaskId: taskID,
			State: taskcommon.ToImportState(state), Reason: properties.GetTaskReason(),
		}, nil
	}
	result := &datapb.SnapshotExportCopyResponse{}
	if err := proto.Unmarshal(response.GetPayload(), result); err != nil {
		return nil, err
	}
	if err := merr.CheckRPCCall(result.GetStatus(), nil); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *cluster) DropSnapshotExportCopy(nodeID, taskID int64) error {
	return c.dropTask(nodeID, snapshotExportProperties(taskID))
}
