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

package task

import (
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
)

type Task interface {
	GetTaskID() int64
	GetTaskType() taskcommon.Type
	GetTaskState() taskcommon.State
	// GetTaskSlot returns the task slot usage (deprecated, for backward compatibility).
	// Use GetTaskSlotV2() for separate CPU and memory slot accounting.
	GetTaskSlot() int64
	// GetTaskSlotV2 returns (cpuSlot, memorySlot) representing the resource requirements for this task.
	GetTaskSlotV2() (float64, float64)
	// AllowCpuOversubscription returns true if this task allows CPU over-subscription.
	// CPU-intensive tasks (e.g., vector index building) should return false to ensure dedicated CPU resources.
	// Memory/IO-intensive tasks (e.g., stats, scalar index, compaction) can return true for better resource utilization.
	AllowCpuOversubscription() bool
	SetTaskTime(timeType taskcommon.TimeType, time time.Time)
	GetTaskTime(timeType taskcommon.TimeType) time.Time
	GetTaskVersion() int64

	CreateTaskOnWorker(nodeID int64, cluster session.Cluster)
	QueryTaskOnWorker(cluster session.Cluster)
	DropTaskOnWorker(cluster session.Cluster)
}

func WrapTaskLog(task Task, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("ID", task.GetTaskID()),
		zap.String("type", string(task.GetTaskType())),
		zap.String("state", task.GetTaskState().String()),
	}
	res = append(res, fields...)
	return res
}
