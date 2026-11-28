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
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

type Task interface {
	GetTaskID() int64
	GetTaskType() taskcommon.Type
	GetTaskState() taskcommon.State
	GetTaskSlot() int64
	// GetTaskResource is the coordinator-side cpu/memory estimate the scheduler
	// places on and the request ships. Never zero; subject to memory admission. A
	// family whose inputs are still resolving answers with an upper bound on
	// itself, and one whose inputs are gone for good answers with the configured
	// floor. CoordinatorTask can retire local work before placement.
	//
	GetTaskResource() taskcommon.Resource
	SetTaskTime(timeType taskcommon.TimeType, time time.Time)
	GetTaskTime(timeType taskcommon.TimeType) time.Time
	GetTaskVersion() int64

	CreateTaskOnWorker(nodeID int64, cluster session.Cluster, resource taskcommon.Resource) DispatchResult
	QueryTaskOnWorker(cluster session.Cluster)
	DropTaskOnWorker(cluster session.Cluster)
}

// DispatchResult distinguishes a changed input that needs preparation from a
// completed dispatch attempt. TaskState remains authoritative for task progress.
type DispatchResult int

const (
	// DispatchHandled processes the resulting task state, including retry backoff.
	DispatchHandled DispatchResult = iota
	// DispatchRequeue leaves the task Init because its input changed before any
	// worker request. Requeue for preparation without changing accumulated failures.
	DispatchRequeue
)

// CoordinatorResult determines how the scheduler handles a local completion attempt.
type CoordinatorResult int

const (
	LocalCompleted CoordinatorResult = iota
	LocalNeedsWorker
	LocalRetry
)

// CoordinatorTask identifies local completion using cached metadata only.
// Completion may persist metadata and runs asynchronously with a timeout.
type CoordinatorTask interface {
	CanCompleteOnCoordinator() bool
	CompleteOnCoordinator(context.Context) CoordinatorResult
}

func WrapTaskLog(task Task, fields ...mlog.Field) []mlog.Field {
	res := []mlog.Field{
		mlog.Int64("ID", task.GetTaskID()),
		mlog.String("type", task.GetTaskType()),
		mlog.String("state", task.GetTaskState().String()),
	}
	res = append(res, fields...)
	return res
}
