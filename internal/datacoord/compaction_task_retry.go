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

package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// classifyFailure decides whether a failed compaction attempt should retry in place
// or terminate the task, generalizing the bounded-retry pattern that previously only
// clusteringCompactionTask implemented.
//
// A retryable error (per merr.IsRetryableErr) under the configured retry budget
// increments retry_times and leaves the task's state untouched, so the task is
// reattempted the next time it is scheduled. Anything else -- a non-retryable error,
// or a retryable one that has exhausted its budget -- transitions the task straight
// to failed and records fail_reason.
//
// err must not be nil. Returns true when the task was left retryable (state unchanged),
// false when the task was transitioned to failed.
func classifyFailure(
	ctx context.Context,
	label string,
	taskProto *datapb.CompactionTask,
	err error,
	updateAndSaveTaskMeta func(opts ...compactionTaskOpt) error,
) bool {
	maxRetryTimes := paramtable.Get().DataCoordCfg.CompactionMaxRetryTimes.GetAsInt32()
	retryTimes := taskProto.GetRetryTimes()

	if merr.IsRetryableErr(err) && retryTimes < maxRetryTimes {
		mlog.Warn(ctx, label+" failed with a retryable error, will retry",
			mlog.Int64("planID", taskProto.GetPlanID()),
			mlog.Int32("retryTimes", retryTimes+1),
			mlog.Int32("maxRetryTimes", maxRetryTimes),
			mlog.Err(err))
		if saveErr := updateAndSaveTaskMeta(setRetryTimes(retryTimes + 1)); saveErr != nil {
			mlog.Warn(ctx, label+" failed to persist retry_times", mlog.Int64("planID", taskProto.GetPlanID()), mlog.Err(saveErr))
		}
		return true
	}

	mlog.Error(ctx, label+" failed with a non-retryable error or exhausted its retry budget, marking task failed",
		mlog.Int64("planID", taskProto.GetPlanID()),
		mlog.Int32("retryTimes", retryTimes),
		mlog.Int32("maxRetryTimes", maxRetryTimes),
		mlog.Err(err))
	if saveErr := updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_failed), setFailReason(err.Error())); saveErr != nil {
		mlog.Warn(ctx, label+" failed to persist failed state", mlog.Int64("planID", taskProto.GetPlanID()), mlog.Err(saveErr))
	}
	return false
}
