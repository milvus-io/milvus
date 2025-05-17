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

package taskcommon

import (
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type State = indexpb.JobState

const (
	None       = indexpb.JobState_JobStateNone
	Init       = indexpb.JobState_JobStateInit
	InProgress = indexpb.JobState_JobStateInProgress
	Finished   = indexpb.JobState_JobStateFinished
	Failed     = indexpb.JobState_JobStateFailed
	Retry      = indexpb.JobState_JobStateRetry
)

func FromImportState(s datapb.ImportTaskStateV2) State {
	switch s {
	case datapb.ImportTaskStateV2_Pending:
		return Init
	case datapb.ImportTaskStateV2_InProgress:
		return InProgress
	case datapb.ImportTaskStateV2_Failed:
		return Failed
	case datapb.ImportTaskStateV2_Completed:
		return Finished
	}
	return None
}

func ToImportState(s State) datapb.ImportTaskStateV2 {
	switch s {
	case Init:
		return datapb.ImportTaskStateV2_Pending
	case InProgress:
		return datapb.ImportTaskStateV2_InProgress
	case Failed:
		return datapb.ImportTaskStateV2_Failed
	case Finished:
		return datapb.ImportTaskStateV2_Completed
	}
	return datapb.ImportTaskStateV2_None
}

func FromCompactionState(s datapb.CompactionTaskState) State {
	switch s {
	case datapb.CompactionTaskState_pipelining:
		return Init
	case datapb.CompactionTaskState_executing:
		return InProgress
	case datapb.CompactionTaskState_completed, datapb.CompactionTaskState_meta_saved,
		datapb.CompactionTaskState_statistic, datapb.CompactionTaskState_indexing, datapb.CompactionTaskState_cleaned:
		return Finished
	case datapb.CompactionTaskState_failed:
		return Failed
	case datapb.CompactionTaskState_timeout:
		return Retry
	}
	return None
}

func ToCompactionState(s State) datapb.CompactionTaskState {
	switch s {
	case Init:
		return datapb.CompactionTaskState_pipelining
	case InProgress:
		return datapb.CompactionTaskState_executing
	case Finished:
		return datapb.CompactionTaskState_completed
	case Failed:
		return datapb.CompactionTaskState_failed
	case Retry:
		return datapb.CompactionTaskState_timeout
	}
	return datapb.CompactionTaskState_unknown
}
