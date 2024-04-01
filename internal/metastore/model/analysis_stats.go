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

package model

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type AnalyzeTask struct {
	TenantID     string
	CollectionID int64
	PartitionID  int64
	FieldID      int64
	FieldName    string
	FieldType    schemapb.DataType
	SegmentIDs   []int64
	TaskID       int64
	Version      int64
	NodeID       int64
	State        indexpb.JobState
	FailReason   string
	Dim          int64
	Deleted      bool
}

func UnmarshalAnalyzeTask(info *indexpb.AnalyzeTask) *AnalyzeTask {
	if info == nil {
		return nil
	}
	return &AnalyzeTask{
		TenantID:     "",
		CollectionID: info.GetCollectionID(),
		PartitionID:  info.GetPartitionID(),
		FieldID:      info.GetFieldID(),
		FieldName:    info.GetFieldName(),
		FieldType:    info.GetFieldType(),
		SegmentIDs:   info.GetSegmentIDs(),
		TaskID:       info.GetTaskID(),
		Version:      info.GetVersion(),
		NodeID:       info.GetNodeID(),
		State:        info.GetState(),
		FailReason:   info.GetFailReason(),
		Dim:          info.GetDim(),
	}
}

func MarshalAnalyzeTask(t *AnalyzeTask) *indexpb.AnalyzeTask {
	if t == nil {
		return nil
	}

	return &indexpb.AnalyzeTask{
		CollectionID: t.CollectionID,
		PartitionID:  t.PartitionID,
		FieldID:      t.FieldID,
		FieldName:    t.FieldName,
		FieldType:    t.FieldType,
		TaskID:       t.TaskID,
		Version:      t.Version,
		SegmentIDs:   t.SegmentIDs,
		NodeID:       t.NodeID,
		State:        t.State,
		FailReason:   t.FailReason,
		Dim:          t.Dim,
	}
}

func CloneAnalyzeTask(t *AnalyzeTask) *AnalyzeTask {
	if t == nil {
		return t
	}
	return &AnalyzeTask{
		TenantID:     t.TenantID,
		CollectionID: t.CollectionID,
		PartitionID:  t.PartitionID,
		FieldID:      t.FieldID,
		FieldName:    t.FieldName,
		FieldType:    t.FieldType,
		SegmentIDs:   t.SegmentIDs,
		TaskID:       t.TaskID,
		Version:      t.Version,
		NodeID:       t.NodeID,
		State:        t.State,
		FailReason:   t.FailReason,
		Dim:          t.Dim,
	}
}
