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
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type ImportTaskFilter func(task ImportTask) bool

func WithStates(states ...datapb.ImportState) ImportTaskFilter {
	return func(task ImportTask) bool {
		for _, state := range states {
			if task.State() == state {
				return true
			}
		}
		return false
	}
}

func WithRequestID(reqID int64) ImportTaskFilter {
	return func(task ImportTask) bool {
		return task.ReqID() == reqID
	}
}

type UpdateAction func(task ImportTask)

func UpdateState(state datapb.ImportState) UpdateAction {
	return func(t ImportTask) {
		t.(*importTask).ImportTaskV2.State = state
	}
}

func UpdateFileInfo(infos []*datapb.ImportFileInfo) UpdateAction {
	return func(t ImportTask) {
		t.(*importTask).ImportTaskV2.FileInfos = infos
	}
}

func UpdateNodeID(nodeID int64) UpdateAction {
	return func(t ImportTask) {
		t.(*importTask).ImportTaskV2.NodeID = nodeID
	}
}

type ImportTask interface {
	ID() int64
	ReqID() int64
	NodeID() int64
	CollectionID() int64
	PartitionID() int64
	SegmentIDs() []int64
	State() datapb.ImportState
	Schema() *schemapb.CollectionSchema
	FileInfos() []*datapb.ImportFileInfo
	Clone() ImportTask
}

type importTask struct {
	*datapb.ImportTaskV2
	schema *schemapb.CollectionSchema
}

func (t *importTask) ID() int64 {
	return t.GetTaskID()
}

func (t *importTask) ReqID() int64 {
	return t.GetRequestID()
}

func (t *importTask) NodeID() int64 {
	return t.GetNodeID()
}

func (t *importTask) CollectionID() int64 {
	return t.GetCollectionID()
}

func (t *importTask) PartitionID() int64 {
	return t.GetPartitionID()
}

func (t *importTask) SegmentIDs() []int64 {
	return t.GetSegmentIDs()
}

func (t *importTask) State() datapb.ImportState {
	return t.GetState()
}

func (t *importTask) Schema() *schemapb.CollectionSchema {
	return t.schema
}

func (t *importTask) FileInfos() []*datapb.ImportFileInfo {
	return t.GetFileInfos()
}

func (t *importTask) Clone() ImportTask {
	return &importTask{
		ImportTaskV2: proto.Clone(t.ImportTaskV2).(*datapb.ImportTaskV2),
		schema:       t.Schema(),
	}
}
