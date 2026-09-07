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

	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	DataViewManager               = dataview.Manager
	Projector                     = dataview.Projector
	LoadableSegment               = dataview.LoadableSegment
	CreateCollectionDataViewEvent = dataview.CreateCollectionDataViewEvent
	FlushDataViewEvent            = dataview.FlushDataViewEvent
)

// dataViewSegmentMemSize estimates the in-memory footprint of a Segment once
// loaded onto a QueryNode, as the sum of its binlog / statslog / bm25-statslog
// payload sizes. It feeds the DataView manager's per-segment MemSize, which
// stays in memory and never enters the viewpb wire format.
func dataViewSegmentMemSize(segment *SegmentInfo) int64 {
	if segment == nil {
		return 0
	}
	var total int64
	for _, fieldBinlog := range segment.GetBinlogs() {
		total += fieldBinlogMemSize(fieldBinlog)
	}
	for _, fieldBinlog := range segment.GetStatslogs() {
		total += fieldBinlogMemSize(fieldBinlog)
	}
	for _, fieldBinlog := range segment.GetBm25Statslogs() {
		total += fieldBinlogMemSize(fieldBinlog)
	}
	return total
}

func fieldBinlogMemSize(fieldBinlog *datapb.FieldBinlog) int64 {
	var total int64
	for _, binlog := range fieldBinlog.GetBinlogs() {
		memorySize := binlog.GetMemorySize()
		if memorySize == 0 {
			memorySize = binlog.GetLogSize()
		}
		total += memorySize
	}
	return total
}

func (s *Server) CreateCollectionDataView(ctx context.Context, collectionID int64, vchannels []string) (*viewpb.DataVersion, error) {
	if s.dataViewManager == nil {
		return nil, merr.WrapErrServiceInternalMsg("DataView manager is not initialized")
	}
	return s.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{
		CollectionID: collectionID,
		VChannels:    vchannels,
	})
}

func (s *Server) DropCollectionDataView(ctx context.Context, collectionID int64) error {
	if s.dataViewManager == nil {
		return merr.WrapErrServiceInternalMsg("DataView manager is not initialized")
	}
	_, err := s.dataViewManager.OnDropCollection(ctx, collectionID)
	return err
}
