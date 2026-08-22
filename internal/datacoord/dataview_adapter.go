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
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type (
	DataViewManager                  = dataview.Manager
	CreateCollectionDataViewEvent    = dataview.CreateCollectionDataViewEvent
	FlushDataViewEvent               = dataview.FlushDataViewEvent
	ImportDataViewEvent              = dataview.ImportDataViewEvent
	CopySegmentCompleteDataViewEvent = dataview.CopySegmentCompleteDataViewEvent
	CompactDataViewEvent             = dataview.CompactDataViewEvent
	L0CompactDataViewEvent           = dataview.L0CompactDataViewEvent
	SegmentManifestVersion           = dataview.SegmentManifestVersion
	ExternalRefreshDataViewEvent     = dataview.ExternalRefreshDataViewEvent
	DropPartitionDataViewEvent       = dataview.DropPartitionDataViewEvent
	TruncateDataViewEvent            = dataview.TruncateDataViewEvent
)

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
