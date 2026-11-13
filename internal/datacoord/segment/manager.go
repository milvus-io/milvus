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

package segment

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type AllocNewGrowingSegmentRequest struct {
	CollectionID         UniqueID
	PartitionID          UniqueID
	SegmentID            UniqueID
	ChannelName          string
	StorageVersion       int64
	IsCreatedByStreaming bool
	SchemaVersion        int32
}

// Manager registers the growing segments that the streaming node allocates.
//
// The streaming node owns the lifecycle of a growing segment: it picks the
// segment ID, decides when the segment is full and reports the flush through
// SaveBinlogPaths. DataCoord only records the segment in its meta.
//
//go:generate mockery --name=Manager --structname=MockManager --output=./  --filename=mock_segment_manager.go --with-expecter --inpackage
type Manager interface {
	// AllocNewGrowingSegment adds a growing segment with the given ID into the segment meta.
	AllocNewGrowingSegment(ctx context.Context, req AllocNewGrowingSegmentRequest) (*meta.SegmentInfo, error)
}

// make sure SegmentManager implements Manager
var _ Manager = (*SegmentManager)(nil)

// SegmentManager is the default implementation of Manager.
type SegmentManager struct {
	meta *meta.Meta
}

// NewSegmentManager should be the only way to retrieve SegmentManager.
func NewSegmentManager(mt *meta.Meta) *SegmentManager {
	return &SegmentManager{meta: mt}
}

// AllocNewGrowingSegment adds a growing segment with the given ID into the segment meta.
func (s *SegmentManager) AllocNewGrowingSegment(ctx context.Context, req AllocNewGrowingSegmentRequest) (*meta.SegmentInfo, error) {
	var manifestPath string
	if req.StorageVersion == storage.StorageV3 {
		basePath := storage.SegmentManifestBasePath(meta.CreateStorageConfig().GetRootPath(), req.CollectionID, req.PartitionID, req.SegmentID)
		manifestPath = packed.MarshalManifestPath(basePath, packed.ManifestEarliest)
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:                   req.SegmentID,
		CollectionID:         req.CollectionID,
		PartitionID:          req.PartitionID,
		InsertChannel:        req.ChannelName,
		NumOfRows:            0,
		State:                commonpb.SegmentState_Growing,
		Level:                datapb.SegmentLevel_L1,
		StorageVersion:       req.StorageVersion,
		IsCreatedByStreaming: req.IsCreatedByStreaming,
		ManifestPath:         manifestPath,
		SchemaVersion:        req.SchemaVersion,
	}
	segment := meta.NewSegmentInfo(segmentInfo)
	if err := s.meta.AddSegment(ctx, segment); err != nil {
		mlog.Error(ctx, "failed to add segment to DataCoord", mlog.Err(err))
		return nil, err
	}
	mlog.Info(ctx, "growing segment registered",
		mlog.Int64("CollectionID", segmentInfo.CollectionID),
		mlog.Int64("SegmentID", segmentInfo.ID),
		mlog.String("Channel", segmentInfo.InsertChannel),
		mlog.Bool("IsCreatedByStreaming", segmentInfo.IsCreatedByStreaming),
		mlog.Int32("SchemaVersion", segmentInfo.SchemaVersion),
	)
	return segment, nil
}
