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

package meta

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	// GetQueryVChanPositions gets the information recovery needed of a channel for QueryCoord
	GetQueryVChanPositions(ch RWChannel, partitionIDs ...UniqueID) *datapb.VchannelInfo
	// GetDataVChanPositions gets the information recovery needed of a channel for DataNode
	GetDataVChanPositions(ch RWChannel, partitionID UniqueID) *datapb.VchannelInfo
	CheckShouldDropChannel(ch string) bool
	FinishDropChannel(ch string, collectionID int64) error
	GetCollection(ctx context.Context, collectionID UniqueID) (*CollectionInfo, error)
	GetCurrentSegmentsView(ctx context.Context, channel RWChannel, partitionIDs ...UniqueID) *SegmentsView
	ListLoadedSegments(ctx context.Context) ([]int64, error)
	GenSnapshot(ctx context.Context, collectionID UniqueID, channelSeekPositions []*msgpb.MsgPosition) (*snapshotstorage.SnapshotData, error)
	GetDeltaLogFromCompactTo(ctx context.Context, segmentID UniqueID) ([]*datapb.FieldBinlog, error)
}

type SegmentsView struct {
	FlushedSegmentIDs   []int64
	GrowingSegmentIDs   []int64
	DroppedSegmentIDs   []int64
	L0SegmentIDs        []int64
	ImportingSegmentIDs []int64
}
