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

package querycoord

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	defaultCollectionID     = UniqueID(2021)
	defaultPartitionID      = UniqueID(2021)
	defaultSegmentID        = UniqueID(2021)
	defaultQueryNodeID      = int64(100)
	defaultChannelNum       = 2
	defaultNumRowPerSegment = 1000
	defaultVecFieldID       = 101
)

func genDefaultCollectionSchema(isBinary bool) *schemapb.CollectionSchema {
	var fieldVec schemapb.FieldSchema
	if isBinary {
		fieldVec = schemapb.FieldSchema{
			FieldID:      UniqueID(defaultVecFieldID),
			Name:         "vec",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_BinaryVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "128",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "metric_type",
					Value: "JACCARD",
				},
			},
		}
	} else {
		fieldVec = schemapb.FieldSchema{
			FieldID:      UniqueID(defaultVecFieldID),
			Name:         "vec",
			IsPrimaryKey: false,
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: "16",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "metric_type",
					Value: "L2",
				},
			},
		}
	}

	return &schemapb.CollectionSchema{
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "row_id", IsPrimaryKey: false, Description: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: 1, Name: "Ts", IsPrimaryKey: false, Description: "Ts", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "field_age", IsPrimaryKey: false, Description: "int64", DataType: schemapb.DataType_Int64},
			&fieldVec,
		},
	}
}

func generateInsertBinLog(segmentID UniqueID) *datapb.SegmentBinlogs {
	schema := genDefaultCollectionSchema(false)
	sizePerRecord, _ := typeutil.EstimateSizePerRecord(schema)

	var fieldBinLogs []*datapb.FieldBinlog
	for _, field := range schema.Fields {
		fieldID := field.FieldID
		binlog := &datapb.Binlog{
			LogSize: int64(sizePerRecord * defaultNumRowPerSegment),
		}
		fieldBinLogs = append(fieldBinLogs, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{binlog},
		})
	}

	return &datapb.SegmentBinlogs{
		SegmentID:    segmentID,
		FieldBinlogs: fieldBinLogs,
		NumOfRows:    defaultNumRowPerSegment,
	}
}

type rootCoordMock struct {
	types.RootCoord
	CollectionIDs []UniqueID
	Col2partition map[UniqueID][]UniqueID
	sync.RWMutex
	returnError     bool
	returnGrpcError bool
	enableIndex     bool

	invalidateCollMetaCacheFailed bool
}

func newRootCoordMock(ctx context.Context) *rootCoordMock {
	collectionIDs := make([]UniqueID, 0)
	col2partition := make(map[UniqueID][]UniqueID)

	return &rootCoordMock{
		CollectionIDs: collectionIDs,
		Col2partition: col2partition,
	}
}

func (rc *rootCoordMock) createCollection(collectionID UniqueID) {
	rc.Lock()
	defer rc.Unlock()

	if _, ok := rc.Col2partition[collectionID]; !ok {
		rc.CollectionIDs = append(rc.CollectionIDs, collectionID)
		partitionIDs := make([]UniqueID, 0)
		partitionIDs = append(partitionIDs, defaultPartitionID+1)
		rc.Col2partition[collectionID] = partitionIDs
	}
}

func (rc *rootCoordMock) createPartition(collectionID UniqueID, partitionID UniqueID) error {
	rc.Lock()
	defer rc.Unlock()

	if partitionIDs, ok := rc.Col2partition[collectionID]; ok {
		partitionExist := false
		for _, id := range partitionIDs {
			if id == partitionID {
				partitionExist = true
				break
			}
		}
		if !partitionExist {
			rc.Col2partition[collectionID] = append(rc.Col2partition[collectionID], partitionID)
		}
		return nil
	}

	return errors.New("collection not exist")
}

func (rc *rootCoordMock) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	if rc.returnGrpcError {
		return nil, errors.New("show partitionIDs failed")
	}

	if rc.returnError {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "show partitionIDs failed",
		}, nil
	}

	rc.createPartition(defaultCollectionID, defaultPartitionID)
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (rc *rootCoordMock) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	if rc.returnGrpcError {
		return nil, errors.New("show partitionIDs failed")
	}

	if rc.returnError {
		return &milvuspb.ShowPartitionsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "show partitionIDs failed",
			},
		}, nil
	}

	collectionID := in.CollectionID
	rc.createCollection(collectionID)
	return &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs: rc.Col2partition[collectionID],
	}, nil
}

func (rc *rootCoordMock) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	if rc.returnGrpcError {
		return nil, errors.New("release DQLMessage stream failed")
	}

	if rc.returnError {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "release DQLMessage stream failed",
		}, nil
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (rc *rootCoordMock) InvalidateCollectionMetaCache(ctx context.Context, in *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	if rc.returnGrpcError {
		return nil, errors.New("InvalidateCollectionMetaCache failed")
	}

	if rc.returnError {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "InvalidateCollectionMetaCache failed",
		}, nil
	}

	if rc.invalidateCollMetaCacheFailed {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "InvalidateCollectionMetaCache failed",
		}, nil
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (rc *rootCoordMock) DescribeSegment(ctx context.Context, req *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	if rc.returnGrpcError {
		return nil, errors.New("describe segment failed")
	}

	if rc.returnError {
		return &milvuspb.DescribeSegmentResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "describe segment failed",
			},
		}, nil
	}

	return &milvuspb.DescribeSegmentResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		EnableIndex: rc.enableIndex,
	}, nil
}

func (rc *rootCoordMock) DescribeSegments(ctx context.Context, req *rootcoordpb.DescribeSegmentsRequest) (*rootcoordpb.DescribeSegmentsResponse, error) {
	if rc.returnGrpcError {
		return nil, errors.New("describe segment failed")
	}

	if rc.returnError {
		return &rootcoordpb.DescribeSegmentsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "describe segments failed",
			},
		}, nil
	}

	ret := &rootcoordpb.DescribeSegmentsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		CollectionID: req.GetCollectionID(),
		SegmentInfos: make(map[int64]*rootcoordpb.SegmentInfos),
	}

	for _, segID := range req.GetSegmentIDs() {
		ret.SegmentInfos[segID] = &rootcoordpb.SegmentInfos{
			IndexInfos: []*etcdpb.SegmentIndexInfo{
				{
					SegmentID:   segID,
					EnableIndex: rc.enableIndex,
				},
			},
			ExtraIndexInfos: nil,
		}
	}

	return ret, nil
}

type dataCoordMock struct {
	types.DataCoord
	collections         []UniqueID
	col2DmChannels      map[UniqueID][]*datapb.VchannelInfo
	partitionID2Segment map[UniqueID][]UniqueID
	Segment2Binlog      map[UniqueID]*datapb.SegmentBinlogs
	baseSegmentID       UniqueID
	channelNumPerCol    int
	returnError         bool
	returnGrpcError     bool
	returnErrorCount    atomic.Int32
	segmentState        commonpb.SegmentState
	errLevel            int

	globalLock      sync.Mutex
	segmentRefCount map[UniqueID]int
}

func newDataCoordMock(ctx context.Context) *dataCoordMock {
	collectionIDs := make([]UniqueID, 0)
	col2DmChannels := make(map[UniqueID][]*datapb.VchannelInfo)
	partitionID2Segments := make(map[UniqueID][]UniqueID)
	segment2Binglog := make(map[UniqueID]*datapb.SegmentBinlogs)

	return &dataCoordMock{
		collections:         collectionIDs,
		col2DmChannels:      col2DmChannels,
		partitionID2Segment: partitionID2Segments,
		Segment2Binlog:      segment2Binglog,
		baseSegmentID:       defaultSegmentID,
		channelNumPerCol:    defaultChannelNum,
		segmentState:        commonpb.SegmentState_Flushed,
		segmentRefCount:     make(map[int64]int),
	}
}

func (data *dataCoordMock) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	collectionID := req.CollectionID
	partitionID := req.PartitionID

	if data.returnGrpcError {
		return nil, errors.New("get recovery info failed")
	}

	if data.returnError {
		return &datapb.GetRecoveryInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "get recovery info failed",
			},
		}, nil
	}

	if data.returnErrorCount.Load() > 0 {
		data.returnErrorCount.Dec()
		return &datapb.GetRecoveryInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "limited get recovery info failed",
			},
		}, nil
	}

	if _, ok := data.col2DmChannels[collectionID]; !ok {
		channelInfos := make([]*datapb.VchannelInfo, 0)
		data.collections = append(data.collections, collectionID)
		for i := int32(0); i < common.DefaultShardsNum; i++ {
			vChannel := fmt.Sprintf("%s_%d_%dv%d", Params.CommonCfg.RootCoordDml, i, collectionID, i)
			channelInfo := &datapb.VchannelInfo{
				CollectionID:        collectionID,
				ChannelName:         vChannel,
				UnflushedSegmentIds: []int64{int64(i*1000 + 1)},
				FlushedSegmentIds:   []int64{int64(i*1000 + 2)},
				DroppedSegmentIds:   []int64{int64(i*1000 + 3)},
				SeekPosition: &internalpb.MsgPosition{
					ChannelName: vChannel,
				},
			}
			channelInfos = append(channelInfos, channelInfo)
		}
		data.col2DmChannels[collectionID] = channelInfos
	}

	if _, ok := data.partitionID2Segment[partitionID]; !ok {
		segmentIDs := make([]UniqueID, 0)
		for i := 0; i < data.channelNumPerCol; i++ {
			segmentID := data.baseSegmentID
			if _, ok := data.Segment2Binlog[segmentID]; !ok {
				segmentBinlog := generateInsertBinLog(segmentID)
				segmentBinlog.InsertChannel = data.col2DmChannels[collectionID][i].ChannelName
				data.Segment2Binlog[segmentID] = segmentBinlog
			}
			segmentIDs = append(segmentIDs, segmentID)
			data.baseSegmentID++
		}
		data.partitionID2Segment[partitionID] = segmentIDs
	}

	binlogs := make([]*datapb.SegmentBinlogs, 0)
	for _, segmentID := range data.partitionID2Segment[partitionID] {
		if _, ok := data.Segment2Binlog[segmentID]; ok {
			binlogs = append(binlogs, data.Segment2Binlog[segmentID])
		}
	}
	return &datapb.GetRecoveryInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Channels: data.col2DmChannels[collectionID],
		Binlogs:  binlogs,
	}, nil
}

func (data *dataCoordMock) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	if data.returnGrpcError {
		return nil, errors.New("get segment states failed")
	}

	if data.returnError {
		return &datapb.GetSegmentStatesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "get segment states failed",
			},
		}, nil
	}

	var segmentStates []*datapb.SegmentStateInfo
	for _, segmentID := range req.SegmentIDs {
		segmentStates = append(segmentStates, &datapb.SegmentStateInfo{
			SegmentID: segmentID,
			State:     data.segmentState,
		})
	}

	return &datapb.GetSegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		States: segmentStates,
	}, nil
}

func (data *dataCoordMock) AcquireSegmentLock(ctx context.Context, req *datapb.AcquireSegmentLockRequest) (*commonpb.Status, error) {
	if data.errLevel == 2 {
		data.errLevel++
		return nil, errors.New("AcquireSegmentLock failed")

	}
	if data.errLevel == 1 {
		data.errLevel++
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "AcquireSegmentLock failed",
		}, nil
	}

	data.globalLock.Lock()
	defer data.globalLock.Unlock()

	log.Debug("acquire segment locks",
		zap.Int64s("segments", req.SegmentIDs))
	for _, segment := range req.SegmentIDs {
		refCount := data.segmentRefCount[segment]
		refCount++
		data.segmentRefCount[segment] = refCount
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}
func (data *dataCoordMock) ReleaseSegmentLock(ctx context.Context, req *datapb.ReleaseSegmentLockRequest) (*commonpb.Status, error) {
	if data.errLevel == 4 {
		data.errLevel++
		return nil, errors.New("ReleaseSegmentLock failed")
	}

	if data.errLevel == 3 {
		data.errLevel++
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "ReleaseSegmentLock failed",
		}, nil
	}

	data.globalLock.Lock()
	defer data.globalLock.Unlock()

	log.Debug("release segment locks",
		zap.Int64s("segments", req.SegmentIDs))
	for _, segment := range req.SegmentIDs {
		refCount := data.segmentRefCount[segment]
		if refCount > 0 {
			refCount--
		}

		data.segmentRefCount[segment] = refCount
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (data *dataCoordMock) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	if data.returnGrpcError {
		return nil, errors.New("mock get segmentInfo failed")
	}
	if data.returnError {
		return &datapb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mock get segmentInfo failed",
			},
		}, nil
	}

	if data.returnErrorCount.Load() > 0 {
		data.returnErrorCount.Dec()
		return &datapb.GetSegmentInfoResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "limited mock get segmentInfo failed",
			},
		}, nil
	}

	var segmentInfos []*datapb.SegmentInfo
	for _, segmentID := range req.SegmentIDs {
		segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
			ID:    segmentID,
			State: data.segmentState,
		})
	}
	return &datapb.GetSegmentInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Infos: segmentInfos,
	}, nil
}

type indexCoordMock struct {
	types.IndexCoord
	chunkManager    storage.ChunkManager
	returnError     bool
	returnGrpcError bool
}

func newIndexCoordMock(path string) (*indexCoordMock, error) {
	cm := storage.NewLocalChunkManager(storage.RootPath(path))
	return &indexCoordMock{
		chunkManager: cm,
	}, nil
}

func (c *indexCoordMock) GetIndexFilePaths(ctx context.Context, req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	if c.returnGrpcError {
		return nil, errors.New("get index file paths failed")
	}

	if c.returnError {
		return &indexpb.GetIndexFilePathsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "get index file path failed",
			},
		}, nil
	}

	indexPathInfos, err := generateIndexFileInfo(req.IndexBuildIDs, c.chunkManager)
	if err != nil {
		return &indexpb.GetIndexFilePathsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
		}, nil
	}

	return &indexpb.GetIndexFilePathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		FilePaths: indexPathInfos,
	}, nil
}
