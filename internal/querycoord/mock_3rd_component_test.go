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

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
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

func (rc *rootCoordMock) ReleaseDQLCache(ctx context.Context, in *proxypb.ReleaseDQLCacheRequest) (*commonpb.Status, error) {
	if rc.returnGrpcError {
		return nil, errors.New("release DQL cache failed")
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
	segmentState        commonpb.SegmentState
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

	if _, ok := data.partitionID2Segment[partitionID]; !ok {
		segmentIDs := make([]UniqueID, 0)
		for i := 0; i < data.channelNumPerCol; i++ {
			segmentID := data.baseSegmentID
			if _, ok := data.Segment2Binlog[segmentID]; !ok {
				segmentBinlog := generateInsertBinLog(segmentID)
				data.Segment2Binlog[segmentID] = segmentBinlog
			}
			segmentIDs = append(segmentIDs, segmentID)
			data.baseSegmentID++
		}
		data.partitionID2Segment[partitionID] = segmentIDs
	}

	if _, ok := data.col2DmChannels[collectionID]; !ok {
		channelInfos := make([]*datapb.VchannelInfo, 0)
		data.collections = append(data.collections, collectionID)
		collectionName := funcutil.RandomString(8)
		for i := int32(0); i < common.DefaultShardsNum; i++ {
			vChannel := fmt.Sprintf("Dml_%s_%d_%d_v", collectionName, collectionID, i)
			channelInfo := &datapb.VchannelInfo{
				CollectionID: collectionID,
				ChannelName:  vChannel,
				SeekPosition: &internalpb.MsgPosition{
					ChannelName: vChannel,
				},
			}
			channelInfos = append(channelInfos, channelInfo)
		}
		data.col2DmChannels[collectionID] = channelInfos
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

type indexCoordMock struct {
	types.IndexCoord
	dataKv          kv.DataKV
	returnError     bool
	returnGrpcError bool
}

func newIndexCoordMock(ctx context.Context) (*indexCoordMock, error) {
	option := &minioKV.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}

	kv, err := minioKV.NewMinIOKV(context.Background(), option)
	if err != nil {
		return nil, err
	}
	return &indexCoordMock{
		dataKv: kv,
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

	indexPathInfos, err := generateIndexFileInfo(req.IndexBuildIDs, c.dataKv)
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
