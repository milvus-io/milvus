// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package grpcquerynode

import (
	"path"
	"strconv"

	"errors"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	internalPb "github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const (
	collectionID = 1

	binlogPathPrefix = "distributed-query-test-binlog"
	indexPathPrefix  = "distributed-query-test-index"

	uidFieldID       = 0
	timestampFieldID = 1
	vecFieldID       = 100
	ageFieldID       = 101
	vecParamsID      = "indexParams"
	vecDataID        = "IVF"
)

var fieldIDs = []int64{uidFieldID, timestampFieldID, vecFieldID, ageFieldID}

/*
 masterMock receive segmentID  ,return indexID, segmentID = IndexID
 dataMock return binlogPath, path = distributed-query-test-binlog/collectionID/segmentID/fieldID
 indexMock return indexPath and IndexParam, indexPath = distributed-query-test-index/collectionID/segmentID/indexID,
			indexParam use default:

indexID: 1

schema:
	collectionID: 1
	partitionID: 1
	segmentID: [1, 10]
	0: int64: uid
	1: int64: timestamp
	100: float32: vec: 16
	101: int32: age

indexParams:
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = "16"
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "4"
*/

type RootCoordMock struct {
	Count int
}

func (m *RootCoordMock) DescribeSegment(in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error) {
	if m.Count < 20 {
		m.Count++
		return nil, errors.New("index not exit")
	}
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = "16"
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "4"

	params := make([]*commonpb.KeyValuePair, 0)
	for k, v := range indexParams {
		params = append(params, &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		})
	}
	rsp := &milvuspb.DescribeSegmentResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexID: in.SegmentID, // use index id as segment id
		BuildID: in.SegmentID,
	}
	return rsp, nil
}

type DataCoordMock struct {
	Count int
}

func (data *DataCoordMock) GetInsertBinlogPaths(req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	if data.Count < 10 {
		data.Count++
		return nil, errors.New("binlog not exist")
	}
	paths := make([]*internalPb.StringList, len(fieldIDs))
	for i := range paths {
		pathKey := path.Join(binlogPathPrefix,
			strconv.FormatInt(collectionID, 10),
			strconv.FormatInt(req.SegmentID, 10),
			strconv.FormatInt(fieldIDs[i], 10))
		paths[i] = &internalPb.StringList{
			Values: []string{pathKey},
		}
	}
	rsp := &datapb.GetInsertBinlogPathsResponse{
		FieldIDs: fieldIDs,
		Paths:    paths,
	}
	return rsp, nil
}

func (data *DataCoordMock) GetSegmentStates(req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	segmentGrowingInfo := &datapb.SegmentStateInfo{
		State: commonpb.SegmentState_Growing,
	}
	segmentFlushedInfo := &datapb.SegmentStateInfo{
		State: commonpb.SegmentState_Flushed,
	}

	if data.Count < 10 {
		data.Count++
		return &datapb.GetSegmentStatesResponse{
			States: []*datapb.SegmentStateInfo{segmentGrowingInfo},
		}, nil
	}

	return &datapb.GetSegmentStatesResponse{
		States: []*datapb.SegmentStateInfo{segmentFlushedInfo},
	}, nil
}

type IndexCoordMock struct {
	Count int
}

func (index *IndexCoordMock) GetIndexFilePaths(req *indexpb.GetIndexFilePathsRequest) (*indexpb.GetIndexFilePathsResponse, error) {
	if index.Count < 30 {
		index.Count++
		return nil, errors.New("index path not exist")
	}
	if len(req.IndexBuildIDs) != 1 {
		panic("illegal index ids")
	}
	segmentID := req.IndexBuildIDs[0] // use index id as segment id
	indexPaths1 := path.Join(indexPathPrefix,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(segmentID, 10),
		vecDataID)
	indexPaths2 := path.Join(indexPathPrefix,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(segmentID, 10),
		vecParamsID)
	indexPathInfo := make([]*indexpb.IndexFilePathInfo, 1)
	indexPathInfo[0] = &indexpb.IndexFilePathInfo{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		IndexFilePaths: []string{indexPaths1, indexPaths2},
	}
	rsp := &indexpb.GetIndexFilePathsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		FilePaths: indexPathInfo,
	}
	return rsp, nil
}
