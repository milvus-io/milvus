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

package indexcoord

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"go.uber.org/zap"
)

type globalMetaBroker struct {
	ctx    context.Context
	cancel context.CancelFunc

	dataCoord types.DataCoord
	rootCoord types.RootCoord
}

func newGlobalMetaBroker(ctx context.Context, dataCoord types.DataCoord, rootCoord types.RootCoord) (*globalMetaBroker, error) {
	childCtx, cancel := context.WithCancel(ctx)
	metaBroker := &globalMetaBroker{
		ctx:       childCtx,
		cancel:    cancel,
		dataCoord: dataCoord,
		rootCoord: rootCoord,
	}
	return metaBroker, nil
}

// getDataSegmentInfosByIDs return the SegmentInfo details according to the given ids through RPC to datacoord
func (broker *globalMetaBroker) getDataSegmentInfosByIDs(segmentIds []int64) ([]*datapb.SegmentInfo, error) {
	var segmentInfos []*datapb.SegmentInfo
	infoResp, err := broker.dataCoord.GetSegmentInfo(broker.ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		SegmentIDs:       segmentIds,
		IncludeUnHealthy: true,
	})
	if err != nil {
		log.Error("Fail to get datapb.SegmentInfo by ids from datacoord", zap.Error(err))
		return nil, err
	}
	if infoResp.GetStatus().ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(infoResp.GetStatus().Reason)
		log.Error("Fail to get datapb.SegmentInfo by ids from datacoord", zap.Error(err))
		return nil, err
	}
	segmentInfos = infoResp.Infos
	return segmentInfos, nil
}

// getFieldSchemaByID communicates with RootCoord and asks for collection information.
func (broker *globalMetaBroker) getFieldSchemaByID(collectionID, fieldID int64) (*schemapb.FieldSchema, error) {
	resp, err := broker.rootCoord.DescribeCollection(broker.ctx, &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_DescribeCollection,
			SourceID: Params.DataCoordCfg.GetNodeID(),
		},
		DbName:       "",
		CollectionID: collectionID,
	})
	if err != nil {
		log.Error("Fail to get schemapb.CollectionSchema by id from rootcoord", zap.Error(err))
		return nil, err
	}
	if resp.GetStatus().ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(resp.GetStatus().Reason)
		log.Error("Fail to get schemapb.CollectionSchema by id from rootcoord", zap.Error(err))
		return nil, err
	}

	for _, f := range resp.Schema.Fields {
		if fieldID == f.FieldID {
			return f, nil
		}
	}

	return nil, fmt.Errorf("field not found by collection id %d, field id %d", collectionID, fieldID)
}
