// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package datacoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

const (
	brokerRPCTimeout = 5 * time.Second
)

type Broker interface {
	DescribeCollectionInternal(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
	ShowPartitionsInternal(ctx context.Context, collectionID int64) ([]int64, error)
	ShowCollections(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error)
	ListDatabases(ctx context.Context) (*milvuspb.ListDatabasesResponse, error)
	HasCollection(ctx context.Context, collectionID int64) (bool, error)
}

type CoordinatorBroker struct {
	rootCoord types.RootCoord
}

func NewCoordinatorBroker(rootCoord types.RootCoord) *CoordinatorBroker {
	return &CoordinatorBroker{
		rootCoord: rootCoord,
	}
}

func (b *CoordinatorBroker) DescribeCollectionInternal(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()
	resp, err := b.rootCoord.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	})
	if err = VerifyResponse(resp, err); err != nil {
		log.Error("DescribeCollectionInternal failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (b *CoordinatorBroker) ShowPartitionsInternal(ctx context.Context, collectionID int64) ([]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()
	resp, err := b.rootCoord.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
			commonpbutil.WithMsgID(0),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	})
	if err = VerifyResponse(resp, err); err != nil {
		log.Error("ShowPartitionsInternal failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
		return nil, err
	}

	return resp.PartitionIDs, nil
}

func (b *CoordinatorBroker) ShowCollections(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()
	resp, err := b.rootCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
		),
		DbName: dbName,
	})

	if err = VerifyResponse(resp, err); err != nil {
		log.Warn("ShowCollections failed",
			zap.String("dbName", dbName),
			zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (b *CoordinatorBroker) ListDatabases(ctx context.Context) (*milvuspb.ListDatabasesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()
	resp, err := b.rootCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
	})
	if err = VerifyResponse(resp, err); err != nil {
		log.Warn("failed to ListDatabases", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// HasCollection communicates with RootCoord and check whether this collection exist from the user's perspective.
func (b *CoordinatorBroker) HasCollection(ctx context.Context, collectionID int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerRPCTimeout)
	defer cancel()
	resp, err := b.rootCoord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	})
	if err != nil {
		return false, err
	}
	if resp == nil {
		return false, errNilResponse
	}
	if resp.Status.ErrorCode == commonpb.ErrorCode_Success {
		return true, nil
	}
	statusErr := common.NewStatusError(resp.Status.ErrorCode, resp.Status.Reason)
	if common.IsCollectionNotExistError(statusErr) {
		return false, nil
	}
	return false, statusErr
}
