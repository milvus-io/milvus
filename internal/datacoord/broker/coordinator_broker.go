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

package broker

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

//go:generate mockery --name=Broker --structname=MockBroker --output=./  --filename=mock_coordinator_broker.go --with-expecter --inpackage
type Broker interface {
	DescribeCollectionInternal(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
	ShowPartitionsInternal(ctx context.Context, collectionID int64) ([]int64, error)
	ShowCollections(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error)
	ShowCollectionIDs(ctx context.Context) (*rootcoordpb.ShowCollectionIDsResponse, error)
	ListDatabases(ctx context.Context) (*milvuspb.ListDatabasesResponse, error)
	HasCollection(ctx context.Context, collectionID int64) (bool, error)
}

type coordinatorBroker struct {
	rootCoord types.RootCoordClient
}

func NewCoordinatorBroker(rootCoord types.RootCoordClient) *coordinatorBroker {
	return &coordinatorBroker{
		rootCoord: rootCoord,
	}
}

func (b *coordinatorBroker) DescribeCollectionInternal(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))

	resp, err := b.rootCoord.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("DescribeCollectionInternal failed", zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) ShowPartitionsInternal(ctx context.Context, collectionID int64) ([]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))

	resp, err := b.rootCoord.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("ShowPartitionsInternal failed",
			zap.Int64("collectionID", collectionID),
			zap.Error(err))
		return nil, err
	}

	return resp.GetPartitionIDs(), nil
}

func (b *coordinatorBroker) ShowCollections(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(zap.String("dbName", dbName))
	resp, err := b.rootCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
		),
		DbName: dbName,
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("ShowCollections failed",
			zap.String("dbName", dbName),
			zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) ShowCollectionIDs(ctx context.Context) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	resp, err := b.rootCoord.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
		),
		AllowUnavailable: true,
	})

	if err = merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("ShowCollectionIDs failed", zap.Error(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) ListDatabases(ctx context.Context) (*milvuspb.ListDatabasesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx)
	resp, err := b.rootCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to ListDatabases", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// HasCollection communicates with RootCoord and check whether this collection exist from the user's perspective.
func (b *coordinatorBroker) HasCollection(ctx context.Context, collectionID int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
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
	err = merr.Error(resp.GetStatus())
	if errors.Is(err, merr.ErrCollectionNotFound) {
		return false, nil
	}
	return err == nil, err
}
