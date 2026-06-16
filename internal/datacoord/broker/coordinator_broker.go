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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

//go:generate mockery --name=Broker --structname=MockBroker --output=./  --filename=mock_coordinator_broker.go --with-expecter --inpackage
type Broker interface {
	DescribeCollectionInternal(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
	DescribeCollectionByName(ctx context.Context, dbName, collectionName string) (*milvuspb.DescribeCollectionResponse, error)
	ShowPartitionsInternal(ctx context.Context, collectionID int64) ([]int64, error)
	ShowCollections(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error)
	ShowCollectionIDs(ctx context.Context, dbNames ...string) (*rootcoordpb.ShowCollectionIDsResponse, error)
	ListDatabases(ctx context.Context) (*milvuspb.ListDatabasesResponse, error)
	HasCollection(ctx context.Context, collectionID int64) (bool, error)
	ShowPartitions(ctx context.Context, collectionID int64) (*milvuspb.ShowPartitionsResponse, error)

	// CreateCollection creates a new collection via RootCoord.
	// Used by DataCoord-driven snapshot restore.
	CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) error

	// CreatePartition creates a new partition via RootCoord.
	// Used by DataCoord-driven snapshot restore.
	CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) error

	// DropCollection drops a collection via RootCoord.
	// Used for rollback when snapshot restore fails.
	DropCollection(ctx context.Context, dbName, collectionName string) error

	// DescribeDatabase retrieves database information via RootCoord.
	// Used for CMEK validation during snapshot restore.
	DescribeDatabase(ctx context.Context, dbName string) (*rootcoordpb.DescribeDatabaseResponse, error)

	// CommitShardSplitRouting commits a shard-split routing change into the
	// collection meta via RootCoord (the write-switch routing commit and the
	// adoption flip of a split). The call is idempotent by shard state.
	CommitShardSplitRouting(ctx context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error
}

type coordinatorBroker struct {
	mixCoord types.MixCoord
}

func NewCoordinatorBroker(mixCoord types.MixCoord) *coordinatorBroker {
	return &coordinatorBroker{
		mixCoord: mixCoord,
	}
}

func (b *coordinatorBroker) DescribeCollectionInternal(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(mlog.FieldCollectionID(collectionID))

	resp, err := b.mixCoord.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "DescribeCollectionInternal failed", mlog.Err(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) DescribeCollectionByName(ctx context.Context, dbName, collectionName string) (*milvuspb.DescribeCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(mlog.FieldDbName(dbName), mlog.FieldCollectionName(collectionName))

	resp, err := b.mixCoord.DescribeCollectionInternal(ctx, &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		DbName:         dbName,
		CollectionName: collectionName,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "DescribeCollectionByName failed", mlog.Err(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) ShowPartitionsInternal(ctx context.Context, collectionID int64) ([]int64, error) {
	resp, err := b.ShowPartitions(ctx, collectionID)
	if err != nil {
		return nil, err
	}

	return resp.GetPartitionIDs(), nil
}

func (b *coordinatorBroker) ShowPartitions(ctx context.Context, collectionID int64) (*milvuspb.ShowPartitionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(mlog.FieldCollectionID(collectionID))

	resp, err := b.mixCoord.ShowPartitionsInternal(ctx, &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "ShowPartitionsInternal failed",
			mlog.FieldCollectionID(collectionID),
			mlog.Err(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) ShowCollections(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(mlog.FieldDbName(dbName))
	resp, err := b.mixCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
		),
		DbName: dbName,
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "ShowCollections failed",
			mlog.FieldDbName(dbName),
			mlog.Err(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) ShowCollectionIDs(ctx context.Context, dbNames ...string) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	resp, err := b.mixCoord.ShowCollectionIDs(ctx, &rootcoordpb.ShowCollectionIDsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections),
		),
		AllowUnavailable: true,
		DbNames:          dbNames,
	})

	if err = merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(ctx, "ShowCollectionIDs failed", mlog.Err(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) ListDatabases(ctx context.Context) (*milvuspb.ListDatabasesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	resp, err := b.mixCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		mlog.Warn(ctx, "failed to ListDatabases", mlog.Err(err))
		return nil, err
	}
	return resp, nil
}

// HasCollection communicates with RootCoord and check whether this collection exist from the user's perspective.
func (b *coordinatorBroker) HasCollection(ctx context.Context, collectionID int64) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	resp, err := b.mixCoord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
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

// CreateCollection creates a new collection via RootCoord.
// Used by DataCoord-driven snapshot restore.
func (b *coordinatorBroker) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) error {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(
		mlog.FieldDbName(req.GetDbName()),
		mlog.FieldCollectionName(req.GetCollectionName()),
	)

	if req.Base == nil {
		req.Base = commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_CreateCollection),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		)
	}

	resp, err := b.mixCoord.CreateCollection(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "CreateCollection failed", mlog.Err(err))
		return err
	}

	log.Info(ctx, "CreateCollection succeeded")
	return nil
}

// CreatePartition creates a new partition via RootCoord.
// Used by DataCoord-driven snapshot restore.
func (b *coordinatorBroker) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) error {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(
		mlog.FieldDbName(req.GetDbName()),
		mlog.FieldCollectionName(req.GetCollectionName()),
		mlog.FieldPartitionName(req.GetPartitionName()),
	)

	if req.Base == nil {
		req.Base = commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_CreatePartition),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		)
	}

	resp, err := b.mixCoord.CreatePartition(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "CreatePartition failed", mlog.Err(err))
		return err
	}

	log.Info(ctx, "CreatePartition succeeded")
	return nil
}

// DropCollection drops a collection via RootCoord.
// Used for rollback when snapshot restore fails.
func (b *coordinatorBroker) DropCollection(ctx context.Context, dbName, collectionName string) error {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(
		mlog.FieldDbName(dbName),
		mlog.FieldCollectionName(collectionName),
	)

	resp, err := b.mixCoord.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DropCollection),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		DbName:         dbName,
		CollectionName: collectionName,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "DropCollection failed", mlog.Err(err))
		return err
	}

	log.Info(ctx, "DropCollection succeeded")
	return nil
}

// DescribeDatabase retrieves database information via RootCoord.
// Used for CMEK validation during snapshot restore.
func (b *coordinatorBroker) DescribeDatabase(ctx context.Context, dbName string) (*rootcoordpb.DescribeDatabaseResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := mlog.With(mlog.FieldDbName(dbName))

	resp, err := b.mixCoord.DescribeDatabase(ctx, &rootcoordpb.DescribeDatabaseRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeDatabase),
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		DbName: dbName,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn(ctx, "DescribeDatabase failed", mlog.Err(err))
		return nil, err
	}

	return resp, nil
}

func (b *coordinatorBroker) CommitShardSplitRouting(ctx context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.String("collectionName", req.GetCollectionName()),
		zap.Int64("collectionID", req.GetCollectionId()),
	)

	if req.Base == nil {
		req.Base = commonpbutil.NewMsgBase(commonpbutil.WithSourceID(paramtable.GetNodeID()))
	}

	resp, err := b.mixCoord.CommitShardSplitRouting(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("CommitShardSplitRouting failed", zap.Error(err))
		return err
	}

	log.Info("CommitShardSplitRouting succeeded")
	return nil
}
