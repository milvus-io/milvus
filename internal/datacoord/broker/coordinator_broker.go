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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

//go:generate mockery --name=Broker --structname=MockBroker --output=./  --filename=mock_coordinator_broker.go --with-expecter --inpackage
type Broker interface {
	GetFileResources(ctx context.Context, resourceIDs ...int64) ([]*internalpb.FileResourceInfo, error)

	// CreateCollection creates a new collection via RootCoord.
	// Used by DataCoord-driven snapshot restore.
	CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) error

	// CreatePartition creates a new partition via RootCoord.
	// Used by DataCoord-driven snapshot restore.
	CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) error

	// DropCollection drops a collection via RootCoord.
	// Used for rollback when snapshot restore fails.
	DropCollection(ctx context.Context, dbName, collectionName string) error
}

type coordinatorBroker struct {
	mixCoord types.MixCoord
}

func NewCoordinatorBroker(mixCoord types.MixCoord) *coordinatorBroker {
	return &coordinatorBroker{
		mixCoord: mixCoord,
	}
}

func (b *coordinatorBroker) GetFileResources(ctx context.Context, resourceIDs ...int64) ([]*internalpb.FileResourceInfo, error) {
	if len(resourceIDs) == 0 {
		return nil, nil
	}

	resources, err := b.mixCoord.GetFileResources(ctx, resourceIDs...)
	if err != nil {
		mlog.Warn(ctx, "get file resources failed", mlog.Err(err))
		return nil, err
	}
	return resources, nil
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
