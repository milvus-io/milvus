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

package rootcoord

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type transferredCollectionApplier interface {
	ApplyTransferredCollection(ctx context.Context, coll *model.Collection) error
}

func (c *Core) CatalogTransferPrepare(ctx context.Context, req *rootcoordpb.CatalogTransferPrepareRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if req == nil {
		return merr.Status(merr.WrapErrParameterMissing("CatalogTransferPrepareRequest")), nil
	}
	if err := validateCatalogTransferIdentity(req.GetTransferId(), req.GetTransferEpoch()); err != nil {
		return merr.Status(err), nil
	}

	if err := c.transferGate.FreezeWithDrain(
		req.GetCollectionId(),
		req.GetTransferId(),
		req.GetTransferEpoch(),
		time.Duration(req.GetDrainTimeoutMs())*time.Millisecond,
	); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (c *Core) CatalogTransferDeactivate(ctx context.Context, req *rootcoordpb.CatalogTransferDeactivateRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if req == nil {
		return merr.Status(merr.WrapErrParameterMissing("CatalogTransferDeactivateRequest")), nil
	}
	if err := validateCatalogTransferIdentity(req.GetTransferId(), req.GetTransferEpoch()); err != nil {
		return merr.Status(err), nil
	}

	if err := c.transferGate.Deactivate(req.GetCollectionId(), req.GetTransferId(), req.GetTransferEpoch()); err != nil {
		return merr.Status(err), nil
	}
	if err := c.ExpireTransferMetaCache(
		ctx,
		req.GetDbName(),
		req.GetCollectionName(),
		req.GetAliases(),
		req.GetCollectionId(),
		req.GetCacheExpireTs(),
	); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (c *Core) CatalogTransferApply(ctx context.Context, req *rootcoordpb.CatalogTransferApplyRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if req == nil {
		return merr.Status(merr.WrapErrParameterMissing("CatalogTransferApplyRequest")), nil
	}
	if err := validateCatalogTransferIdentity(req.GetTransferId(), req.GetTransferEpoch()); err != nil {
		return merr.Status(err), nil
	}

	coll := model.UnmarshalCollectionModel(req.GetCollection())
	if coll == nil {
		return merr.Status(merr.WrapErrParameterMissing("collection")), nil
	}
	coll.Partitions = make([]*model.Partition, 0, len(req.GetPartitions()))
	for _, partitionInfo := range req.GetPartitions() {
		if partitionInfo == nil {
			return merr.Status(merr.WrapErrParameterInvalidMsg("nil partition in transfer request, collection id: %d", coll.CollectionID)), nil
		}
		partition := model.UnmarshalPartitionModel(partitionInfo)
		if partition.CollectionID != coll.CollectionID {
			return merr.Status(merr.WrapErrParameterInvalidMsg("partition collection id mismatch, collection id: %d, partition id: %d, partition collection id: %d", coll.CollectionID, partition.PartitionID, partition.CollectionID)), nil
		}
		coll.Partitions = append(coll.Partitions, partition)
	}
	coll.Aliases = make([]string, 0, len(req.GetAliases()))
	for _, aliasInfo := range req.GetAliases() {
		if aliasInfo == nil {
			return merr.Status(merr.WrapErrParameterInvalidMsg("nil alias in transfer request, collection id: %d", coll.CollectionID)), nil
		}
		alias := model.UnmarshalAliasModel(aliasInfo)
		if alias.CollectionID != coll.CollectionID {
			return merr.Status(merr.WrapErrParameterInvalidMsg("alias collection id mismatch, collection id: %d, alias: %s, alias collection id: %d", coll.CollectionID, alias.Name, alias.CollectionID)), nil
		}
		if alias.Available() {
			coll.Aliases = append(coll.Aliases, alias.Name)
		}
	}

	applier, ok := c.meta.(transferredCollectionApplier)
	if !ok {
		return merr.Status(merr.WrapErrServiceInternalMsg("rootcoord meta does not support transferred collection apply")), nil
	}
	if err := applier.ApplyTransferredCollection(ctx, coll); err != nil {
		return merr.Status(err), nil
	}
	if err := c.ExpireTransferMetaCache(
		ctx,
		coll.DBName,
		coll.Name,
		coll.Aliases,
		coll.CollectionID,
		req.GetCacheExpireTs(),
	); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func (c *Core) CatalogTransferAbort(ctx context.Context, req *rootcoordpb.CatalogTransferAbortRequest) (*commonpb.Status, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return merr.Status(err), nil
	}
	if req == nil {
		return merr.Status(merr.WrapErrParameterMissing("CatalogTransferAbortRequest")), nil
	}
	if err := validateCatalogTransferIdentity(req.GetTransferId(), req.GetTransferEpoch()); err != nil {
		return merr.Status(err), nil
	}

	if err := c.transferGate.Abort(req.GetCollectionId(), req.GetTransferId(), req.GetTransferEpoch()); err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}

func validateCatalogTransferIdentity(transferID string, transferEpoch int64) error {
	if transferID == "" {
		return merr.WrapErrParameterInvalidMsg("transfer id is required")
	}
	if transferEpoch <= 0 {
		return merr.WrapErrParameterInvalidMsg("transfer epoch must be positive")
	}
	return nil
}
