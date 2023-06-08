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
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

//go:generate mockery --name=IMetaTable --outpkg=mockrootcoord --filename=meta_table.go --with-expecter
type IMetaTable interface {
	AddCollection(ctx context.Context, coll *model.Collection) error
	ChangeCollectionState(ctx context.Context, collectionID UniqueID, state pb.CollectionState, ts Timestamp) error
	RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error
	GetCollectionByName(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error)
	GetCollectionByID(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error)
	ListCollections(ctx context.Context, ts Timestamp) ([]*model.Collection, error)
	ListAbnormalCollections(ctx context.Context, ts Timestamp) ([]*model.Collection, error)
	ListCollectionPhysicalChannels() map[typeutil.UniqueID][]string
	GetCollectionVirtualChannels(colID int64) []string
	AddPartition(ctx context.Context, partition *model.Partition) error
	ChangePartitionState(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state pb.PartitionState, ts Timestamp) error
	RemovePartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error
	CreateAlias(ctx context.Context, alias string, collectionName string, ts Timestamp) error
	DropAlias(ctx context.Context, alias string, ts Timestamp) error
	AlterAlias(ctx context.Context, alias string, collectionName string, ts Timestamp) error
	AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp) error
	RenameCollection(ctx context.Context, oldName string, newName string, ts Timestamp) error

	// TODO: it'll be a big cost if we handle the time travel logic, since we should always list all aliases in catalog.
	IsAlias(name string) bool
	ListAliasesByID(collID UniqueID) []string

	// TODO: better to accept ctx.
	GetPartitionNameByID(collID UniqueID, partitionID UniqueID, ts Timestamp) (string, error) // serve for bulk insert.
	GetPartitionByName(collID UniqueID, partitionName string, ts Timestamp) (UniqueID, error) // serve for bulk insert.

	// TODO: better to accept ctx.
	AddCredential(credInfo *internalpb.CredentialInfo) error
	GetCredential(username string) (*internalpb.CredentialInfo, error)
	DeleteCredential(username string) error
	AlterCredential(credInfo *internalpb.CredentialInfo) error
	ListCredentialUsernames() (*milvuspb.ListCredUsersResponse, error)

	// TODO: better to accept ctx.
	CreateRole(tenant string, entity *milvuspb.RoleEntity) error
	DropRole(tenant string, roleName string) error
	OperateUserRole(tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error
	SelectRole(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error)
	SelectUser(tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
	OperatePrivilege(tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error
	SelectGrant(tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error)
	DropGrant(tenant string, role *milvuspb.RoleEntity) error
	ListPolicy(tenant string) ([]string, error)
	ListUserRole(tenant string) ([]string, error)
}

type MetaTable struct {
	ctx     context.Context
	catalog metastore.RootCoordCatalog

	collID2Meta  map[typeutil.UniqueID]*model.Collection // collection id -> collection meta
	collName2ID  map[string]typeutil.UniqueID            // collection name to collection id
	collAlias2ID map[string]typeutil.UniqueID            // collection alias to collection id

	ddLock         sync.RWMutex
	permissionLock sync.RWMutex
}

func NewMetaTable(ctx context.Context, catalog metastore.RootCoordCatalog) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:     contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue()),
		catalog: catalog,
	}
	if err := mt.reload(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reload() error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	record := timerecord.NewTimeRecorder("rootcoord")
	mt.collID2Meta = make(map[UniqueID]*model.Collection)
	mt.collName2ID = make(map[string]UniqueID)
	mt.collAlias2ID = make(map[string]UniqueID)

	collectionNum := int64(0)
	partitionNum := int64(0)

	// max ts means listing latest resources, meta table should always cache the latest version of catalog.
	collections, err := mt.catalog.ListCollections(mt.ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	for name, collection := range collections {
		mt.collID2Meta[collection.CollectionID] = collection
		mt.collName2ID[name] = collection.CollectionID

		if collection.Available() {
			collectionNum++
			partitionNum += int64(collection.GetPartitionNum(true))
		}
	}

	// max ts means listing latest resources, meta table should always cache the latest version of catalog.
	aliases, err := mt.catalog.ListAliases(mt.ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	for _, alias := range aliases {
		mt.collAlias2ID[alias.Name] = alias.CollectionID
	}

	metrics.RootCoordNumOfCollections.Set(float64(collectionNum))
	metrics.RootCoordNumOfPartitions.WithLabelValues().Set(float64(partitionNum))
	log.Info("RootCoord meta table reload done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (mt *MetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	// Note:
	// 1, idempotency check was already done outside;
	// 2, no need to check time travel logic, since ts should always be the latest;

	if coll.State != pb.CollectionState_CollectionCreating {
		return fmt.Errorf("collection state should be creating, collection name: %s, collection id: %d, state: %s", coll.Name, coll.CollectionID, coll.State)
	}
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.CreateCollection(ctx1, coll, coll.CreateTime); err != nil {
		return err
	}
	mt.collName2ID[coll.Name] = coll.CollectionID
	mt.collID2Meta[coll.CollectionID] = coll.Clone()
	log.Ctx(ctx).Info("add collection to meta table", zap.String("collection", coll.Name),
		zap.Int64("id", coll.CollectionID), zap.Uint64("ts", coll.CreateTime))
	return nil
}

func (mt *MetaTable) ChangeCollectionState(ctx context.Context, collectionID UniqueID, state pb.CollectionState, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return nil
	}
	clone := coll.Clone()
	clone.State = state
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterCollection(ctx1, coll, clone, metastore.MODIFY, ts); err != nil {
		return err
	}
	mt.collID2Meta[collectionID] = clone

	switch state {
	case pb.CollectionState_CollectionCreated:
		metrics.RootCoordNumOfCollections.Inc()
		metrics.RootCoordNumOfPartitions.WithLabelValues().Add(float64(coll.GetPartitionNum(true)))
	default:
		metrics.RootCoordNumOfCollections.Dec()
		metrics.RootCoordNumOfPartitions.WithLabelValues().Sub(float64(coll.GetPartitionNum(true)))
	}

	log.Ctx(ctx).Info("change collection state", zap.Int64("collection", collectionID),
		zap.String("state", state.String()), zap.Uint64("ts", ts))

	return nil
}

func (mt *MetaTable) removeIfNameMatchedInternal(collectionID UniqueID, name string) {
	id, ok := mt.collName2ID[name]
	if ok && id == collectionID {
		delete(mt.collName2ID, name)
	}
}

func (mt *MetaTable) removeIfAliasMatchedInternal(collectionID UniqueID, alias string) {
	id, ok := mt.collAlias2ID[alias]
	if ok && id == collectionID {
		delete(mt.collAlias2ID, alias)
	}
}

func (mt *MetaTable) removeIfMatchedInternal(collectionID UniqueID, name string) {
	mt.removeIfNameMatchedInternal(collectionID, name)
	mt.removeIfAliasMatchedInternal(collectionID, name)
}

func (mt *MetaTable) removeAllNamesIfMatchedInternal(collectionID UniqueID, names []string) {
	for _, name := range names {
		mt.removeIfMatchedInternal(collectionID, name)
	}
}

func (mt *MetaTable) removeCollectionByIDInternal(collectionID UniqueID) {
	delete(mt.collID2Meta, collectionID)
}

func (mt *MetaTable) RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	// Note: we cannot handle case that dropping collection with `ts1` but a collection exists in catalog with newer ts
	// which is bigger than `ts1`. So we assume that ts should always be the latest.

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	aliases := mt.listAliasesByID(collectionID)
	if err := mt.catalog.DropCollection(ctx1, &model.Collection{CollectionID: collectionID, Aliases: aliases}, ts); err != nil {
		return err
	}

	allNames := common.CloneStringList(aliases)

	var name string
	coll, ok := mt.collID2Meta[collectionID]
	if ok && coll != nil {
		name = coll.Name
		allNames = append(allNames, name)
	}

	// We cannot delete the name directly, since newly collection with same name may be created.
	mt.removeAllNamesIfMatchedInternal(collectionID, allNames)
	mt.removeCollectionByIDInternal(collectionID)

	log.Ctx(ctx).Info("remove collection", zap.String("name", name), zap.Int64("id", collectionID), zap.Strings("aliases", aliases))
	return nil
}

func filterUnavailable(coll *model.Collection) *model.Collection {
	clone := coll.Clone()
	// pick available partitions.
	clone.Partitions = nil
	for _, partition := range coll.Partitions {
		if partition.Available() {
			clone.Partitions = append(clone.Partitions, partition.Clone())
		}
	}
	return clone
}

// getLatestCollectionByIDInternal should be called with ts = typeutil.MaxTimestamp
func (mt *MetaTable) getLatestCollectionByIDInternal(ctx context.Context, collectionID UniqueID, allowAvailable bool) (*model.Collection, error) {
	coll, ok := mt.collID2Meta[collectionID]
	if !ok || coll == nil {
		return nil, common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %d", collectionID))
	}
	if allowAvailable {
		return coll.Clone(), nil
	}
	if !coll.Available() {
		return nil, common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %d", collectionID))
	}
	return filterUnavailable(coll), nil
}

// getCollectionByIDInternal get collection by collection id without lock.
func (mt *MetaTable) getCollectionByIDInternal(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	if isMaxTs(ts) {
		return mt.getLatestCollectionByIDInternal(ctx, collectionID, allowUnavailable)
	}

	var coll *model.Collection
	var err error

	coll, ok := mt.collID2Meta[collectionID]
	if !ok || coll == nil || !coll.Available() || coll.CreateTime > ts {
		// travel meta information from catalog.
		ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
		coll, err = mt.catalog.GetCollectionByID(ctx1, collectionID, ts)
		if err != nil {
			return nil, err
		}
	}

	if coll == nil {
		// use coll.Name to match error message of regression. TODO: remove this after error code is ready.
		return nil, common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %s", coll.Name))
	}

	if allowUnavailable {
		return coll.Clone(), nil
	}

	if !coll.Available() {
		// use coll.Name to match error message of regression. TODO: remove this after error code is ready.
		return nil, common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %s", coll.Name))
	}

	return filterUnavailable(coll), nil
}

func (mt *MetaTable) GetCollectionByName(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getCollectionByNameInternal(ctx, collectionName, ts)
}

func (mt *MetaTable) getCollectionByNameInternal(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
	collectionID, ok := mt.collAlias2ID[collectionName]
	if ok {
		return mt.getCollectionByIDInternal(ctx, collectionID, ts, false)
	}

	collectionID, ok = mt.collName2ID[collectionName]
	if ok {
		return mt.getCollectionByIDInternal(ctx, collectionID, ts, false)
	}

	if isMaxTs(ts) {
		return nil, common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %s", collectionName))
	}

	// travel meta information from catalog. No need to check time travel logic again, since catalog already did.
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	coll, err := mt.catalog.GetCollectionByName(ctx1, collectionName, ts)
	if err != nil {
		return nil, err
	}

	if coll == nil || !coll.Available() {
		return nil, common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %s", collectionName))
	}
	return filterUnavailable(coll), nil
}

func (mt *MetaTable) GetCollectionByID(ctx context.Context, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.getCollectionByIDInternal(ctx, collectionID, ts, allowUnavailable)
}

func (mt *MetaTable) ListCollections(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if isMaxTs(ts) {
		return mt.listCollectionFromCache()
	}

	// list collections should always be loaded from catalog.
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	colls, err := mt.catalog.ListCollections(ctx1, ts)
	if err != nil {
		return nil, err
	}
	onlineCollections := make([]*model.Collection, 0, len(colls))
	for _, coll := range colls {
		if coll.Available() {
			onlineCollections = append(onlineCollections, coll)
		}
	}
	return onlineCollections, nil
}

func (mt *MetaTable) listCollectionFromCache() ([]*model.Collection, error) {
	collectionFromCache := make([]*model.Collection, 0)
	for _, meta := range mt.collID2Meta {
		if meta.Available() {
			collectionFromCache = append(collectionFromCache, meta)
		}
	}
	return collectionFromCache, nil
}

func (mt *MetaTable) ListAbnormalCollections(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	// list collections should always be loaded from catalog.
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	colls, err := mt.catalog.ListCollections(ctx1, ts)
	if err != nil {
		return nil, err
	}
	abnormalCollections := make([]*model.Collection, 0, len(colls))
	for _, coll := range colls {
		if !coll.Available() {
			abnormalCollections = append(abnormalCollections, coll)
		}
	}
	return abnormalCollections, nil
}

// ListCollectionPhysicalChannels list physical channels of all collections.
func (mt *MetaTable) ListCollectionPhysicalChannels() map[typeutil.UniqueID][]string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	chanMap := make(map[UniqueID][]string)

	for id, collInfo := range mt.collID2Meta {
		chanMap[id] = common.CloneStringList(collInfo.PhysicalChannelNames)
	}

	return chanMap
}

func (mt *MetaTable) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterCollection(ctx1, oldColl, newColl, metastore.MODIFY, ts); err != nil {
		return err
	}
	mt.collID2Meta[oldColl.CollectionID] = newColl
	log.Info("alter collection finished", zap.Int64("collectionID", oldColl.CollectionID), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) RenameCollection(ctx context.Context, oldName string, newName string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx = contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	log := log.Ctx(ctx).With(zap.String("oldName", oldName), zap.String("newName", newName))

	//old collection should not be an alias
	_, ok := mt.collAlias2ID[oldName]
	if ok {
		log.Warn("unsupported use a alias to rename collection")
		return fmt.Errorf("unsupported use an alias to rename collection, alias:%s", oldName)
	}

	// check new collection already exists
	newColl, err := mt.getCollectionByNameInternal(ctx, newName, ts)
	if newColl != nil {
		return fmt.Errorf("duplicated new collection name :%s with other collection name or alias", newName)
	}
	if err != nil && !common.IsCollectionNotExistErrorV2(err) {
		log.Warn("check new collection name fail")
		return err
	}

	// get old collection meta
	oldColl, err := mt.getCollectionByNameInternal(ctx, oldName, ts)
	if err != nil {
		return err
	}

	newColl = oldColl.Clone()
	newColl.Name = newName
	if err := mt.catalog.AlterCollection(ctx, oldColl, newColl, metastore.MODIFY, ts); err != nil {
		return err
	}

	mt.collName2ID[newName] = oldColl.CollectionID
	delete(mt.collName2ID, oldName)
	mt.collID2Meta[oldColl.CollectionID] = newColl

	log.Info("rename collection finished")
	return nil
}

// GetCollectionVirtualChannels returns virtual channels of a given collection.
func (mt *MetaTable) GetCollectionVirtualChannels(colID int64) []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	for id, collInfo := range mt.collID2Meta {
		if id == colID {
			return common.CloneStringList(collInfo.VirtualChannelNames)
		}
	}
	return nil
}

func (mt *MetaTable) AddPartition(ctx context.Context, partition *model.Partition) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll, ok := mt.collID2Meta[partition.CollectionID]
	if !ok || !coll.Available() {
		return fmt.Errorf("collection not exists: %d", partition.CollectionID)
	}
	if partition.State != pb.PartitionState_PartitionCreating {
		return fmt.Errorf("partition state is not created, collection: %d, partition: %d, state: %s", partition.CollectionID, partition.PartitionID, partition.State)
	}
	if err := mt.catalog.CreatePartition(ctx, partition, partition.PartitionCreatedTimestamp); err != nil {
		return err
	}
	mt.collID2Meta[partition.CollectionID].Partitions = append(mt.collID2Meta[partition.CollectionID].Partitions, partition.Clone())

	metrics.RootCoordNumOfPartitions.WithLabelValues().Inc()

	log.Ctx(ctx).Info("add partition to meta table",
		zap.Int64("collection", partition.CollectionID), zap.String("partition", partition.PartitionName),
		zap.Int64("partitionid", partition.PartitionID), zap.Uint64("ts", partition.PartitionCreatedTimestamp))

	return nil
}

func (mt *MetaTable) ChangePartitionState(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state pb.PartitionState, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return nil
	}
	for idx, part := range coll.Partitions {
		if part.PartitionID == partitionID {
			clone := part.Clone()
			clone.State = state
			ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
			if err := mt.catalog.AlterPartition(ctx1, part, clone, metastore.MODIFY, ts); err != nil {
				return err
			}
			mt.collID2Meta[collectionID].Partitions[idx] = clone

			switch state {
			case pb.PartitionState_PartitionCreated:
				// support Dynamic load/release partitions
				metrics.RootCoordNumOfPartitions.WithLabelValues().Inc()
			default:
				metrics.RootCoordNumOfPartitions.WithLabelValues().Dec()
			}

			log.Ctx(ctx).Info("change partition state", zap.Int64("collection", collectionID),
				zap.Int64("partition", partitionID), zap.String("state", state.String()),
				zap.Uint64("ts", ts))

			return nil
		}
	}
	return fmt.Errorf("partition not exist, collection: %d, partition: %d", collectionID, partitionID)
}

func (mt *MetaTable) RemovePartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.DropPartition(ctx1, collectionID, partitionID, ts); err != nil {
		return err
	}
	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return nil
	}
	var loc = -1
	for idx, part := range coll.Partitions {
		if part.PartitionID == partitionID {
			loc = idx
			break
		}
	}
	if loc != -1 {
		coll.Partitions = append(coll.Partitions[:loc], coll.Partitions[loc+1:]...)
	}
	log.Info("remove partition", zap.Int64("collection", collectionID), zap.Int64("partition", partitionID), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) CreateAlias(ctx context.Context, alias string, collectionName string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	// It's ok that we don't read from catalog when cache missed.
	// Since cache always keep the latest version, and the ts should always be the latest.

	if _, ok := mt.collName2ID[alias]; ok {
		return fmt.Errorf("cannot create alias, collection already exists with same name: %s", alias)
	}

	collectionID, ok := mt.collName2ID[collectionName]
	if !ok {
		// you cannot alias to a non-existent collection.
		return fmt.Errorf("collection not exists: %s", collectionName)
	}

	// check if alias exists.
	aliasedCollectionID, ok := mt.collAlias2ID[alias]
	if ok && aliasedCollectionID == collectionID {
		log.Warn("add duplicate alias", zap.String("alias", alias), zap.String("collection", collectionName), zap.Uint64("ts", ts))
		return nil
	} else if ok {
		// TODO: better to check if aliasedCollectionID exist or is available, though not very possible.
		aliasedColl := mt.collID2Meta[aliasedCollectionID]
		return fmt.Errorf("alias exists and already aliased to another collection, alias: %s, collection: %s, other collection: %s", alias, collectionName, aliasedColl.Name)
	}
	// alias didn't exist.

	coll, ok := mt.collID2Meta[collectionID]
	if !ok || !coll.Available() {
		// you cannot alias to a non-existent collection.
		return fmt.Errorf("collection not exists: %s", collectionName)
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.CreateAlias(ctx1, &model.Alias{
		Name:         alias,
		CollectionID: collectionID,
		CreatedTime:  ts,
		State:        pb.AliasState_AliasCreated,
	}, ts); err != nil {
		return err
	}
	mt.collAlias2ID[alias] = collectionID
	log.Info("create alias", zap.String("alias", alias), zap.String("collection", collectionName), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) DropAlias(ctx context.Context, alias string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.DropAlias(ctx1, alias, ts); err != nil {
		return err
	}
	delete(mt.collAlias2ID, alias)
	log.Info("drop alias", zap.String("alias", alias), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) AlterAlias(ctx context.Context, alias string, collectionName string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	// It's ok that we don't read from catalog when cache missed.
	// Since cache always keep the latest version, and the ts should always be the latest.

	if _, ok := mt.collName2ID[alias]; ok {
		return fmt.Errorf("cannot alter alias, collection already exists with same name: %s", alias)
	}

	collectionID, ok := mt.collName2ID[collectionName]
	if !ok {
		// you cannot alias to a non-existent collection.
		return fmt.Errorf("collection not exists: %s", collectionName)
	}

	coll, ok := mt.collID2Meta[collectionID]
	if !ok || !coll.Available() {
		// you cannot alias to a non-existent collection.
		return fmt.Errorf("collection not exists: %s", collectionName)
	}

	// check if alias exists.
	_, ok = mt.collAlias2ID[alias]
	if !ok {
		//
		return fmt.Errorf("failed to alter alias, alias does not exist: %s", alias)
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterAlias(ctx1, &model.Alias{
		Name:         alias,
		CollectionID: collectionID,
		CreatedTime:  ts,
		State:        pb.AliasState_AliasCreated,
	}, ts); err != nil {
		return err
	}

	// alias switch to another collection anyway.
	mt.collAlias2ID[alias] = collectionID
	log.Info("alter alias", zap.String("alias", alias), zap.String("collection", collectionName), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) IsAlias(name string) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	_, ok := mt.collAlias2ID[name]
	return ok
}

func (mt *MetaTable) listAliasesByID(collID UniqueID) []string {
	ret := make([]string, 0)
	for alias, id := range mt.collAlias2ID {
		if id == collID {
			ret = append(ret, alias)
		}
	}
	return ret
}

func (mt *MetaTable) ListAliasesByID(collID UniqueID) []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.listAliasesByID(collID)
}

// GetCollectionNameByID serve for bulk insert. TODO: why this didn't accept ts?
// [Deprecated]
func (mt *MetaTable) GetCollectionNameByID(collID UniqueID) (string, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	coll, ok := mt.collID2Meta[collID]
	if !ok || !coll.Available() {
		return "", fmt.Errorf("collection not exist: %d", collID)
	}

	return coll.Name, nil
}

// GetPartitionNameByID serve for bulk insert.
func (mt *MetaTable) GetPartitionNameByID(collID UniqueID, partitionID UniqueID, ts Timestamp) (string, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	coll, ok := mt.collID2Meta[collID]
	if ok && coll.Available() && coll.CreateTime <= ts {
		// cache hit.
		for _, partition := range coll.Partitions {
			if partition.Available() && partition.PartitionID == partitionID && partition.PartitionCreatedTimestamp <= ts {
				// cache hit.
				return partition.PartitionName, nil
			}
		}
	}
	// cache miss, get from catalog anyway.
	coll, err := mt.catalog.GetCollectionByID(mt.ctx, collID, ts)
	if err != nil {
		return "", err
	}
	if !coll.Available() {
		return "", fmt.Errorf("collection not exist: %d", collID)
	}
	for _, partition := range coll.Partitions {
		// no need to check time travel logic again, since catalog already did.
		if partition.Available() && partition.PartitionID == partitionID {
			return partition.PartitionName, nil
		}
	}
	return "", fmt.Errorf("partition not exist: %d", partitionID)
}

// GetCollectionIDByName serve for bulk insert. TODO: why this didn't accept ts?
// [Deprecated]
func (mt *MetaTable) GetCollectionIDByName(name string) (UniqueID, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	id, ok := mt.collName2ID[name]
	if !ok {
		return InvalidCollectionID, fmt.Errorf("collection not exists: %s", name)
	}
	return id, nil
}

// GetPartitionByName serve for bulk insert.
func (mt *MetaTable) GetPartitionByName(collID UniqueID, partitionName string, ts Timestamp) (UniqueID, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	coll, ok := mt.collID2Meta[collID]
	if ok && coll.Available() && coll.CreateTime <= ts {
		// cache hit.
		for _, partition := range coll.Partitions {
			if partition.Available() && partition.PartitionName == partitionName && partition.PartitionCreatedTimestamp <= ts {
				// cache hit.
				return partition.PartitionID, nil
			}
		}
	}
	// cache miss, get from catalog anyway.
	coll, err := mt.catalog.GetCollectionByID(mt.ctx, collID, ts)
	if err != nil {
		return common.InvalidPartitionID, err
	}
	if !coll.Available() {
		return common.InvalidPartitionID, fmt.Errorf("collection not exist: %d", collID)
	}
	for _, partition := range coll.Partitions {
		// no need to check time travel logic again, since catalog already did.
		if partition.Available() && partition.PartitionName == partitionName {
			return partition.PartitionID, nil
		}
	}

	log.Error("partition ID not found for partition name", zap.String("partitionName", partitionName),
		zap.Int64("collectionID", collID), zap.String("collectionName", coll.Name))
	return common.InvalidPartitionID, fmt.Errorf("partition ID not found for partition name '%s' in collection '%s'",
		partitionName, coll.Name)
}

// AddCredential add credential
func (mt *MetaTable) AddCredential(credInfo *internalpb.CredentialInfo) error {
	if credInfo.Username == "" {
		return fmt.Errorf("username is empty")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	usernames, err := mt.catalog.ListCredentials(mt.ctx)
	if err != nil {
		return err
	}
	if len(usernames) >= Params.ProxyCfg.MaxUserNum.GetAsInt() {
		errMsg := "unable to add user because the number of users has reached the limit"
		log.Error(errMsg, zap.Int("max_user_num", Params.ProxyCfg.MaxUserNum.GetAsInt()))
		return errors.New(errMsg)
	}

	if origin, _ := mt.catalog.GetCredential(mt.ctx, credInfo.Username); origin != nil {
		return fmt.Errorf("user already exists: %s", credInfo.Username)
	}

	credential := &model.Credential{
		Username:          credInfo.Username,
		EncryptedPassword: credInfo.EncryptedPassword,
	}
	return mt.catalog.CreateCredential(mt.ctx, credential)
}

// AlterCredential update credential
func (mt *MetaTable) AlterCredential(credInfo *internalpb.CredentialInfo) error {
	if credInfo.Username == "" {
		return fmt.Errorf("username is empty")
	}

	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	credential := &model.Credential{
		Username:          credInfo.Username,
		EncryptedPassword: credInfo.EncryptedPassword,
	}
	return mt.catalog.AlterCredential(mt.ctx, credential)
}

// GetCredential get credential by username
func (mt *MetaTable) GetCredential(username string) (*internalpb.CredentialInfo, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	credential, err := mt.catalog.GetCredential(mt.ctx, username)
	return model.MarshalCredentialModel(credential), err
}

// DeleteCredential delete credential
func (mt *MetaTable) DeleteCredential(username string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DropCredential(mt.ctx, username)
}

// ListCredentialUsernames list credential usernames
func (mt *MetaTable) ListCredentialUsernames() (*milvuspb.ListCredUsersResponse, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	usernames, err := mt.catalog.ListCredentials(mt.ctx)
	if err != nil {
		return nil, fmt.Errorf("list credential usernames err:%w", err)
	}
	return &milvuspb.ListCredUsersResponse{Usernames: usernames}, nil
}

// CreateRole create role
func (mt *MetaTable) CreateRole(tenant string, entity *milvuspb.RoleEntity) error {
	if funcutil.IsEmptyString(entity.Name) {
		return fmt.Errorf("the role name in the role info is empty")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	results, err := mt.catalog.ListRole(mt.ctx, tenant, nil, false)
	if err != nil {
		logger.Error("fail to list roles", zap.Error(err))
		return err
	}
	if len(results) >= Params.ProxyCfg.MaxRoleNum.GetAsInt() {
		errMsg := "unable to create role because the number of roles has reached the limit"
		log.Error(errMsg, zap.Int("max_role_num", Params.ProxyCfg.MaxRoleNum.GetAsInt()))
		return errors.New(errMsg)
	}

	return mt.catalog.CreateRole(mt.ctx, tenant, entity)
}

// DropRole drop role info
func (mt *MetaTable) DropRole(tenant string, roleName string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DropRole(mt.ctx, tenant, roleName)
}

// OperateUserRole operate the relationship between a user and a role, including adding a user to a role and removing a user from a role
func (mt *MetaTable) OperateUserRole(tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	if funcutil.IsEmptyString(userEntity.Name) {
		return fmt.Errorf("username in the user entity is empty")
	}
	if funcutil.IsEmptyString(roleEntity.Name) {
		return fmt.Errorf("role name in the role entity is empty")
	}

	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.AlterUserRole(mt.ctx, tenant, userEntity, roleEntity, operateType)
}

// SelectRole select role.
// Enter the role condition by the entity param. And this param is nil, which means selecting all roles.
// Get all users that are added to the role by setting the includeUserInfo param to true.
func (mt *MetaTable) SelectRole(tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListRole(mt.ctx, tenant, entity, includeUserInfo)
}

// SelectUser select user.
// Enter the user condition by the entity param. And this param is nil, which means selecting all users.
// Get all roles that are added the user to by setting the includeRoleInfo param to true.
func (mt *MetaTable) SelectUser(tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListUser(mt.ctx, tenant, entity, includeRoleInfo)
}

// OperatePrivilege grant or revoke privilege by setting the operateType param
func (mt *MetaTable) OperatePrivilege(tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	if funcutil.IsEmptyString(entity.ObjectName) {
		return fmt.Errorf("the object name in the grant entity is empty")
	}
	if entity.Object == nil || funcutil.IsEmptyString(entity.Object.Name) {
		return fmt.Errorf("the object entity in the grant entity is invalid")
	}
	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return fmt.Errorf("the role entity in the grant entity is invalid")
	}
	if entity.Grantor == nil {
		return fmt.Errorf("the grantor in the grant entity is empty")
	}
	if entity.Grantor.Privilege == nil || funcutil.IsEmptyString(entity.Grantor.Privilege.Name) {
		return fmt.Errorf("the privilege name in the grant entity is empty")
	}
	if entity.Grantor.User == nil || funcutil.IsEmptyString(entity.Grantor.User.Name) {
		return fmt.Errorf("the grantor name in the grant entity is empty")
	}
	if !funcutil.IsRevoke(operateType) && !funcutil.IsGrant(operateType) {
		return fmt.Errorf("the operate type in the grant entity is invalid")
	}

	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.AlterGrant(mt.ctx, tenant, entity, operateType)
}

// SelectGrant select grant
// The principal entity MUST be not empty in the grant entity
// The resource entity and the resource name are optional, and the two params should be not empty together when you select some grants about the resource kind.
func (mt *MetaTable) SelectGrant(tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var entities []*milvuspb.GrantEntity
	if entity == nil {
		return entities, fmt.Errorf("the grant entity is nil")
	}

	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return entities, fmt.Errorf("the role entity in the grant entity is invalid")
	}

	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListGrant(mt.ctx, tenant, entity)
}

func (mt *MetaTable) DropGrant(tenant string, role *milvuspb.RoleEntity) error {
	if role == nil || funcutil.IsEmptyString(role.Name) {
		return fmt.Errorf("the role entity is invalid when dropping the grant")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DeleteGrant(mt.ctx, tenant, role)
}

func (mt *MetaTable) ListPolicy(tenant string) ([]string, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListPolicy(mt.ctx, tenant)
}

func (mt *MetaTable) ListUserRole(tenant string) ([]string, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListUserRole(mt.ctx, tenant)
}
