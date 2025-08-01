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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

//go:generate mockery --name=IMetaTable --structname=MockIMetaTable --output=./  --filename=mock_meta_table.go --with-expecter --inpackage
type IMetaTable interface {
	GetDatabaseByID(ctx context.Context, dbID int64, ts Timestamp) (*model.Database, error)
	GetDatabaseByName(ctx context.Context, dbName string, ts Timestamp) (*model.Database, error)
	CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error
	DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error
	ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error)
	AlterDatabase(ctx context.Context, oldDB *model.Database, newDB *model.Database, ts typeutil.Timestamp) error

	AddCollection(ctx context.Context, coll *model.Collection) error
	ChangeCollectionState(ctx context.Context, collectionID UniqueID, state pb.CollectionState, ts Timestamp) error
	RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error
	// GetCollectionID retrieves the corresponding collectionID based on the collectionName.
	// If the collection does not exist, it will return InvalidCollectionID.
	// Please use the function with caution.
	GetCollectionID(ctx context.Context, dbName string, collectionName string) UniqueID
	GetCollectionByName(ctx context.Context, dbName string, collectionName string, ts Timestamp) (*model.Collection, error)
	GetCollectionByID(ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error)
	GetCollectionByIDWithMaxTs(ctx context.Context, collectionID UniqueID) (*model.Collection, error)
	ListCollections(ctx context.Context, dbName string, ts Timestamp, onlyAvail bool) ([]*model.Collection, error)
	ListAllAvailCollections(ctx context.Context) map[int64][]int64
	// ListAllAvailPartitions returns the partition ids of all available collections.
	// The key of the map is the database id, and the value is a map of collection id to partition ids.
	ListAllAvailPartitions(ctx context.Context) map[int64]map[int64][]int64
	ListCollectionPhysicalChannels(ctx context.Context) map[typeutil.UniqueID][]string
	GetCollectionVirtualChannels(ctx context.Context, colID int64) []string
	GetPChannelInfo(ctx context.Context, pchannel string) *rootcoordpb.GetPChannelInfoResponse
	AddPartition(ctx context.Context, partition *model.Partition) error
	ChangePartitionState(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state pb.PartitionState, ts Timestamp) error
	RemovePartition(ctx context.Context, dbID int64, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error
	CreateAlias(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error
	DropAlias(ctx context.Context, dbName string, alias string, ts Timestamp) error
	AlterAlias(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error
	DescribeAlias(ctx context.Context, dbName string, alias string, ts Timestamp) (string, error)
	ListAliases(ctx context.Context, dbName string, collectionName string, ts Timestamp) ([]string, error)
	AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp, fieldModify bool) error
	RenameCollection(ctx context.Context, dbName string, oldName string, newDBName string, newName string, ts Timestamp) error
	GetGeneralCount(ctx context.Context) int

	// TODO: it'll be a big cost if we handle the time travel logic, since we should always list all aliases in catalog.
	IsAlias(ctx context.Context, db, name string) bool
	ListAliasesByID(ctx context.Context, collID UniqueID) []string

	AddCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error
	GetCredential(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	DeleteCredential(ctx context.Context, username string) error
	AlterCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error
	ListCredentialUsernames(ctx context.Context) (*milvuspb.ListCredUsersResponse, error)

	CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
	DropRole(ctx context.Context, tenant string, roleName string) error
	OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error
	SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error)
	SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
	OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error
	SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error)
	DropGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error
	ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error)
	ListUserRole(ctx context.Context, tenant string) ([]string, error)
	BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error)
	RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error
	IsCustomPrivilegeGroup(ctx context.Context, groupName string) (bool, error)
	CreatePrivilegeGroup(ctx context.Context, groupName string) error
	DropPrivilegeGroup(ctx context.Context, groupName string) error
	ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error)
	OperatePrivilegeGroup(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error
	GetPrivilegeGroupRoles(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error)
}

// MetaTable is a persistent meta set of all databases, collections and partitions.
type MetaTable struct {
	ctx     context.Context
	catalog metastore.RootCoordCatalog

	tsoAllocator tso.Allocator

	dbName2Meta map[string]*model.Database              // database name ->  db meta
	collID2Meta map[typeutil.UniqueID]*model.Collection // collection id -> collection meta

	generalCnt int // sum of product of partition number and shard number

	// collections *collectionDb
	names   *nameDb
	aliases *nameDb

	ddLock         sync.RWMutex
	permissionLock sync.RWMutex
}

// NewMetaTable creates a new MetaTable with specified catalog and allocator.
func NewMetaTable(ctx context.Context, catalog metastore.RootCoordCatalog, tsoAllocator tso.Allocator) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:          contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue()),
		catalog:      catalog,
		tsoAllocator: tsoAllocator,
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
	mt.dbName2Meta = make(map[string]*model.Database)
	mt.collID2Meta = make(map[UniqueID]*model.Collection)
	mt.names = newNameDb()
	mt.aliases = newNameDb()

	metrics.RootCoordNumOfCollections.Reset()
	metrics.RootCoordNumOfPartitions.Reset()
	metrics.RootCoordNumOfDatabases.Set(0)

	// recover databases.
	dbs, err := mt.catalog.ListDatabases(mt.ctx, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	log.Ctx(mt.ctx).Info("recover databases", zap.Int("num of dbs", len(dbs)))
	for _, db := range dbs {
		mt.dbName2Meta[db.Name] = db
	}
	dbNames := maps.Keys(mt.dbName2Meta)
	// create default database.
	if !funcutil.SliceContain(dbNames, util.DefaultDBName) {
		if err := mt.createDefaultDb(); err != nil {
			return err
		}
	} else {
		mt.names.createDbIfNotExist(util.DefaultDBName)
		mt.aliases.createDbIfNotExist(util.DefaultDBName)
	}

	// in order to support backward compatibility with meta of the old version, it also
	// needs to reload collections that have no database
	if err := mt.reloadWithNonDatabase(); err != nil {
		return err
	}

	// recover collections from db namespace
	for dbName, db := range mt.dbName2Meta {
		partitionNum := int64(0)
		collectionNum := int64(0)

		mt.names.createDbIfNotExist(dbName)

		start := time.Now()
		// TODO: async list collections to accelerate cases with multiple databases.
		collections, err := mt.catalog.ListCollections(mt.ctx, db.ID, typeutil.MaxTimestamp)
		if err != nil {
			return err
		}
		for _, collection := range collections {
			if collection.DBName == "" {
				collection.DBName = dbName
			}
			mt.collID2Meta[collection.CollectionID] = collection
			if collection.Available() {
				mt.names.insert(dbName, collection.Name, collection.CollectionID)
				pn := collection.GetPartitionNum(true)
				mt.generalCnt += pn * int(collection.ShardsNum)
				collectionNum++
				partitionNum += int64(pn)
			}
		}

		metrics.RootCoordNumOfDatabases.Inc()
		metrics.RootCoordNumOfCollections.WithLabelValues(dbName).Add(float64(collectionNum))
		metrics.RootCoordNumOfPartitions.WithLabelValues().Add(float64(partitionNum))
		log.Ctx(mt.ctx).Info("collections recovered from db", zap.String("db_name", dbName),
			zap.Int64("collection_num", collectionNum),
			zap.Int64("partition_num", partitionNum),
			zap.Duration("dur", time.Since(start)))
	}

	// recover aliases from db namespace
	for dbName, db := range mt.dbName2Meta {
		mt.aliases.createDbIfNotExist(dbName)
		aliases, err := mt.catalog.ListAliases(mt.ctx, db.ID, typeutil.MaxTimestamp)
		if err != nil {
			return err
		}
		for _, alias := range aliases {
			mt.aliases.insert(dbName, alias.Name, alias.CollectionID)
		}
	}

	log.Ctx(mt.ctx).Info("rootcoord start to recover the channel stats for streaming coord balancer")
	vchannels := make([]string, 0, len(mt.collID2Meta)*2)
	for _, coll := range mt.collID2Meta {
		if coll.Available() {
			vchannels = append(vchannels, coll.VirtualChannelNames...)
		}
	}
	channel.RecoverPChannelStatsManager(vchannels)

	log.Ctx(mt.ctx).Info("RootCoord meta table reload done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

// insert into default database if the collections doesn't inside some database
func (mt *MetaTable) reloadWithNonDatabase() error {
	collectionNum := int64(0)
	partitionNum := int64(0)
	oldCollections, err := mt.catalog.ListCollections(mt.ctx, util.NonDBID, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	for _, collection := range oldCollections {
		mt.collID2Meta[collection.CollectionID] = collection
		if collection.Available() {
			mt.names.insert(util.DefaultDBName, collection.Name, collection.CollectionID)
			pn := collection.GetPartitionNum(true)
			mt.generalCnt += pn * int(collection.ShardsNum)
			collectionNum++
			partitionNum += int64(pn)
		}
	}

	if collectionNum > 0 {
		log.Ctx(mt.ctx).Info("recover collections without db", zap.Int64("collection_num", collectionNum), zap.Int64("partition_num", partitionNum))
	}

	aliases, err := mt.catalog.ListAliases(mt.ctx, util.NonDBID, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	for _, alias := range aliases {
		mt.aliases.insert(util.DefaultDBName, alias.Name, alias.CollectionID)
	}

	metrics.RootCoordNumOfCollections.WithLabelValues(util.DefaultDBName).Add(float64(collectionNum))
	metrics.RootCoordNumOfPartitions.WithLabelValues().Add(float64(partitionNum))
	return nil
}

func (mt *MetaTable) createDefaultDb() error {
	ts, err := mt.tsoAllocator.GenerateTSO(1)
	if err != nil {
		return err
	}

	s := Params.RootCoordCfg.DefaultDBProperties.GetValue()
	defaultProperties, err := funcutil.String2KeyValuePair(s)
	if err != nil {
		return err
	}

	return mt.createDatabasePrivate(mt.ctx, model.NewDefaultDatabase(defaultProperties), ts)
}

func (mt *MetaTable) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.createDatabasePrivate(ctx, db, ts); err != nil {
		return err
	}
	metrics.RootCoordNumOfDatabases.Inc()
	return nil
}

func (mt *MetaTable) createDatabasePrivate(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	dbName := db.Name
	if mt.names.exist(dbName) || mt.aliases.exist(dbName) {
		return fmt.Errorf("database already exist: %s", dbName)
	}

	if err := mt.catalog.CreateDatabase(ctx, db, ts); err != nil {
		return err
	}

	mt.names.createDbIfNotExist(dbName)
	mt.aliases.createDbIfNotExist(dbName)
	mt.dbName2Meta[dbName] = db

	log.Ctx(ctx).Info("create database", zap.String("db", dbName), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) AlterDatabase(ctx context.Context, oldDB *model.Database, newDB *model.Database, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if oldDB.Name != newDB.Name || oldDB.ID != newDB.ID || oldDB.State != newDB.State {
		return errors.New("alter database name/id is not supported!")
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterDatabase(ctx1, newDB, ts); err != nil {
		return err
	}
	mt.dbName2Meta[oldDB.Name] = newDB
	log.Ctx(ctx).Info("alter database finished", zap.String("dbName", oldDB.Name), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if dbName == util.DefaultDBName {
		return errors.New("can not drop default database")
	}

	db, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp)
	if err != nil {
		log.Ctx(ctx).Warn("not found database", zap.String("db", dbName))
		return nil
	}

	colls, err := mt.listCollectionFromCache(ctx, dbName, true)
	if err != nil {
		return err
	}
	if len(colls) > 0 {
		return fmt.Errorf("database:%s not empty, must drop all collections before drop database", dbName)
	}

	if err := mt.catalog.DropDatabase(ctx, db.ID, ts); err != nil {
		return err
	}

	mt.names.dropDb(dbName)
	mt.aliases.dropDb(dbName)
	delete(mt.dbName2Meta, dbName)

	metrics.RootCoordNumOfDatabases.Dec()
	log.Ctx(ctx).Info("drop database", zap.String("db", dbName), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return maps.Values(mt.dbName2Meta), nil
}

func (mt *MetaTable) GetDatabaseByID(ctx context.Context, dbID int64, ts Timestamp) (*model.Database, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getDatabaseByIDInternal(ctx, dbID, ts)
}

func (mt *MetaTable) getDatabaseByIDInternal(ctx context.Context, dbID int64, ts Timestamp) (*model.Database, error) {
	for _, db := range maps.Values(mt.dbName2Meta) {
		if db.ID == dbID {
			return db, nil
		}
	}
	return nil, fmt.Errorf("database dbID:%d not found", dbID)
}

func (mt *MetaTable) GetDatabaseByName(ctx context.Context, dbName string, ts Timestamp) (*model.Database, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getDatabaseByNameInternal(ctx, dbName, ts)
}

func (mt *MetaTable) getDatabaseByNameInternal(ctx context.Context, dbName string, _ Timestamp) (*model.Database, error) {
	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty")
		dbName = util.DefaultDBName
	}

	db, ok := mt.dbName2Meta[dbName]
	if !ok {
		return nil, merr.WrapErrDatabaseNotFound(dbName)
	}

	return db, nil
}

func (mt *MetaTable) AddCollection(ctx context.Context, coll *model.Collection) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	// Note:
	// 1, idempotency check was already done outside;
	// 2, no need to check time travel logic, since ts should always be the latest;

	db, err := mt.getDatabaseByIDInternal(ctx, coll.DBID, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	if coll.State != pb.CollectionState_CollectionCreating {
		return fmt.Errorf("collection state should be creating, collection name: %s, collection id: %d, state: %s", coll.Name, coll.CollectionID, coll.State)
	}
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.CreateCollection(ctx1, coll, coll.CreateTime); err != nil {
		return err
	}

	mt.collID2Meta[coll.CollectionID] = coll.Clone()
	mt.names.insert(db.Name, coll.Name, coll.CollectionID)

	channel.StaticPChannelStatsManager.MustGet().AddVChannel(coll.VirtualChannelNames...)
	log.Ctx(ctx).Info("add collection to meta table",
		zap.Int64("dbID", coll.DBID),
		zap.String("collection", coll.Name),
		zap.Int64("id", coll.CollectionID),
		zap.Uint64("ts", coll.CreateTime),
	)
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
	if err := mt.catalog.AlterCollection(ctx1, coll, clone, metastore.MODIFY, ts, false); err != nil {
		return err
	}
	mt.collID2Meta[collectionID] = clone

	db, err := mt.getDatabaseByIDInternal(ctx, coll.DBID, typeutil.MaxTimestamp)
	if err != nil {
		return fmt.Errorf("dbID not found for collection:%d", collectionID)
	}

	pn := coll.GetPartitionNum(true)

	switch state {
	case pb.CollectionState_CollectionCreated:
		mt.generalCnt += pn * int(coll.ShardsNum)
		metrics.RootCoordNumOfCollections.WithLabelValues(db.Name).Inc()
		metrics.RootCoordNumOfPartitions.WithLabelValues().Add(float64(pn))
	case pb.CollectionState_CollectionDropping:
		mt.generalCnt -= pn * int(coll.ShardsNum)
		channel.StaticPChannelStatsManager.MustGet().RemoveVChannel(coll.VirtualChannelNames...)
		metrics.RootCoordNumOfCollections.WithLabelValues(db.Name).Dec()
		metrics.RootCoordNumOfPartitions.WithLabelValues().Sub(float64(pn))
	}

	log.Ctx(ctx).Info("change collection state", zap.Int64("collection", collectionID),
		zap.String("state", state.String()), zap.Uint64("ts", ts))

	return nil
}

func (mt *MetaTable) removeIfNameMatchedInternal(collectionID UniqueID, name string) {
	mt.names.removeIf(func(db string, collection string, id UniqueID) bool {
		return collectionID == id
	})
}

func (mt *MetaTable) removeIfAliasMatchedInternal(collectionID UniqueID, alias string) {
	mt.aliases.removeIf(func(db string, collection string, id UniqueID) bool {
		return collectionID == id
	})
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
	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		log.Ctx(ctx).Warn("not found collection, skip remove", zap.Int64("collectionID", collectionID))
		return nil
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	aliases := mt.listAliasesByID(collectionID)
	newColl := &model.Collection{
		CollectionID:      collectionID,
		Partitions:        model.ClonePartitions(coll.Partitions),
		Fields:            model.CloneFields(coll.Fields),
		StructArrayFields: model.CloneStructArrayFields(coll.StructArrayFields),
		Aliases:           aliases,
		DBID:              coll.DBID,
	}
	if err := mt.catalog.DropCollection(ctx1, newColl, ts); err != nil {
		return err
	}

	allNames := common.CloneStringList(aliases)
	allNames = append(allNames, coll.Name)

	// We cannot delete the name directly, since newly collection with same name may be created.
	mt.removeAllNamesIfMatchedInternal(collectionID, allNames)
	mt.removeCollectionByIDInternal(collectionID)

	log.Ctx(ctx).Info("remove collection",
		zap.Int64("dbID", coll.DBID),
		zap.String("name", coll.Name),
		zap.Int64("id", collectionID),
		zap.Strings("aliases", aliases),
	)
	return nil
}

// Note: The returned model.Collection is read-only. Do NOT modify it directly,
// as it may cause unexpected behavior or inconsistencies.
func filterUnavailable(coll *model.Collection) *model.Collection {
	clone := coll.ShallowClone()
	// pick available partitions.
	clone.Partitions = make([]*model.Partition, 0, len(coll.Partitions))
	for _, partition := range coll.Partitions {
		if partition.Available() {
			clone.Partitions = append(clone.Partitions, partition)
		}
	}
	return clone
}

// getLatestCollectionByIDInternal should be called with ts = typeutil.MaxTimestamp
// Note: The returned model.Collection is read-only. Do NOT modify it directly,
// as it may cause unexpected behavior or inconsistencies.
func (mt *MetaTable) getLatestCollectionByIDInternal(ctx context.Context, collectionID UniqueID, allowUnavailable bool) (*model.Collection, error) {
	coll, ok := mt.collID2Meta[collectionID]
	if !ok || coll == nil {
		log.Warn("not found collection", zap.Int64("collectionID", collectionID))
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	if allowUnavailable {
		return coll.Clone(), nil
	}
	if !coll.Available() {
		log.Warn("collection not available", zap.Int64("collectionID", collectionID), zap.Any("state", coll.State))
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	return filterUnavailable(coll), nil
}

// getCollectionByIDInternal get collection by collection id without lock.
// Note: The returned model.Collection is read-only. Do NOT modify it directly,
// as it may cause unexpected behavior or inconsistencies.
func (mt *MetaTable) getCollectionByIDInternal(ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	if isMaxTs(ts) {
		return mt.getLatestCollectionByIDInternal(ctx, collectionID, allowUnavailable)
	}

	var coll *model.Collection
	coll, ok := mt.collID2Meta[collectionID]
	if !ok || coll == nil || !coll.Available() || coll.CreateTime > ts {
		// travel meta information from catalog.
		ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
		db, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp)
		if err != nil {
			return nil, err
		}
		coll, err = mt.catalog.GetCollectionByID(ctx1, db.ID, ts, collectionID)
		if err != nil {
			return nil, err
		}
	}

	if coll == nil {
		// use coll.Name to match error message of regression. TODO: remove this after error code is ready.
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	if allowUnavailable {
		return coll.Clone(), nil
	}

	if !coll.Available() {
		// use coll.Name to match error message of regression. TODO: remove this after error code is ready.
		return nil, merr.WrapErrCollectionNotFound(dbName, coll.Name)
	}

	return filterUnavailable(coll), nil
}

func (mt *MetaTable) GetCollectionByName(ctx context.Context, dbName string, collectionName string, ts Timestamp) (*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getCollectionByNameInternal(ctx, dbName, collectionName, ts)
}

// GetCollectionID retrieves the corresponding collectionID based on the collectionName.
// If the collection does not exist, it will return InvalidCollectionID.
// Please use the function with caution.
func (mt *MetaTable) GetCollectionID(ctx context.Context, dbName string, collectionName string) UniqueID {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Warn("db name is empty", zap.String("collectionName", collectionName))
		dbName = util.DefaultDBName
	}

	_, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp)
	if err != nil {
		return InvalidCollectionID
	}

	collectionID, ok := mt.aliases.get(dbName, collectionName)
	if ok {
		return collectionID
	}

	collectionID, ok = mt.names.get(dbName, collectionName)
	if ok {
		return collectionID
	}
	return InvalidCollectionID
}

// Note: The returned model.Collection is read-only. Do NOT modify it directly,
// as it may cause unexpected behavior or inconsistencies.
func (mt *MetaTable) getCollectionByNameInternal(ctx context.Context, dbName string, collectionName string, ts Timestamp) (*model.Collection, error) {
	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty", zap.String("collectionName", collectionName), zap.Uint64("ts", ts))
		dbName = util.DefaultDBName
	}

	db, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp)
	if err != nil {
		return nil, err
	}

	collectionID, ok := mt.aliases.get(dbName, collectionName)
	if ok {
		return mt.getCollectionByIDInternal(ctx, dbName, collectionID, ts, false)
	}

	collectionID, ok = mt.names.get(dbName, collectionName)
	if ok {
		return mt.getCollectionByIDInternal(ctx, dbName, collectionID, ts, false)
	}

	if isMaxTs(ts) {
		return nil, merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName)
	}

	// travel meta information from catalog. No need to check time travel logic again, since catalog already did.
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	coll, err := mt.catalog.GetCollectionByName(ctx1, db.ID, collectionName, ts)
	if err != nil {
		return nil, err
	}

	if coll == nil || !coll.Available() {
		return nil, merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName)
	}
	return filterUnavailable(coll), nil
}

func (mt *MetaTable) GetCollectionByID(ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.getCollectionByIDInternal(ctx, dbName, collectionID, ts, allowUnavailable)
}

// GetCollectionByIDWithMaxTs get collection, dbName can be ignored if ts is max timestamps
func (mt *MetaTable) GetCollectionByIDWithMaxTs(ctx context.Context, collectionID UniqueID) (*model.Collection, error) {
	return mt.GetCollectionByID(ctx, "", collectionID, typeutil.MaxTimestamp, false)
}

func (mt *MetaTable) ListAllAvailCollections(ctx context.Context) map[int64][]int64 {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	ret := make(map[int64][]int64, len(mt.dbName2Meta))
	for _, dbMeta := range mt.dbName2Meta {
		ret[dbMeta.ID] = make([]int64, 0)
	}

	for collID, collMeta := range mt.collID2Meta {
		if !collMeta.Available() {
			continue
		}
		dbID := collMeta.DBID
		if dbID == util.NonDBID {
			ret[util.DefaultDBID] = append(ret[util.DefaultDBID], collID)
			continue
		}
		ret[dbID] = append(ret[dbID], collID)
	}

	return ret
}

func (mt *MetaTable) ListAllAvailPartitions(ctx context.Context) map[int64]map[int64][]int64 {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	ret := make(map[int64]map[int64][]int64, len(mt.dbName2Meta))
	for _, dbMeta := range mt.dbName2Meta {
		// Database may not have available collections.
		ret[dbMeta.ID] = make(map[int64][]int64, 64)
	}
	for _, collMeta := range mt.collID2Meta {
		if !collMeta.Available() {
			continue
		}
		dbID := collMeta.DBID
		if dbID == util.NonDBID {
			dbID = util.DefaultDBID
		}
		if _, ok := ret[dbID]; !ok {
			ret[dbID] = make(map[int64][]int64, 64)
		}
		ret[dbID][collMeta.CollectionID] = lo.Map(collMeta.Partitions, func(part *model.Partition, _ int) int64 { return part.PartitionID })
	}
	return ret
}

func (mt *MetaTable) ListCollections(ctx context.Context, dbName string, ts Timestamp, onlyAvail bool) ([]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if isMaxTs(ts) {
		return mt.listCollectionFromCache(ctx, dbName, onlyAvail)
	}

	db, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp)
	if err != nil {
		return nil, err
	}

	// list collections should always be loaded from catalog.
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	colls, err := mt.catalog.ListCollections(ctx1, db.ID, ts)
	if err != nil {
		return nil, err
	}
	onlineCollections := make([]*model.Collection, 0, len(colls))
	for _, coll := range colls {
		if onlyAvail && !coll.Available() {
			continue
		}
		onlineCollections = append(onlineCollections, coll)
	}
	return onlineCollections, nil
}

func (mt *MetaTable) listCollectionFromCache(ctx context.Context, dbName string, onlyAvail bool) ([]*model.Collection, error) {
	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty")
		dbName = util.DefaultDBName
	}

	db, ok := mt.dbName2Meta[dbName]
	if !ok {
		return nil, merr.WrapErrDatabaseNotFound(dbName)
	}

	collectionFromCache := make([]*model.Collection, 0, len(mt.collID2Meta))
	for _, collMeta := range mt.collID2Meta {
		if (collMeta.DBID != util.NonDBID && db.ID == collMeta.DBID) ||
			(collMeta.DBID == util.NonDBID && dbName == util.DefaultDBName) {
			if onlyAvail && !collMeta.Available() {
				continue
			}

			collectionFromCache = append(collectionFromCache, collMeta)
		}
	}
	return collectionFromCache, nil
}

// ListCollectionPhysicalChannels list physical channels of all collections.
func (mt *MetaTable) ListCollectionPhysicalChannels(ctx context.Context) map[typeutil.UniqueID][]string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	chanMap := make(map[UniqueID][]string)

	for id, collInfo := range mt.collID2Meta {
		chanMap[id] = common.CloneStringList(collInfo.PhysicalChannelNames)
	}

	return chanMap
}

func (mt *MetaTable) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp, fieldModify bool) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterCollection(ctx1, oldColl, newColl, metastore.MODIFY, ts, fieldModify); err != nil {
		return err
	}
	mt.collID2Meta[oldColl.CollectionID] = newColl
	log.Ctx(ctx).Info("alter collection finished", zap.Int64("collectionID", oldColl.CollectionID), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) RenameCollection(ctx context.Context, dbName string, oldName string, newDBName string, newName string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx = contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	log := log.Ctx(ctx).With(
		zap.String("oldDBName", dbName),
		zap.String("oldName", oldName),
		zap.String("newDBName", newDBName),
		zap.String("newName", newName),
	)

	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Warn("db name is empty")
		dbName = util.DefaultDBName
	}

	if newDBName == "" {
		log.Warn("target db name is empty")
		newDBName = dbName
	}

	// check target db
	targetDB, ok := mt.dbName2Meta[newDBName]
	if !ok {
		return fmt.Errorf("target database:%s not found", newDBName)
	}

	// old collection should not be an alias
	_, ok = mt.aliases.get(dbName, oldName)
	if ok {
		log.Warn("unsupported use a alias to rename collection")
		return fmt.Errorf("unsupported use an alias to rename collection, alias:%s", oldName)
	}

	_, ok = mt.aliases.get(newDBName, newName)
	if ok {
		log.Warn("cannot rename collection to an existing alias")
		return fmt.Errorf("cannot rename collection to an existing alias: %s", newName)
	}

	// check new collection already exists
	coll, err := mt.getCollectionByNameInternal(ctx, newDBName, newName, ts)
	if coll != nil {
		log.Warn("check new collection fail")
		return fmt.Errorf("duplicated new collection name %s:%s with other collection name or alias", newDBName, newName)
	}
	if err != nil && !errors.Is(err, merr.ErrCollectionNotFound) {
		log.Warn("check new collection name fail")
		return err
	}

	// get old collection meta
	oldColl, err := mt.getCollectionByNameInternal(ctx, dbName, oldName, ts)
	if err != nil {
		log.Warn("get old collection fail")
		return err
	}

	// unsupported rename collection while the collection has aliases
	aliases := mt.listAliasesByID(oldColl.CollectionID)
	if len(aliases) > 0 && oldColl.DBID != targetDB.ID {
		return errors.New("fail to rename db name, must drop all aliases of this collection before rename")
	}

	newColl := oldColl.Clone()
	newColl.Name = newName
	newColl.DBName = dbName
	newColl.DBID = targetDB.ID
	if oldColl.DBID == newColl.DBID {
		if err := mt.catalog.AlterCollection(ctx, oldColl, newColl, metastore.MODIFY, ts, false); err != nil {
			log.Warn("alter collection by catalog failed", zap.Error(err))
			return err
		}
	} else {
		if err := mt.catalog.AlterCollectionDB(ctx, oldColl, newColl, ts); err != nil {
			log.Warn("alter collectionDB by catalog failed", zap.Error(err))
			return err
		}
	}

	mt.names.insert(newDBName, newName, oldColl.CollectionID)
	mt.names.remove(dbName, oldName)

	mt.collID2Meta[oldColl.CollectionID] = newColl

	log.Info("rename collection finished")
	return nil
}

// GetCollectionVirtualChannels returns virtual channels of a given collection.
func (mt *MetaTable) GetCollectionVirtualChannels(ctx context.Context, colID int64) []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	for id, collInfo := range mt.collID2Meta {
		if id == colID {
			return common.CloneStringList(collInfo.VirtualChannelNames)
		}
	}
	return nil
}

// GetPChannelInfo returns infos on pchannel.
func (mt *MetaTable) GetPChannelInfo(ctx context.Context, pchannel string) *rootcoordpb.GetPChannelInfoResponse {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	resp := &rootcoordpb.GetPChannelInfoResponse{
		Status:      merr.Success(),
		Collections: make([]*rootcoordpb.CollectionInfoOnPChannel, 0),
	}
	for _, collInfo := range mt.collID2Meta {
		if collInfo.State != pb.CollectionState_CollectionCreated && collInfo.State != pb.CollectionState_CollectionDropping {
			// streamingnode will receive the createCollectionMessage to recover if the collection is creating.
			// streamingnode use it to recover the collection state at first time streaming arch enabled.
			// streamingnode will get the dropping collection and drop it before streaming arch enabled.
			continue
		}
		if idx := lo.IndexOf(collInfo.PhysicalChannelNames, pchannel); idx >= 0 {
			partitions := make([]*rootcoordpb.PartitionInfoOnPChannel, 0, len(collInfo.Partitions))
			for _, part := range collInfo.Partitions {
				partitions = append(partitions, &rootcoordpb.PartitionInfoOnPChannel{
					PartitionId: part.PartitionID,
				})
			}
			resp.Collections = append(resp.Collections, &rootcoordpb.CollectionInfoOnPChannel{
				CollectionId: collInfo.CollectionID,
				Partitions:   partitions,
				Vchannel:     collInfo.VirtualChannelNames[idx],
				State:        collInfo.State,
			})
		}
	}
	return resp
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
	if err := mt.catalog.CreatePartition(ctx, coll.DBID, partition, partition.PartitionCreatedTimestamp); err != nil {
		return err
	}
	mt.collID2Meta[partition.CollectionID].Partitions = append(mt.collID2Meta[partition.CollectionID].Partitions, partition.Clone())

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
			if err := mt.catalog.AlterPartition(ctx1, coll.DBID, part, clone, metastore.MODIFY, ts); err != nil {
				return err
			}
			mt.collID2Meta[collectionID].Partitions[idx] = clone

			switch state {
			case pb.PartitionState_PartitionCreated:
				mt.generalCnt += int(coll.ShardsNum) // 1 partition * shardNum
				// support Dynamic load/release partitions
				metrics.RootCoordNumOfPartitions.WithLabelValues().Inc()
			case pb.PartitionState_PartitionDropping:
				mt.generalCnt -= int(coll.ShardsNum) // 1 partition * shardNum
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

func (mt *MetaTable) RemovePartition(ctx context.Context, dbID int64, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.DropPartition(ctx1, dbID, collectionID, partitionID, ts); err != nil {
		return err
	}
	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return nil
	}
	loc := -1
	for idx, part := range coll.Partitions {
		if part.PartitionID == partitionID {
			loc = idx
			break
		}
	}
	if loc != -1 {
		coll.Partitions = append(coll.Partitions[:loc], coll.Partitions[loc+1:]...)
	}
	log.Ctx(ctx).Info("remove partition", zap.Int64("collection", collectionID), zap.Int64("partition", partitionID), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) CreateAlias(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty", zap.String("alias", alias), zap.String("collection", collectionName))
		dbName = util.DefaultDBName
	}

	// It's ok that we don't read from catalog when cache missed.
	// Since cache always keep the latest version, and the ts should always be the latest.

	if !mt.names.exist(dbName) {
		return merr.WrapErrDatabaseNotFound(dbName)
	}

	if collID, ok := mt.names.get(dbName, alias); ok {
		coll, ok := mt.collID2Meta[collID]
		if !ok {
			return errors.New("meta error, name mapped non-exist collection id")
		}
		// allow alias with dropping&dropped
		if coll.State != pb.CollectionState_CollectionDropping && coll.State != pb.CollectionState_CollectionDropped {
			return merr.WrapErrAliasCollectionNameConflict(dbName, alias)
		}
	}

	collectionID, ok := mt.names.get(dbName, collectionName)
	if !ok {
		// you cannot alias to a non-existent collection.
		return merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName)
	}

	// check if alias exists.
	aliasedCollectionID, ok := mt.aliases.get(dbName, alias)
	if ok && aliasedCollectionID == collectionID {
		log.Ctx(ctx).Warn("add duplicate alias", zap.String("alias", alias), zap.String("collection", collectionName), zap.Uint64("ts", ts))
		return nil
	} else if ok {
		// TODO: better to check if aliasedCollectionID exist or is available, though not very possible.
		aliasedColl := mt.collID2Meta[aliasedCollectionID]
		msg := fmt.Sprintf("%s is alias to another collection: %s", alias, aliasedColl.Name)
		return merr.WrapErrAliasAlreadyExist(dbName, alias, msg)
	}
	// alias didn't exist.

	coll, ok := mt.collID2Meta[collectionID]
	if !ok || !coll.Available() {
		// you cannot alias to a non-existent collection.
		return merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName)
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.CreateAlias(ctx1, &model.Alias{
		Name:         alias,
		CollectionID: collectionID,
		CreatedTime:  ts,
		State:        pb.AliasState_AliasCreated,
		DbID:         coll.DBID,
	}, ts); err != nil {
		return err
	}

	mt.aliases.insert(dbName, alias, collectionID)

	log.Ctx(ctx).Info("create alias",
		zap.String("db", dbName),
		zap.String("alias", alias),
		zap.String("collection", collectionName),
		zap.Int64("id", coll.CollectionID),
		zap.Uint64("ts", ts),
	)

	return nil
}

func (mt *MetaTable) DropAlias(ctx context.Context, dbName string, alias string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty", zap.String("alias", alias), zap.Uint64("ts", ts))
		dbName = util.DefaultDBName
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	db, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	if err := mt.catalog.DropAlias(ctx1, db.ID, alias, ts); err != nil {
		return err
	}

	mt.aliases.remove(dbName, alias)

	log.Ctx(ctx).Info("drop alias",
		zap.String("db", dbName),
		zap.String("alias", alias),
		zap.Uint64("ts", ts),
	)

	return nil
}

func (mt *MetaTable) AlterAlias(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	// backward compatibility for rolling  upgrade
	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty", zap.String("alias", alias), zap.String("collection", collectionName))
		dbName = util.DefaultDBName
	}

	// It's ok that we don't read from catalog when cache missed.
	// Since cache always keep the latest version, and the ts should always be the latest.

	if !mt.names.exist(dbName) {
		return merr.WrapErrDatabaseNotFound(dbName)
	}

	if collID, ok := mt.names.get(dbName, alias); ok {
		coll := mt.collID2Meta[collID]
		// allow alias with dropping&dropped
		if coll.State != pb.CollectionState_CollectionDropping && coll.State != pb.CollectionState_CollectionDropped {
			return merr.WrapErrAliasCollectionNameConflict(dbName, alias)
		}
	}

	collectionID, ok := mt.names.get(dbName, collectionName)
	if !ok {
		// you cannot alias to a non-existent collection.
		return merr.WrapErrCollectionNotFound(collectionName)
	}

	coll, ok := mt.collID2Meta[collectionID]
	if !ok || !coll.Available() {
		// you cannot alias to a non-existent collection.
		return merr.WrapErrCollectionNotFound(collectionName)
	}

	// check if alias exists.
	_, ok = mt.aliases.get(dbName, alias)
	if !ok {
		//
		return merr.WrapErrAliasNotFound(dbName, alias)
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterAlias(ctx1, &model.Alias{
		Name:         alias,
		CollectionID: collectionID,
		CreatedTime:  ts,
		State:        pb.AliasState_AliasCreated,
		DbID:         coll.DBID,
	}, ts); err != nil {
		return err
	}

	// alias switch to another collection anyway.
	mt.aliases.insert(dbName, alias, collectionID)

	log.Ctx(ctx).Info("alter alias",
		zap.String("db", dbName),
		zap.String("alias", alias),
		zap.String("collection", collectionName),
		zap.Uint64("ts", ts),
	)

	return nil
}

func (mt *MetaTable) DescribeAlias(ctx context.Context, dbName string, alias string, ts Timestamp) (string, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty", zap.String("alias", alias))
		dbName = util.DefaultDBName
	}

	// check if database exists.
	dbExist := mt.aliases.exist(dbName)
	if !dbExist {
		return "", merr.WrapErrDatabaseNotFound(dbName)
	}
	// check if alias exists.
	collectionID, ok := mt.aliases.get(dbName, alias)
	if !ok {
		return "", merr.WrapErrAliasNotFound(dbName, alias)
	}

	collectionMeta, ok := mt.collID2Meta[collectionID]
	if !ok {
		return "", merr.WrapErrCollectionIDOfAliasNotFound(collectionID)
	}
	if collectionMeta.State == pb.CollectionState_CollectionCreated {
		return collectionMeta.Name, nil
	}
	return "", merr.WrapErrAliasNotFound(dbName, alias)
}

func (mt *MetaTable) ListAliases(ctx context.Context, dbName string, collectionName string, ts Timestamp) ([]string, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if dbName == "" {
		log.Ctx(ctx).Warn("db name is empty", zap.String("collection", collectionName))
		dbName = util.DefaultDBName
	}

	// check if database exists.
	dbExist := mt.aliases.exist(dbName)
	if !dbExist {
		return nil, merr.WrapErrDatabaseNotFound(dbName)
	}
	var aliases []string
	if collectionName == "" {
		collections := mt.aliases.listCollections(dbName)
		for name, collectionID := range collections {
			if collectionMeta, ok := mt.collID2Meta[collectionID]; ok &&
				collectionMeta.State == pb.CollectionState_CollectionCreated {
				aliases = append(aliases, name)
			}
		}
	} else {
		collectionID, exist := mt.names.get(dbName, collectionName)
		collectionMeta, exist2 := mt.collID2Meta[collectionID]
		if exist && exist2 && collectionMeta.State == pb.CollectionState_CollectionCreated {
			aliases = mt.listAliasesByID(collectionID)
		} else {
			return nil, merr.WrapErrCollectionNotFound(collectionName)
		}
	}
	return aliases, nil
}

func (mt *MetaTable) IsAlias(ctx context.Context, db, name string) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	_, ok := mt.aliases.get(db, name)
	return ok
}

func (mt *MetaTable) listAliasesByID(collID UniqueID) []string {
	ret := make([]string, 0)
	mt.aliases.iterate(func(db string, collection string, id UniqueID) bool {
		if collID == id {
			ret = append(ret, collection)
		}
		return true
	})
	return ret
}

func (mt *MetaTable) ListAliasesByID(ctx context.Context, collID UniqueID) []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.listAliasesByID(collID)
}

// GetGeneralCount gets the general count(sum of product of partition number and shard number).
func (mt *MetaTable) GetGeneralCount(ctx context.Context) int {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.generalCnt
}

// AddCredential add credential
func (mt *MetaTable) AddCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	if credInfo.Username == "" {
		return errors.New("username is empty")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	usernames, err := mt.catalog.ListCredentials(ctx)
	if err != nil {
		return err
	}
	if len(usernames) >= Params.ProxyCfg.MaxUserNum.GetAsInt() {
		errMsg := "unable to add user because the number of users has reached the limit"
		log.Ctx(ctx).Error(errMsg, zap.Int("max_user_num", Params.ProxyCfg.MaxUserNum.GetAsInt()))
		return errors.New(errMsg)
	}

	if origin, _ := mt.catalog.GetCredential(ctx, credInfo.Username); origin != nil {
		return fmt.Errorf("user already exists: %s", credInfo.Username)
	}

	credential := &model.Credential{
		Username:          credInfo.Username,
		EncryptedPassword: credInfo.EncryptedPassword,
	}
	return mt.catalog.CreateCredential(ctx, credential)
}

// AlterCredential update credential
func (mt *MetaTable) AlterCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	if credInfo.Username == "" {
		return errors.New("username is empty")
	}

	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	credential := &model.Credential{
		Username:          credInfo.Username,
		EncryptedPassword: credInfo.EncryptedPassword,
	}
	return mt.catalog.AlterCredential(ctx, credential)
}

// GetCredential get credential by username
func (mt *MetaTable) GetCredential(ctx context.Context, username string) (*internalpb.CredentialInfo, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	credential, err := mt.catalog.GetCredential(ctx, username)
	return model.MarshalCredentialModel(credential), err
}

// DeleteCredential delete credential
func (mt *MetaTable) DeleteCredential(ctx context.Context, username string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DropCredential(ctx, username)
}

// ListCredentialUsernames list credential usernames
func (mt *MetaTable) ListCredentialUsernames(ctx context.Context) (*milvuspb.ListCredUsersResponse, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	usernames, err := mt.catalog.ListCredentials(ctx)
	if err != nil {
		return nil, fmt.Errorf("list credential usernames err:%w", err)
	}
	return &milvuspb.ListCredUsersResponse{Usernames: usernames}, nil
}

// CreateRole create role
func (mt *MetaTable) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	if funcutil.IsEmptyString(entity.Name) {
		return errors.New("the role name in the role info is empty")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	results, err := mt.catalog.ListRole(ctx, tenant, nil, false)
	if err != nil {
		log.Ctx(ctx).Warn("fail to list roles", zap.Error(err))
		return err
	}
	for _, result := range results {
		if result.GetRole().GetName() == entity.Name {
			log.Ctx(ctx).Info("role already exists", zap.String("role", entity.Name))
			return common.NewIgnorableError(errors.Newf("role [%s] already exists", entity))
		}
	}
	if len(results) >= Params.ProxyCfg.MaxRoleNum.GetAsInt() {
		errMsg := "unable to create role because the number of roles has reached the limit"
		log.Ctx(ctx).Warn(errMsg, zap.Int("max_role_num", Params.ProxyCfg.MaxRoleNum.GetAsInt()))
		return errors.New(errMsg)
	}

	return mt.catalog.CreateRole(ctx, tenant, entity)
}

// DropRole drop role info
func (mt *MetaTable) DropRole(ctx context.Context, tenant string, roleName string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DropRole(ctx, tenant, roleName)
}

// OperateUserRole operate the relationship between a user and a role, including adding a user to a role and removing a user from a role
func (mt *MetaTable) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	if funcutil.IsEmptyString(userEntity.Name) {
		return errors.New("username in the user entity is empty")
	}
	if funcutil.IsEmptyString(roleEntity.Name) {
		return errors.New("role name in the role entity is empty")
	}

	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.AlterUserRole(ctx, tenant, userEntity, roleEntity, operateType)
}

// SelectRole select role.
// Enter the role condition by the entity param. And this param is nil, which means selecting all roles.
// Get all users that are added to the role by setting the includeUserInfo param to true.
func (mt *MetaTable) SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListRole(ctx, tenant, entity, includeUserInfo)
}

// SelectUser select user.
// Enter the user condition by the entity param. And this param is nil, which means selecting all users.
// Get all roles that are added the user to by setting the includeRoleInfo param to true.
func (mt *MetaTable) SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListUser(ctx, tenant, entity, includeRoleInfo)
}

// OperatePrivilege grant or revoke privilege by setting the operateType param
func (mt *MetaTable) OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	if funcutil.IsEmptyString(entity.ObjectName) {
		return errors.New("the object name in the grant entity is empty")
	}
	if entity.Object == nil || funcutil.IsEmptyString(entity.Object.Name) {
		return errors.New("the object entity in the grant entity is invalid")
	}
	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return errors.New("the role entity in the grant entity is invalid")
	}
	if entity.Grantor == nil {
		return errors.New("the grantor in the grant entity is empty")
	}
	if entity.Grantor.Privilege == nil || funcutil.IsEmptyString(entity.Grantor.Privilege.Name) {
		return errors.New("the privilege name in the grant entity is empty")
	}
	if entity.Grantor.User == nil || funcutil.IsEmptyString(entity.Grantor.User.Name) {
		return errors.New("the grantor name in the grant entity is empty")
	}
	if !funcutil.IsRevoke(operateType) && !funcutil.IsGrant(operateType) {
		return errors.New("the operate type in the grant entity is invalid")
	}
	if entity.DbName == "" {
		entity.DbName = util.DefaultDBName
	}

	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.AlterGrant(ctx, tenant, entity, operateType)
}

// SelectGrant select grant
// The principal entity MUST be not empty in the grant entity
// The resource entity and the resource name are optional, and the two params should be not empty together when you select some grants about the resource kind.
func (mt *MetaTable) SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var entities []*milvuspb.GrantEntity
	if entity == nil {
		return entities, errors.New("the grant entity is nil")
	}

	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return entities, errors.New("the role entity in the grant entity is invalid")
	}
	if entity.DbName == "" {
		entity.DbName = util.DefaultDBName
	}

	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListGrant(ctx, tenant, entity)
}

func (mt *MetaTable) DropGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	if role == nil || funcutil.IsEmptyString(role.Name) {
		return errors.New("the role entity is invalid when dropping the grant")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DeleteGrant(ctx, tenant, role)
}

func (mt *MetaTable) ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListPolicy(ctx, tenant)
}

func (mt *MetaTable) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListUserRole(ctx, tenant)
}

func (mt *MetaTable) BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.BackupRBAC(ctx, tenant)
}

func (mt *MetaTable) RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.RestoreRBAC(ctx, tenant, meta)
}

// check if the privilege group name is defined by users
func (mt *MetaTable) IsCustomPrivilegeGroup(ctx context.Context, groupName string) (bool, error) {
	privGroups, err := mt.catalog.ListPrivilegeGroups(ctx)
	if err != nil {
		return false, err
	}
	for _, group := range privGroups {
		if group.GroupName == groupName {
			return true, nil
		}
	}
	return false, nil
}

func (mt *MetaTable) CreatePrivilegeGroup(ctx context.Context, groupName string) error {
	if funcutil.IsEmptyString(groupName) {
		return errors.New("the privilege group name is empty")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	definedByUsers, err := mt.IsCustomPrivilegeGroup(ctx, groupName)
	if err != nil {
		return err
	}
	if definedByUsers {
		return merr.WrapErrParameterInvalidMsg("privilege group name [%s] is defined by users", groupName)
	}
	if util.IsPrivilegeNameDefined(groupName) {
		return merr.WrapErrParameterInvalidMsg("privilege group name [%s] is defined by built in privileges or privilege groups in system", groupName)
	}
	data := &milvuspb.PrivilegeGroupInfo{
		GroupName:  groupName,
		Privileges: make([]*milvuspb.PrivilegeEntity, 0),
	}
	return mt.catalog.SavePrivilegeGroup(ctx, data)
}

func (mt *MetaTable) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	if funcutil.IsEmptyString(groupName) {
		return errors.New("the privilege group name is empty")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	definedByUsers, err := mt.IsCustomPrivilegeGroup(ctx, groupName)
	if err != nil {
		return err
	}
	if !definedByUsers {
		return nil
	}
	// check if the group is used by any role
	roles, err := mt.catalog.ListRole(ctx, util.DefaultTenant, nil, false)
	if err != nil {
		return err
	}
	roleEntity := lo.Map(roles, func(entity *milvuspb.RoleResult, _ int) *milvuspb.RoleEntity {
		return entity.GetRole()
	})
	for _, role := range roleEntity {
		grants, err := mt.catalog.ListGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
			Role:   role,
			DbName: util.AnyWord,
		})
		if err != nil {
			return err
		}
		for _, grant := range grants {
			if grant.Grantor.Privilege.Name == groupName {
				return errors.Newf("privilege group [%s] is used by role [%s], Use REVOKE API to revoke it first", groupName, role.GetName())
			}
		}
	}
	return mt.catalog.DropPrivilegeGroup(ctx, groupName)
}

func (mt *MetaTable) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.ListPrivilegeGroups(ctx)
}

func (mt *MetaTable) OperatePrivilegeGroup(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
	if funcutil.IsEmptyString(groupName) {
		return errors.New("the privilege group name is empty")
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	if util.IsBuiltinPrivilegeGroup(groupName) {
		return merr.WrapErrParameterInvalidMsg("the privilege group name [%s] is defined by built in privilege groups in system", groupName)
	}

	// validate input params
	definedByUsers, err := mt.IsCustomPrivilegeGroup(ctx, groupName)
	if err != nil {
		return err
	}
	if !definedByUsers {
		return merr.WrapErrParameterInvalidMsg("there is no privilege group name [%s] to operate", groupName)
	}
	groups, err := mt.catalog.ListPrivilegeGroups(ctx)
	if err != nil {
		return err
	}
	for _, p := range privileges {
		if util.IsPrivilegeNameDefined(p.Name) {
			continue
		}
		for _, group := range groups {
			// add privileges for custom privilege group
			if group.GroupName == p.Name {
				privileges = append(privileges, group.Privileges...)
			} else {
				return merr.WrapErrParameterInvalidMsg("there is no privilege name or privilege group name [%s] defined in system to operate", p.Name)
			}
		}
	}

	// merge with current privileges
	group, err := mt.catalog.GetPrivilegeGroup(ctx, groupName)
	if err != nil {
		log.Ctx(ctx).Warn("fail to get privilege group", zap.String("privilege_group", groupName), zap.Error(err))
		return err
	}
	privSet := lo.SliceToMap(group.Privileges, func(p *milvuspb.PrivilegeEntity) (string, struct{}) {
		return p.Name, struct{}{}
	})
	switch operateType {
	case milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup:
		for _, p := range privileges {
			privSet[p.Name] = struct{}{}
		}
	case milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup:
		for _, p := range privileges {
			delete(privSet, p.Name)
		}
	default:
		log.Ctx(ctx).Warn("unsupported operate type", zap.Any("operate_type", operateType))
		return fmt.Errorf("unsupported operate type: %v", operateType)
	}

	mergedPrivs := lo.Map(lo.Keys(privSet), func(priv string, _ int) *milvuspb.PrivilegeEntity {
		return &milvuspb.PrivilegeEntity{Name: priv}
	})
	data := &milvuspb.PrivilegeGroupInfo{
		GroupName:  groupName,
		Privileges: mergedPrivs,
	}
	return mt.catalog.SavePrivilegeGroup(ctx, data)
}

func (mt *MetaTable) GetPrivilegeGroupRoles(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error) {
	if funcutil.IsEmptyString(groupName) {
		return nil, errors.New("the privilege group name is empty")
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	// get all roles
	roles, err := mt.catalog.ListRole(ctx, util.DefaultTenant, nil, false)
	if err != nil {
		return nil, err
	}
	roleEntity := lo.Map(roles, func(entity *milvuspb.RoleResult, _ int) *milvuspb.RoleEntity {
		return entity.GetRole()
	})

	rolesMap := make(map[*milvuspb.RoleEntity]struct{})
	for _, role := range roleEntity {
		grants, err := mt.catalog.ListGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
			Role:   role,
			DbName: util.AnyWord,
		})
		if err != nil {
			return nil, err
		}
		for _, grant := range grants {
			if grant.Grantor.Privilege.Name == groupName {
				rolesMap[role] = struct{}{}
			}
		}
	}
	return lo.Keys(rolesMap), nil
}
