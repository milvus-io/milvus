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
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	errIgnoredAlterAlias       = errors.New("ignored alter alias")       // alias already created on current collection, so it can be ignored.
	errIgnoredAlterCollection  = errors.New("ignored alter collection")  // collection already created, so it can be ignored.
	errIgnoredAlterDatabase    = errors.New("ignored alter database")    // database already created, so it can be ignored.
	errIgnoredCreateCollection = errors.New("ignored create collection") // create collection with same schema, so it can be ignored.
	errIgnoerdCreatePartition  = errors.New("ignored create partition")  // partition is already exist, so it can be ignored.
	errIgnoredDropCollection   = errors.New("ignored drop collection")   // drop collection or database not found, so it can be ignored.
	errIgnoredDropPartition    = errors.New("ignored drop partition")    // drop partition not found, so it can be ignored.

	errAlterCollectionNotFound = errors.New("alter collection not found") // alter collection not found, so it can be ignored.
)

type MetaTableChecker interface {
	RBACChecker

	CheckIfDatabaseCreatable(ctx context.Context, req *milvuspb.CreateDatabaseRequest) error
	CheckIfDatabaseDroppable(ctx context.Context, req *milvuspb.DropDatabaseRequest) error

	CheckIfAliasCreatable(ctx context.Context, dbName string, alias string, collectionName string) error
	CheckIfAliasAlterable(ctx context.Context, dbName string, alias string, collectionName string) error
	CheckIfAliasDroppable(ctx context.Context, dbName string, alias string) error
}

//go:generate mockery --name=IMetaTable --structname=MockIMetaTable --output=./  --filename=mock_meta_table.go --with-expecter --inpackage
type IMetaTable interface {
	MetaTableChecker

	GetDatabaseByID(ctx context.Context, dbID int64, ts Timestamp) (*model.Database, error)
	GetDatabaseByName(ctx context.Context, dbName string, ts Timestamp) (*model.Database, error)
	CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error
	DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error
	ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error)
	AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error

	AddCollection(ctx context.Context, coll *model.Collection) error
	DropCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error
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
	DropPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error
	RemovePartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error

	// Alias
	AlterAlias(ctx context.Context, result message.BroadcastResultAlterAliasMessageV2) error
	DropAlias(ctx context.Context, result message.BroadcastResultDropAliasMessageV2) error
	DescribeAlias(ctx context.Context, dbName string, alias string, ts Timestamp) (string, error)
	ListAliases(ctx context.Context, dbName string, collectionName string, ts Timestamp) ([]string, error)

	AlterCollection(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error
	CheckIfCollectionRenamable(ctx context.Context, dbName string, oldName string, newDBName string, newName string) error
	GetGeneralCount(ctx context.Context) int

	// TODO: it'll be a big cost if we handle the time travel logic, since we should always list all aliases in catalog.
	IsAlias(ctx context.Context, db, name string) bool
	ListAliasesByID(ctx context.Context, collID UniqueID) []string

	GetCredential(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	InitCredential(ctx context.Context) error
	DeleteCredential(ctx context.Context, result message.BroadcastResultDropUserMessageV2) error
	AlterCredential(ctx context.Context, result message.BroadcastResultAlterUserMessageV2) error
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

	defaultRootKey := paramtable.GetCipherParams().DefaultRootKey.GetValue()
	if hookutil.IsClusterEncyptionEnabled() && len(defaultRootKey) > 0 {
		// Set unique ID as ezID because the default dbID for each cluster
		// is the same
		ezID, err := mt.tsoAllocator.GenerateTSO(1)
		if err != nil {
			return err
		}

		cipherProps := hookutil.GetDBCipherProperties(ezID, defaultRootKey)
		defaultProperties = append(defaultProperties, cipherProps...)
	}

	return mt.createDatabasePrivate(mt.ctx, model.NewDefaultDatabase(defaultProperties), ts)
}

func (mt *MetaTable) CheckIfDatabaseCreatable(ctx context.Context, req *milvuspb.CreateDatabaseRequest) error {
	dbName := req.GetDbName()

	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if _, ok := mt.dbName2Meta[dbName]; ok || mt.aliases.exist(dbName) || mt.names.exist(dbName) {
		// TODO: idempotency check here.
		return fmt.Errorf("database already exist: %s", dbName)
	}

	cfgMaxDatabaseNum := Params.RootCoordCfg.MaxDatabaseNum.GetAsInt()
	if len(mt.dbName2Meta) > cfgMaxDatabaseNum { // not include default database so use > instead of >= here.
		return merr.WrapErrDatabaseNumLimitExceeded(cfgMaxDatabaseNum)
	}
	return nil
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
	if err := hookutil.CreateEZByDBProperties(db.Properties); err != nil {
		return err
	}

	if err := mt.catalog.CreateDatabase(ctx, db, ts); err != nil {
		hookutil.RemoveEZByDBProperties(db.Properties) // ignore the error since create database failed
		return err
	}

	mt.names.createDbIfNotExist(dbName)
	mt.aliases.createDbIfNotExist(dbName)
	mt.dbName2Meta[dbName] = db

	log.Ctx(ctx).Info("create database", zap.String("db", dbName), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) AlterDatabase(ctx context.Context, newDB *model.Database, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterDatabase(ctx1, newDB, ts); err != nil {
		return err
	}
	mt.dbName2Meta[newDB.Name] = newDB
	log.Ctx(ctx).Info("alter database finished", zap.String("dbName", newDB.Name), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) CheckIfDatabaseDroppable(ctx context.Context, req *milvuspb.DropDatabaseRequest) error {
	dbName := req.GetDbName()
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if dbName == util.DefaultDBName {
		return errors.New("can not drop default database")
	}

	if _, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp); err != nil {
		log.Ctx(ctx).Warn("not found database", zap.String("db", dbName))
		return err
	}

	colls, err := mt.listCollectionFromCache(ctx, dbName, true)
	if err != nil {
		return err
	}
	if len(colls) > 0 {
		return fmt.Errorf("database:%s not empty, must drop all collections before drop database", dbName)
	}
	return nil
}

func (mt *MetaTable) DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	db, err := mt.getDatabaseByNameInternal(ctx, dbName, typeutil.MaxTimestamp)
	if err != nil {
		log.Ctx(ctx).Warn("not found database", zap.String("db", dbName))
		return nil
	}
	if err := mt.catalog.DropDatabase(ctx, db.ID, ts); err != nil {
		return err
	}

	// Call back cipher plugin when dropping database succeeded
	if err := hookutil.RemoveEZByDBProperties(db.Properties); err != nil {
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
	if coll.State != pb.CollectionState_CollectionCreated {
		return fmt.Errorf("collection state should be created, collection name: %s, collection id: %d, state: %s", coll.Name, coll.CollectionID, coll.State)
	}

	// check if there's a collection meta with the same collection id.
	// merge the collection meta together.
	if _, ok := mt.collID2Meta[coll.CollectionID]; ok {
		log.Ctx(ctx).Info("collection already created, skip add collection to meta table", zap.Int64("collectionID", coll.CollectionID))
		return nil
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.CreateCollection(ctx1, coll, coll.CreateTime); err != nil {
		return err
	}

	mt.collID2Meta[coll.CollectionID] = coll.Clone()
	mt.names.insert(coll.DBName, coll.Name, coll.CollectionID)

	pn := coll.GetPartitionNum(true)
	mt.generalCnt += pn * int(coll.ShardsNum)
	metrics.RootCoordNumOfCollections.WithLabelValues(coll.DBName).Inc()
	metrics.RootCoordNumOfPartitions.WithLabelValues().Add(float64(pn))

	channel.StaticPChannelStatsManager.MustGet().AddVChannel(coll.VirtualChannelNames...)
	log.Ctx(ctx).Info("add collection to meta table",
		zap.Int64("dbID", coll.DBID),
		zap.String("collection", coll.Name),
		zap.Int64("id", coll.CollectionID),
		zap.Uint64("ts", coll.CreateTime),
	)
	return nil
}

func (mt *MetaTable) DropCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return nil
	}
	if coll.State == pb.CollectionState_CollectionDropping {
		return nil
	}

	clone := coll.Clone()
	clone.State = pb.CollectionState_CollectionDropping
	clone.UpdateTimestamp = ts

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

	mt.generalCnt -= pn * int(coll.ShardsNum)
	channel.StaticPChannelStatsManager.MustGet().RemoveVChannel(coll.VirtualChannelNames...)
	metrics.RootCoordNumOfCollections.WithLabelValues(db.Name).Dec()
	metrics.RootCoordNumOfPartitions.WithLabelValues().Sub(float64(pn))

	log.Ctx(ctx).Info("drop collection from meta table", zap.Int64("collection", collectionID),
		zap.String("state", coll.State.String()), zap.Uint64("ts", ts))
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
	if coll.State != pb.CollectionState_CollectionDropping {
		return fmt.Errorf("remove collection which state is not dropping, collectionID: %d, state: %s", collectionID, coll.State.String())
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
	ctx = contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	coll, err := mt.catalog.GetCollectionByName(ctx, db.ID, db.Name, collectionName, ts)
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

// AlterCollection is used to alter a collection in the meta table.
func (mt *MetaTable) AlterCollection(ctx context.Context, result message.BroadcastResultAlterCollectionMessageV2) error {
	header := result.Message.Header()
	body := result.Message.MustBody()

	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll, ok := mt.collID2Meta[header.CollectionId]
	if !ok {
		// collection not exists, return directly.
		return errAlterCollectionNotFound
	}

	oldColl := coll.Clone()
	newColl := coll.Clone()
	newColl.ApplyUpdates(header, body)
	fieldModify := false
	dbChanged := false
	for _, path := range header.UpdateMask.GetPaths() {
		switch path {
		case message.FieldMaskCollectionSchema:
			fieldModify = true
		case message.FieldMaskDB:
			dbChanged = true
		}
	}
	newColl.UpdateTimestamp = result.GetMaxTimeTick()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if !dbChanged {
		if err := mt.catalog.AlterCollection(ctx1, oldColl, newColl, metastore.MODIFY, newColl.UpdateTimestamp, fieldModify); err != nil {
			return err
		}
	} else {
		if err := mt.catalog.AlterCollectionDB(ctx1, oldColl, newColl, newColl.UpdateTimestamp); err != nil {
			return err
		}
	}

	mt.names.remove(oldColl.DBName, oldColl.Name)
	mt.names.insert(newColl.DBName, newColl.Name, newColl.CollectionID)
	mt.collID2Meta[header.CollectionId] = newColl
	log.Ctx(ctx).Info("alter collection finished", zap.Bool("dbChanged", dbChanged), zap.Int64("collectionID", oldColl.CollectionID), zap.Uint64("ts", newColl.UpdateTimestamp))
	return nil
}

func (mt *MetaTable) CheckIfCollectionRenamable(ctx context.Context, dbName string, oldName string, newDBName string, newName string) error {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	ctx = contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	log := log.Ctx(ctx).With(
		zap.String("oldDBName", dbName),
		zap.String("newDBName", newDBName),
		zap.String("oldName", oldName),
		zap.String("newName", newName),
	)

	// DB name already filled in rename collection task prepare
	// get target db
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
	coll, err := mt.getCollectionByNameInternal(ctx, newDBName, newName, typeutil.MaxTimestamp)
	if coll != nil {
		log.Warn("duplicated new collection name, already taken by another collection or alias.")
		return fmt.Errorf("duplicated new collection name %s:%s with other collection name or alias", newDBName, newName)
	}
	if err != nil && !errors.Is(err, merr.ErrCollectionNotFound) {
		log.Warn("fail to check if new collection name is already taken", zap.Error(err))
		return err
	}

	// get old collection meta
	oldColl, err := mt.getCollectionByNameInternal(ctx, dbName, oldName, typeutil.MaxTimestamp)
	if err != nil {
		log.Warn("fail to find collection with old name", zap.Error(err))
		return err
	}

	// unsupported rename collection while the collection has aliases
	aliases := mt.listAliasesByID(oldColl.CollectionID)
	if len(aliases) > 0 && oldColl.DBID != targetDB.ID {
		return errors.New("fail to rename db name, must drop all aliases of this collection before rename")
	}
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

	if partition.State != pb.PartitionState_PartitionCreated {
		return fmt.Errorf("partition state is not created, collection: %d, partition: %d, state: %s", partition.CollectionID, partition.PartitionID, partition.State)
	}

	// idempotency check here.
	for _, part := range coll.Partitions {
		if part.PartitionID == partition.PartitionID {
			log.Ctx(ctx).Info("partition already exists, ignore the operation", zap.Int64("collection", partition.CollectionID), zap.Int64("partition", partition.PartitionID))
			return nil
		}
	}
	if err := mt.catalog.CreatePartition(ctx, coll.DBID, partition, partition.PartitionCreatedTimestamp); err != nil {
		return err
	}
	mt.collID2Meta[partition.CollectionID].Partitions = append(mt.collID2Meta[partition.CollectionID].Partitions, partition.Clone())

	log.Ctx(ctx).Info("add partition to meta table",
		zap.Int64("collection", partition.CollectionID), zap.String("partition", partition.PartitionName),
		zap.Int64("partitionid", partition.PartitionID), zap.Uint64("ts", partition.PartitionCreatedTimestamp))
	mt.generalCnt += int(coll.ShardsNum) // 1 partition * shardNum
	// support Dynamic load/release partitions
	metrics.RootCoordNumOfPartitions.WithLabelValues().Inc()

	return nil
}

func (mt *MetaTable) DropPartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return nil
	}
	for idx, part := range coll.Partitions {
		if part.PartitionID == partitionID {
			if part.State == pb.PartitionState_PartitionDropping {
				// promise idempotency here.
				return nil
			}
			clone := part.Clone()
			clone.State = pb.PartitionState_PartitionDropping
			ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
			if err := mt.catalog.AlterPartition(ctx1, coll.DBID, part, clone, metastore.MODIFY, ts); err != nil {
				return err
			}
			mt.collID2Meta[collectionID].Partitions[idx] = clone

			log.Ctx(ctx).Info("drop partition", zap.Int64("collection", collectionID),
				zap.Int64("partition", partitionID),
				zap.Uint64("ts", ts))

			mt.generalCnt -= int(coll.ShardsNum) // 1 partition * shardNum
			metrics.RootCoordNumOfPartitions.WithLabelValues().Dec()
			return nil
		}
	}
	// partition not found, so promise idempotency here.
	return nil
}

func (mt *MetaTable) RemovePartition(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

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
	if loc == -1 {
		log.Ctx(ctx).Warn("not found partition, skip remove", zap.Int64("collection", collectionID), zap.Int64("partition", partitionID))
		return nil
	}
	partition := coll.Partitions[loc]
	if partition.State != pb.PartitionState_PartitionDropping {
		return fmt.Errorf("remove partition which state is not dropping, collection: %d, partition: %d, state: %s", collectionID, partitionID, partition.State.String())
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.DropPartition(ctx1, coll.DBID, collectionID, partitionID, ts); err != nil {
		return err
	}
	coll.Partitions = append(coll.Partitions[:loc], coll.Partitions[loc+1:]...)
	log.Ctx(ctx).Info("remove partition", zap.Int64("collection", collectionID), zap.Int64("partition", partitionID), zap.Uint64("ts", ts))
	return nil
}

func (mt *MetaTable) CheckIfAliasCreatable(ctx context.Context, dbName string, alias string, collectionName string) error {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
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
		log.Ctx(ctx).Warn("add duplicate alias", zap.String("alias", alias), zap.String("collection", collectionName))
		return errIgnoredAlterAlias
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
	return nil
}

func (mt *MetaTable) CheckIfAliasDroppable(ctx context.Context, dbName string, alias string) error {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if _, ok := mt.aliases.get(dbName, alias); !ok {
		return merr.WrapErrAliasNotFound(dbName, alias)
	}
	return nil
}

func (mt *MetaTable) DropAlias(ctx context.Context, result message.BroadcastResultDropAliasMessageV2) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	header := result.Message.Header()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.DropAlias(ctx1, header.DbId, header.Alias, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	mt.aliases.remove(header.DbName, header.Alias)

	log.Ctx(ctx).Info("drop alias",
		zap.String("db", header.DbName),
		zap.String("alias", header.Alias),
		zap.Uint64("ts", result.GetControlChannelResult().TimeTick),
	)
	return nil
}

func (mt *MetaTable) AlterAlias(ctx context.Context, result message.BroadcastResultAlterAliasMessageV2) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	header := result.Message.Header()
	if err := mt.catalog.AlterAlias(ctx, &model.Alias{
		Name:         header.Alias,
		CollectionID: header.CollectionId,
		CreatedTime:  result.GetControlChannelResult().TimeTick,
		State:        pb.AliasState_AliasCreated,
		DbID:         header.DbId,
	}, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}

	// alias switch to another collection anyway.
	mt.aliases.insert(header.DbName, header.Alias, header.CollectionId)

	log.Ctx(ctx).Info("alter alias",
		zap.String("db", header.DbName),
		zap.String("alias", header.Alias),
		zap.String("collection", header.CollectionName),
		zap.Uint64("ts", result.GetControlChannelResult().TimeTick),
	)
	return nil
}

func (mt *MetaTable) CheckIfAliasAlterable(ctx context.Context, dbName string, alias string, collectionName string) error {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
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
	existAliasCollectionID, ok := mt.aliases.get(dbName, alias)
	if !ok {
		return merr.WrapErrAliasNotFound(dbName, alias)
	}
	if existAliasCollectionID == collectionID {
		return errIgnoredAlterAlias
	}
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

func (mt *MetaTable) InitCredential(ctx context.Context) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	credInfo, err := mt.catalog.GetCredential(ctx, util.UserRoot)
	if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		return err
	}
	if credInfo != nil {
		return nil
	}
	encryptedRootPassword, err := crypto.PasswordEncrypt(Params.CommonCfg.DefaultRootPassword.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("RootCoord init user root failed", zap.Error(err))
		return err
	}
	log.Ctx(ctx).Info("RootCoord init user root")
	err = mt.catalog.AlterCredential(ctx, &model.Credential{
		Username:          util.UserRoot,
		EncryptedPassword: encryptedRootPassword,
	})
	if err != nil {
		log.Ctx(ctx).Warn("RootCoord init user root failed", zap.Error(err))
		return err
	}
	return nil
}

func (mt *MetaTable) CheckIfAddCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	if funcutil.IsEmptyString(credInfo.GetUsername()) {
		return errEmptyUsername
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	usernames, err := mt.catalog.ListCredentials(ctx)
	if err != nil {
		return err
	}
	// check if the username already exists.
	for _, username := range usernames {
		if username == credInfo.GetUsername() {
			return errUserAlreadyExists
		}
	}

	// check if the number of users has reached the limit.
	maxUserNum := Params.ProxyCfg.MaxUserNum.GetAsInt()
	if len(usernames) >= maxUserNum {
		errMsg := "unable to add user because the number of users has reached the limit"
		log.Ctx(ctx).Error(errMsg, zap.Int("maxUserNum", maxUserNum))
		return errors.New(errMsg)
	}
	return nil
}

func (mt *MetaTable) CheckIfUpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	if funcutil.IsEmptyString(credInfo.GetUsername()) {
		return errEmptyUsername
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	// check if the number of credential exists.
	if _, err := mt.catalog.GetCredential(ctx, credInfo.GetUsername()); err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return errUserNotFound
		}
		return err
	}
	return nil
}

// AlterCredential update credential
func (mt *MetaTable) AlterCredential(ctx context.Context, result message.BroadcastResultAlterUserMessageV2) error {
	body := result.Message.MustBody()

	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	existsCredential, err := mt.catalog.GetCredential(ctx, body.CredentialInfo.Username)
	if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		return err
	}
	// if the credential already exists and the version is not greater than the current timetick.
	if existsCredential != nil && existsCredential.TimeTick >= result.GetControlChannelResult().TimeTick {
		log.Info("credential already exists and the version is not greater than the current timetick",
			zap.String("username", body.CredentialInfo.Username),
			zap.Uint64("incoming", result.GetControlChannelResult().TimeTick),
			zap.Uint64("current", existsCredential.TimeTick),
		)
		return nil
	}
	credential := &model.Credential{
		Username:          body.CredentialInfo.Username,
		EncryptedPassword: body.CredentialInfo.EncryptedPassword,
		TimeTick:          result.GetControlChannelResult().TimeTick,
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

func (mt *MetaTable) CheckIfDeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) error {
	if funcutil.IsEmptyString(req.GetUsername()) {
		return errEmptyUsername
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	// check if the number of credential exists.
	if _, err := mt.catalog.GetCredential(ctx, req.GetUsername()); err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return errUserNotFound
		}
		return err
	}
	return nil
}

// DeleteCredential delete credential
func (mt *MetaTable) DeleteCredential(ctx context.Context, result message.BroadcastResultDropUserMessageV2) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	existsCredential, err := mt.catalog.GetCredential(ctx, result.Message.Header().UserName)
	if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		return err
	}
	// if the credential already exists and the version is not greater than the current timetick.
	if existsCredential != nil && existsCredential.TimeTick >= result.GetControlChannelResult().TimeTick {
		log.Info("credential already exists and the version is not greater than the current timetick",
			zap.String("username", result.Message.Header().UserName),
			zap.Uint64("incoming", result.GetControlChannelResult().TimeTick),
			zap.Uint64("current", existsCredential.TimeTick),
		)
		return nil
	}
	return mt.catalog.DropCredential(ctx, result.Message.Header().UserName)
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

// CheckIfCreateRole checks if the role can be created.
func (mt *MetaTable) CheckIfCreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) error {
	if funcutil.IsEmptyString(in.GetEntity().GetName()) {
		return errEmptyRoleName
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	results, err := mt.catalog.ListRole(ctx, util.DefaultTenant, nil, false)
	if err != nil {
		log.Ctx(ctx).Warn("fail to list roles", zap.Error(err))
		return err
	}
	for _, result := range results {
		if result.GetRole().GetName() == in.GetEntity().GetName() {
			log.Ctx(ctx).Info("role already exists", zap.String("role", in.GetEntity().GetName()))
			return errRoleAlreadyExists
		}
	}
	if len(results) >= Params.ProxyCfg.MaxRoleNum.GetAsInt() {
		errMsg := "unable to create role because the number of roles has reached the limit"
		log.Ctx(ctx).Warn(errMsg, zap.Int("max_role_num", Params.ProxyCfg.MaxRoleNum.GetAsInt()))
		return errors.New(errMsg)
	}
	return nil
}

// CreateRole create role
func (mt *MetaTable) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.CreateRole(ctx, tenant, entity)
}

func (mt *MetaTable) CheckIfDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error {
	if funcutil.IsEmptyString(in.GetRoleName()) {
		return errEmptyRoleName
	}
	if util.IsBuiltinRole(in.GetRoleName()) {
		return merr.WrapErrPrivilegeNotPermitted("the role[%s] is a builtin role, which can't be dropped", in.GetRoleName())
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	if _, err := mt.catalog.ListRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: in.GetRoleName()}, false); err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return errRoleNotExists
		}
		return err
	}
	if in.GetForceDrop() {
		return nil
	}

	grantEntities, err := mt.catalog.ListGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
		Role:   &milvuspb.RoleEntity{Name: in.GetRoleName()},
		DbName: "*",
	})
	if err != nil {
		return err
	}
	if len(grantEntities) != 0 {
		errMsg := "fail to drop the role that it has privileges. Use REVOKE API to revoke privileges"
		return errors.New(errMsg)
	}
	return nil
}

// DropRole drop role info
func (mt *MetaTable) DropRole(ctx context.Context, tenant string, roleName string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DropRole(ctx, tenant, roleName)
}

func (mt *MetaTable) CheckIfOperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) error {
	if funcutil.IsEmptyString(req.GetUsername()) {
		return errors.New("username in the user entity is empty")
	}
	if funcutil.IsEmptyString(req.GetRoleName()) {
		return errors.New("role name in the role entity is empty")
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	if _, err := mt.catalog.ListRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: req.RoleName}, false); err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return errRoleNotExists
		}
		return err
	}
	if req.Type != milvuspb.OperateUserRoleType_RemoveUserFromRole {
		if _, err := mt.catalog.ListUser(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: req.Username}, false); err != nil {
			errMsg := "not found the user, maybe the user isn't existed or internal system error"
			return errors.New(errMsg)
		}
	}
	return nil
}

// OperateUserRole operate the relationship between a user and a role, including adding a user to a role and removing a user from a role
func (mt *MetaTable) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
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

func (mt *MetaTable) CheckIfRBACRestorable(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) error {
	meta := req.GetRBACMeta()
	if len(meta.GetRoles()) == 0 && len(meta.GetPrivilegeGroups()) == 0 && len(meta.GetGrants()) == 0 && len(meta.GetUsers()) == 0 {
		return errEmptyRBACMeta
	}

	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	// check if role already exists
	existRoles, err := mt.catalog.ListRole(ctx, util.DefaultTenant, nil, false)
	if err != nil {
		return err
	}
	existRoleMap := lo.SliceToMap(existRoles, func(entity *milvuspb.RoleResult) (string, struct{}) { return entity.GetRole().GetName(), struct{}{} })
	existRoleAfterRestoreMap := lo.SliceToMap(existRoles, func(entity *milvuspb.RoleResult) (string, struct{}) { return entity.GetRole().GetName(), struct{}{} })
	for _, role := range meta.GetRoles() {
		if _, ok := existRoleMap[role.GetName()]; ok {
			return errors.Newf("role [%s] already exists", role.GetName())
		}
		existRoleAfterRestoreMap[role.GetName()] = struct{}{}
	}

	// check if privilege group already exists
	existPrivGroups, err := mt.catalog.ListPrivilegeGroups(ctx)
	if err != nil {
		return err
	}
	existPrivGroupMap := lo.SliceToMap(existPrivGroups, func(entity *milvuspb.PrivilegeGroupInfo) (string, struct{}) { return entity.GetGroupName(), struct{}{} })
	existPrivGroupAfterRestoreMap := lo.SliceToMap(existPrivGroups, func(entity *milvuspb.PrivilegeGroupInfo) (string, struct{}) { return entity.GetGroupName(), struct{}{} })
	for _, group := range meta.GetPrivilegeGroups() {
		if _, ok := existPrivGroupMap[group.GetGroupName()]; ok {
			return errors.Newf("privilege group [%s] already exists", group.GetGroupName())
		}
		existPrivGroupAfterRestoreMap[group.GetGroupName()] = struct{}{}
	}

	// check if grant can be restored
	for _, grant := range meta.GetGrants() {
		privName := grant.GetGrantor().GetPrivilege().GetName()
		if _, ok := existPrivGroupAfterRestoreMap[privName]; !ok && !util.IsPrivilegeNameDefined(privName) {
			return errors.Newf("privilege [%s] does not exist", privName)
		}
	}

	// check if user can be restored
	existUser, err := mt.catalog.ListUser(ctx, util.DefaultTenant, nil, false)
	if err != nil {
		return err
	}
	existUserMap := lo.SliceToMap(existUser, func(entity *milvuspb.UserResult) (string, struct{}) { return entity.GetUser().GetName(), struct{}{} })
	for _, user := range meta.GetUsers() {
		if _, ok := existUserMap[user.GetUser()]; ok {
			return errors.Newf("user [%s] already exists", user.GetUser())
		}

		// check if user-role can be restored
		for _, role := range user.GetRoles() {
			if _, ok := existRoleAfterRestoreMap[role.GetName()]; !ok {
				return errors.Newf("role [%s] does not exist", role.GetName())
			}
		}
	}
	return nil
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

func (mt *MetaTable) CheckIfPrivilegeGroupCreatable(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) error {
	if funcutil.IsEmptyString(req.GetGroupName()) {
		return errEmptyPrivilegeGroupName
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	definedByUsers, err := mt.IsCustomPrivilegeGroup(ctx, req.GetGroupName())
	if err != nil {
		return err
	}
	if definedByUsers {
		return merr.WrapErrParameterInvalidMsg("privilege group name [%s] is defined by users", req.GetGroupName())
	}
	if util.IsPrivilegeNameDefined(req.GetGroupName()) {
		return merr.WrapErrParameterInvalidMsg("privilege group name [%s] is defined by built in privileges or privilege groups in system", req.GetGroupName())
	}
	return nil
}

func (mt *MetaTable) CreatePrivilegeGroup(ctx context.Context, groupName string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	data := &milvuspb.PrivilegeGroupInfo{
		GroupName:  groupName,
		Privileges: make([]*milvuspb.PrivilegeEntity, 0),
	}
	return mt.catalog.SavePrivilegeGroup(ctx, data)
}

func (mt *MetaTable) CheckIfPrivilegeGroupDropable(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) error {
	if funcutil.IsEmptyString(req.GetGroupName()) {
		return errEmptyPrivilegeGroupName
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	definedByUsers, err := mt.IsCustomPrivilegeGroup(ctx, req.GetGroupName())
	if err != nil {
		return err
	}
	if !definedByUsers {
		return errNotCustomPrivilegeGroup
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
			if grant.Grantor.Privilege.Name == req.GetGroupName() {
				return errors.Newf("privilege group [%s] is used by role [%s], Use REVOKE API to revoke it first", req.GetGroupName(), role.GetName())
			}
		}
	}
	return nil
}

func (mt *MetaTable) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DropPrivilegeGroup(ctx, groupName)
}

func (mt *MetaTable) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	return mt.catalog.ListPrivilegeGroups(ctx)
}

// CheckIfPrivilegeGroupAlterable checks if the privilege group can be altered.
func (mt *MetaTable) CheckIfPrivilegeGroupAlterable(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) error {
	if funcutil.IsEmptyString(req.GetGroupName()) {
		return errEmptyPrivilegeGroupName
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	groups, err := mt.catalog.ListPrivilegeGroups(ctx)
	if err != nil {
		return err
	}
	currenctGroups := lo.SliceToMap(groups, func(group *milvuspb.PrivilegeGroupInfo) (string, []*milvuspb.PrivilegeEntity) {
		return group.GroupName, group.Privileges
	})
	// check if the privilege group is defined by users
	if _, ok := currenctGroups[req.GroupName]; !ok {
		return merr.WrapErrParameterInvalidMsg("there is no privilege group name [%s] defined in system to operate", req.GroupName)
	}

	if len(req.Privileges) == 0 {
		return merr.WrapErrParameterInvalidMsg("privileges is empty when alter the privilege group")
	}
	// check if the new incoming privileges are defined by users or built in
	for _, p := range req.Privileges {
		if util.IsPrivilegeNameDefined(p.Name) {
			continue
		}
		if _, ok := currenctGroups[p.Name]; !ok {
			return merr.WrapErrParameterInvalidMsg("there is no privilege name or privilege group name [%s] defined in system to operate", p.Name)
		}
	}

	if req.Type == milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup {
		// Check if all privileges are the same privilege level
		privilegeLevels := lo.SliceToMap(lo.Union(req.Privileges, currenctGroups[req.GroupName]), func(p *milvuspb.PrivilegeEntity) (string, struct{}) {
			return util.GetPrivilegeLevel(p.Name), struct{}{}
		})
		if len(privilegeLevels) > 1 {
			return merr.WrapErrParameterInvalidMsg("privileges are not the same privilege level")
		}
	}
	return nil
}

func (mt *MetaTable) OperatePrivilegeGroup(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

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
