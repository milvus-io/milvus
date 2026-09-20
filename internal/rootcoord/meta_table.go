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
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/rbacutil"
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

const rlsRecoveryConcurrency = 32

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
	// Deprecated: will be removed in the 3.0 after implementing ack sync up semantic.
	// It will be used to forbid the compaction of current collection when truncate collection operation is in progress.
	BeginTruncateCollection(ctx context.Context, collectionID UniqueID) error
	// TruncateCollection is called when the truncate collection message is acknowledged.
	TruncateCollection(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error
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
	AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
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

	PrepareCreateRLSPolicy(ctx context.Context, req *rlsutil.CreateRowPolicyRequest, policyID int64) (*model.RLSPolicy, error)
	PrepareUpdateRLSPolicy(ctx context.Context, req *rlsutil.UpdateRowPolicyRequest) (*model.RLSPolicy, error)
	PrepareDropRLSPolicy(ctx context.Context, req *rlsutil.DropRowPolicyRequest) (*model.RLSPolicy, error)
	ApplyAlterRLSPolicy(ctx context.Context, policy *model.RLSPolicy) error
	ApplyDropRLSPolicy(ctx context.Context, collectionID int64, policyName string) error
	ListRLSPolicies(ctx context.Context, req *rlsutil.ListRowPoliciesRequest) ([]*rlsutil.RowPolicy, error)
	PrepareSetRLSPrincipalTags(ctx context.Context, req *rlsutil.SetRLSPrincipalTagsRequest) (*model.RLSPrincipal, error)
	PrepareDeleteRLSPrincipalTags(ctx context.Context, req *rlsutil.DeleteRLSPrincipalTagsRequest) (*model.RLSPrincipal, bool, error)
	ApplyAlterRLSPrincipal(ctx context.Context, principal *model.RLSPrincipal) error
	ApplyDropRLSPrincipal(ctx context.Context, collectionID int64, principalName string) error
	GetRLSPrincipalTags(ctx context.Context, req *rlsutil.GetRLSPrincipalTagsRequest) (map[string]rlsutil.TagValue, error)
	ListRLSPrincipals(ctx context.Context, req *rlsutil.ListRLSPrincipalsRequest) ([]string, error)
	GetRLSMetadata(ctx context.Context, collectionID int64, kind rootcoordpb.RLSMetadataKind, principalName string) (*model.RLSMetadata, error)
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
		if err := mt.reloadCollectionsRLSMetadata(mt.ctx, collections); err != nil {
			return err
		}
		for _, collection := range collections {
			if collection.DBName != "" && collection.DBName != dbName {
				log.Ctx(mt.ctx).Warn(
					"collection dbname is not correct, it will be fixed",
					zap.Int64("collection_id", collection.CollectionID),
					zap.String("db_name", dbName),
					zap.String("collection_name", collection.Name),
					zap.String("collection_dbname", collection.DBName),
				)
			}
			collection.DBName = dbName // some collections may not have db name or its dbname is not correct, we should fix it here.
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
	if err := mt.reloadCollectionsRLSMetadata(mt.ctx, oldCollections); err != nil {
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
	// Generate ezID and db ts for default database
	// Use unique ID as ezID because the default dbID(1) for each cluster is the same
	ts, err := mt.tsoAllocator.GenerateTSO(2)
	if err != nil {
		return err
	}

	s := Params.RootCoordCfg.DefaultDBProperties.GetValue()
	defaultProperties, err := funcutil.String2KeyValuePair(s)
	if err != nil {
		return err
	}

	// Apply same encryption logic as regular database creation
	// This respects the defaultKey setting
	defaultProperties, err = hookutil.TidyDBCipherProperties(int64(ts-1), defaultProperties)
	if err != nil {
		return err
	}

	// Create EZ if encryption is enabled
	if err := hookutil.CreateEZByDBProperties(defaultProperties); err != nil {
		return err
	}

	return mt.createDatabasePrivate(mt.ctx, model.NewDefaultDatabase(defaultProperties), ts)
}

func (mt *MetaTable) CheckIfDatabaseCreatable(ctx context.Context, req *milvuspb.CreateDatabaseRequest) error {
	dbName := req.GetDbName()

	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if _, ok := mt.dbName2Meta[dbName]; ok || mt.aliases.exist(dbName) || mt.names.exist(dbName) {
		// TODO: idempotency check here.
		return merr.WrapErrParameterInvalidMsg("database already exist: %s", dbName)
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
	if err := mt.catalog.CreateDatabase(ctx, db, ts); err != nil {
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
		return merr.WrapErrParameterInvalidMsg("can not drop default database")
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
		return merr.WrapErrParameterInvalidMsg("database:%s not empty, must drop all collections before drop database", dbName)
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
	return nil, merr.WrapErrDatabaseNotFound(dbID)
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
		return merr.WrapErrServiceInternalMsg("collection state should be created, collection name: %s, collection id: %d, state: %s", coll.Name, coll.CollectionID, coll.State)
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
	log.Ctx(ctx).Info("update coll state to dropping",
		zap.Int64("collectionID", collectionID),
		zap.String("state", clone.State.String()),
	)

	db, err := mt.getDatabaseByIDInternal(ctx, coll.DBID, typeutil.MaxTimestamp)
	if err != nil {
		return merr.Wrapf(err, "dbID not found for collection:%d", collectionID)
	}

	pn := coll.GetPartitionNum(true)

	mt.generalCnt -= pn * int(coll.ShardsNum)
	channel.StaticPChannelStatsManager.MustGet().RemoveVChannel(coll.VirtualChannelNames...)
	metrics.RootCoordNumOfCollections.WithLabelValues(db.Name).Dec()
	metrics.RootCoordNumOfPartitions.WithLabelValues().Sub(float64(pn))

	log.Ctx(ctx).Info("drop collection from meta table", zap.Int64("collection", collectionID),
		zap.String("state", coll.State.String()), zap.Uint64("ts", ts))

	// Delete all grants referencing this collection immediately so they don't
	// linger until the tombstone sweeper runs (which can take minutes).
	if err := mt.catalog.DeleteGrantByCollectionName(ctx1, util.DefaultTenant, db.Name, coll.Name); err != nil {
		log.Ctx(ctx).Warn("failed to delete grants for dropped collection, skipping",
			zap.String("dbName", db.Name), zap.String("collectionName", coll.Name), zap.Error(err))
	}

	return nil
}

func (mt *MetaTable) removeIfNameMatchedInternal(ctx context.Context, collectionID UniqueID, name string) {
	mt.names.removeIf(func(db string, collection string, id UniqueID) bool {
		if collectionID == id {
			log.Ctx(ctx).Info("remove from names",
				zap.String("dbName", db),
				zap.String("collectionName", collection),
				zap.Int64("collectionID", id),
			)
			return true
		}
		return false
	})
}

func (mt *MetaTable) removeIfAliasMatchedInternal(ctx context.Context, collectionID UniqueID, alias string) {
	mt.aliases.removeIf(func(db string, collection string, id UniqueID) bool {
		if collectionID == id {
			log.Ctx(ctx).Info("remove from aliases",
				zap.String("dbName", db),
				zap.String("alias", collection),
				zap.Int64("collectionID", id),
			)
			return true
		}
		return false
	})
}

func (mt *MetaTable) removeIfMatchedInternal(ctx context.Context, collectionID UniqueID, name string) {
	mt.removeIfNameMatchedInternal(ctx, collectionID, name)
	mt.removeIfAliasMatchedInternal(ctx, collectionID, name)
}

func (mt *MetaTable) removeAllNamesIfMatchedInternal(ctx context.Context, collectionID UniqueID, names []string) {
	for _, name := range names {
		mt.removeIfMatchedInternal(ctx, collectionID, name)
	}
}

func (mt *MetaTable) removeCollectionByIDInternal(ctx context.Context, collectionID UniqueID) {
	delete(mt.collID2Meta, collectionID)
	log.Ctx(ctx).Info("delete from collID2Meta",
		zap.Int64("collectionID", collectionID),
	)
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
		return merr.WrapErrServiceInternalMsg("remove collection which state is not dropping, collectionID: %d, state: %s", collectionID, coll.State.String())
	}

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	aliases := mt.listAliasesByID(collectionID)
	newColl := &model.Collection{
		CollectionID:      collectionID,
		Partitions:        model.ClonePartitions(coll.Partitions),
		Fields:            model.CloneFields(coll.Fields),
		StructArrayFields: model.CloneStructArrayFields(coll.StructArrayFields),
		Functions:         model.CloneFunctions(coll.Functions),
		RLSPolicies:       model.CloneRLSPolicyMap(coll.RLSPolicies),
		RLSPrincipals:     model.CloneRLSPrincipals(coll.RLSPrincipals),
		Aliases:           aliases,
		DBID:              coll.DBID,
	}
	if err := mt.catalog.DropCollection(ctx1, newColl, ts); err != nil {
		return err
	}

	if err := mt.catalog.DeleteGrantByCollectionName(ctx1, util.DefaultTenant, coll.DBName, coll.Name); err != nil {
		log.Ctx(ctx).Warn("failed to delete grants for dropped collection, skipping",
			zap.String("dbName", coll.DBName), zap.String("collectionName", coll.Name), zap.Error(err))
	}

	allNames := common.CloneStringList(aliases)
	allNames = append(allNames, coll.Name)

	// We cannot delete the name directly, since newly collection with same name may be created.
	mt.removeAllNamesIfMatchedInternal(ctx, collectionID, allNames)
	mt.removeCollectionByIDInternal(ctx, collectionID)

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

	if oldColl.Name != newColl.Name || oldColl.DBName != newColl.DBName {
		if err := mt.catalog.MigrateGrantCollectionName(ctx1, util.DefaultTenant, oldColl.DBName, oldColl.Name, newColl.DBName, newColl.Name); err != nil {
			log.Ctx(ctx).Warn("failed to migrate grants for renamed collection, skipping",
				zap.String("oldDBName", oldColl.DBName), zap.String("oldName", oldColl.Name),
				zap.String("newDBName", newColl.DBName), zap.String("newName", newColl.Name), zap.Error(err))
		}
	}

	mt.names.remove(oldColl.DBName, oldColl.Name)
	mt.names.insert(newColl.DBName, newColl.Name, newColl.CollectionID)
	mt.collID2Meta[header.CollectionId] = newColl
	log.Ctx(ctx).Info("alter collection finished",
		zap.String("oldDBName", oldColl.DBName),
		zap.String("newDBName", newColl.DBName),
		zap.String("oldCollectionName", oldColl.Name),
		zap.String("newCollectionName", newColl.Name),
		zap.Int64("headerCollectionID", header.CollectionId),
		zap.Int64("newCollectionID", newColl.CollectionID),
		zap.Int64("oldCollectionID", oldColl.CollectionID),
		zap.Bool("dbChanged", dbChanged),
		zap.Uint64("ts", newColl.UpdateTimestamp),
	)
	return nil
}

func (mt *MetaTable) BeginTruncateCollection(ctx context.Context, collectionID UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return errAlterCollectionNotFound
	}

	// Apply the properties to override the existing properties.
	newProperties := common.CloneKeyValuePairs(coll.Properties).ToMap()
	key := common.CollectionOnTruncatingKey
	if _, ok := newProperties[key]; ok && newProperties[key] == "1" {
		return nil
	}
	newProperties[key] = "1"
	oldColl := coll.Clone()
	newColl := coll.Clone()
	newColl.Properties = common.NewKeyValuePairs(newProperties)

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterCollection(ctx1, oldColl, newColl, metastore.MODIFY, newColl.UpdateTimestamp, false); err != nil {
		return err
	}
	mt.collID2Meta[coll.CollectionID] = newColl
	return nil
}

func (mt *MetaTable) TruncateCollection(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collectionID := result.Message.Header().CollectionId
	coll, ok := mt.collID2Meta[collectionID]
	if !ok {
		return errAlterCollectionNotFound
	}

	oldColl := coll.Clone()

	// remmove the truncating key from the properties and update the last truncate time tick of the shard infos
	newColl := coll.Clone()
	newProperties := common.CloneKeyValuePairs(coll.Properties).ToMap()
	delete(newProperties, common.CollectionOnTruncatingKey)
	newColl.Properties = common.NewKeyValuePairs(newProperties)
	for vchannel := range newColl.ShardInfos {
		newColl.ShardInfos[vchannel].LastTruncateTimeTick = result.Results[vchannel].TimeTick
	}
	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterCollection(ctx1, oldColl, newColl, metastore.MODIFY, newColl.UpdateTimestamp, false); err != nil {
		return err
	}
	mt.collID2Meta[coll.CollectionID] = newColl
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
		return merr.WrapErrDatabaseNotFound(newDBName)
	}

	// old collection should not be an alias
	_, ok = mt.aliases.get(dbName, oldName)
	if ok {
		log.Warn("unsupported use a alias to rename collection")
		return merr.WrapErrParameterInvalidMsg("unsupported use an alias to rename collection, alias:%s", oldName)
	}

	_, ok = mt.aliases.get(newDBName, newName)
	if ok {
		log.Warn("cannot rename collection to an existing alias")
		return merr.WrapErrAsInputError(merr.WrapErrAliasCollectionNameConflict(newDBName, newName))
	}

	// check new collection already exists
	coll, err := mt.getCollectionByNameInternal(ctx, newDBName, newName, typeutil.MaxTimestamp)
	if coll != nil {
		log.Warn("duplicated new collection name, already taken by another collection or alias.")
		return merr.WrapErrParameterInvalidMsg("duplicated new collection name %s:%s with other collection name or alias", newDBName, newName)
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
		return merr.WrapErrParameterInvalidMsg("fail to rename db name, must drop all aliases of this collection before rename")
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
		return merr.WrapErrServiceInternalMsg("collection not exists: %d", partition.CollectionID)
	}

	if partition.State != pb.PartitionState_PartitionCreated {
		return merr.WrapErrServiceInternalMsg("partition state is not created, collection: %d, partition: %d, state: %s", partition.CollectionID, partition.PartitionID, partition.State)
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
		return merr.WrapErrServiceInternalMsg("remove partition which state is not dropping, collection: %d, partition: %d, state: %s", collectionID, partitionID, partition.State.String())
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
			return merr.WrapErrServiceInternalMsg("meta error, name mapped non-exist collection id")
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
		zap.String("collectionName", header.CollectionName),
		zap.Int64("collectionID", header.CollectionId),
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
		return merr.WrapErrParameterInvalidMsg("username is empty")
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
		return merr.WrapErrServiceQuotaExceeded(errMsg)
	}
	return nil
}

func (mt *MetaTable) CheckIfUpdateCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error {
	if funcutil.IsEmptyString(credInfo.GetUsername()) {
		return merr.WrapErrParameterInvalidMsg("username is empty")
	}
	hasEncryptedPassword := credInfo.GetEncryptedPassword() != ""
	hasSha256Password := credInfo.GetSha256Password() != ""
	if hasEncryptedPassword != hasSha256Password {
		return merr.WrapErrParameterInvalidMsg("credential password update must include both encrypted and sha256 password")
	}
	if !hasEncryptedPassword && !hasSha256Password && credInfo.Description == nil {
		return merr.WrapErrParameterInvalidMsg("credential update must change password or description")
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
		log.Ctx(ctx).Info("credential already exists and the version is not greater than the current timetick",
			zap.String("username", body.CredentialInfo.Username),
			zap.Uint64("incoming", result.GetControlChannelResult().TimeTick),
			zap.Uint64("current", existsCredential.TimeTick),
		)
		return nil
	}
	encryptedPassword := body.CredentialInfo.EncryptedPassword
	description := body.CredentialInfo.GetDescription()
	if existsCredential != nil {
		if encryptedPassword == "" {
			encryptedPassword = existsCredential.EncryptedPassword
		}
		if body.CredentialInfo.Description == nil {
			description = existsCredential.Description
		}
	}
	credential := &model.Credential{
		Username:          body.CredentialInfo.Username,
		EncryptedPassword: encryptedPassword,
		Description:       description,
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
		return merr.WrapErrParameterInvalidMsg("username is empty")
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
		log.Ctx(ctx).Info("credential already exists and the version is not greater than the current timetick",
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
		return nil, merr.Wrap(err, "failed to list credential usernames")
	}
	return &milvuspb.ListCredUsersResponse{Usernames: usernames}, nil
}

// CheckIfCreateRole checks if the role can be created.
func (mt *MetaTable) CheckIfCreateRole(ctx context.Context, in *milvuspb.CreateRoleRequest) error {
	if funcutil.IsEmptyString(in.GetEntity().GetName()) {
		return merr.WrapErrParameterInvalidMsg("role name is empty")
	}
	if err := validateRoleDescription(in.GetEntity().GetDescription()); err != nil {
		return err
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
		return merr.WrapErrServiceQuotaExceeded(errMsg)
	}
	return nil
}

// CreateRole create role
func (mt *MetaTable) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.CreateRole(ctx, tenant, entity)
}

func (mt *MetaTable) CheckIfAlterRole(ctx context.Context, in *milvuspb.AlterRoleRequest) error {
	if funcutil.IsEmptyString(in.GetRoleName()) {
		return merr.WrapErrParameterInvalidMsg("role name is empty")
	}
	if util.IsBuiltinRole(in.GetRoleName()) || lo.Contains(util.DefaultRoles, in.GetRoleName()) {
		return merr.WrapErrPrivilegeNotPermitted("the role[%s] is a builtin role, which can't be altered", in.GetRoleName())
	}
	if err := validateRoleDescription(in.GetDescription()); err != nil {
		return err
	}
	mt.permissionLock.RLock()
	defer mt.permissionLock.RUnlock()

	if _, err := mt.catalog.ListRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: in.GetRoleName()}, false); err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return errRoleNotExists
		}
		return err
	}
	return nil
}

func (mt *MetaTable) AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	if funcutil.IsEmptyString(entity.GetName()) {
		return merr.WrapErrParameterInvalidMsg("role name is empty")
	}
	if util.IsBuiltinRole(entity.GetName()) || lo.Contains(util.DefaultRoles, entity.GetName()) {
		return merr.WrapErrPrivilegeNotPermitted("the role[%s] is a builtin role, which can't be altered", entity.GetName())
	}
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	if _, err := mt.catalog.ListRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: entity.GetName()}, false); err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return errRoleNotExists
		}
		return err
	}
	return mt.catalog.AlterRole(ctx, tenant, entity)
}

func (mt *MetaTable) CheckIfDropRole(ctx context.Context, in *milvuspb.DropRoleRequest) error {
	if funcutil.IsEmptyString(in.GetRoleName()) {
		return merr.WrapErrParameterInvalidMsg("role name is empty")
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
		return merr.WrapErrParameterInvalidMsg(errMsg)
	}
	return nil
}

func validateRoleDescription(description string) error {
	return rbacutil.ValidateRoleDescription(description, Params.ProxyCfg.MaxRoleDescriptionLength.GetAsInt())
}

// DropRole drop role info
func (mt *MetaTable) DropRole(ctx context.Context, tenant string, roleName string) error {
	mt.permissionLock.Lock()
	defer mt.permissionLock.Unlock()

	return mt.catalog.DropRole(ctx, tenant, roleName)
}

func (mt *MetaTable) CheckIfOperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) error {
	if funcutil.IsEmptyString(req.GetUsername()) {
		return merr.WrapErrParameterInvalidMsg("username in the user entity is empty")
	}
	if funcutil.IsEmptyString(req.GetRoleName()) {
		return merr.WrapErrParameterInvalidMsg("role name in the role entity is empty")
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
			if errors.Is(err, merr.ErrIoKeyNotFound) {
				return merr.WrapErrParameterInvalidMsg("user %q not found", req.GetUsername())
			}
			return merr.Wrap(err, "failed to check user existence")
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
		return merr.WrapErrParameterInvalidMsg("the object name in the grant entity is empty")
	}
	if entity.Object == nil || funcutil.IsEmptyString(entity.Object.Name) {
		return merr.WrapErrParameterInvalidMsg("the object entity in the grant entity is invalid")
	}
	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return merr.WrapErrParameterInvalidMsg("the role entity in the grant entity is invalid")
	}
	if entity.Grantor == nil {
		return merr.WrapErrParameterInvalidMsg("the grantor in the grant entity is empty")
	}
	if entity.Grantor.Privilege == nil || funcutil.IsEmptyString(entity.Grantor.Privilege.Name) {
		return merr.WrapErrParameterInvalidMsg("the privilege name in the grant entity is empty")
	}
	if entity.Grantor.User == nil || funcutil.IsEmptyString(entity.Grantor.User.Name) {
		return merr.WrapErrParameterInvalidMsg("the grantor name in the grant entity is empty")
	}
	if !funcutil.IsRevoke(operateType) && !funcutil.IsGrant(operateType) {
		return merr.WrapErrParameterInvalidMsg("the operate type in the grant entity is invalid")
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
		return entities, merr.WrapErrParameterInvalidMsg("the grant entity is nil")
	}

	if entity.Role == nil || funcutil.IsEmptyString(entity.Role.Name) {
		return entities, merr.WrapErrParameterInvalidMsg("the role entity in the grant entity is invalid")
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
		return merr.WrapErrParameterInvalidMsg("the role entity is invalid when dropping the grant")
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
		if err := validateRoleDescription(role.GetDescription()); err != nil {
			return err
		}
		if _, ok := existRoleMap[role.GetName()]; ok {
			return merr.WrapErrParameterInvalidMsg("role [%s] already exists", role.GetName())
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
			return merr.WrapErrParameterInvalidMsg("privilege group [%s] already exists", group.GetGroupName())
		}
		existPrivGroupAfterRestoreMap[group.GetGroupName()] = struct{}{}
	}

	// check if grant can be restored
	for _, grant := range meta.GetGrants() {
		privName := grant.GetGrantor().GetPrivilege().GetName()
		if util.IsAnyWord(privName) {
			continue
		}
		if _, ok := existPrivGroupAfterRestoreMap[privName]; !ok && !util.IsPrivilegeNameDefined(privName) {
			return merr.WrapErrParameterInvalidMsg("privilege [%s] does not exist", privName)
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
			return merr.WrapErrParameterInvalidMsg("user [%s] already exists", user.GetUser())
		}

		// check if user-role can be restored
		for _, role := range user.GetRoles() {
			if _, ok := existRoleAfterRestoreMap[role.GetName()]; !ok {
				return merr.WrapErrParameterInvalidMsg("role [%s] does not exist", role.GetName())
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
		return merr.WrapErrParameterInvalidMsg("privilege group name is empty")
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
		return merr.WrapErrParameterInvalidMsg("privilege group name is empty")
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
				return merr.WrapErrParameterInvalidMsg("privilege group [%s] is used by role [%s], Use REVOKE API to revoke it first", req.GetGroupName(), role.GetName())
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
		return merr.WrapErrParameterInvalidMsg("privilege group name is empty")
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
		return merr.WrapErrParameterInvalidMsg("unsupported operate type: %v", operateType)
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
		return nil, merr.WrapErrParameterInvalidMsg("the privilege group name is empty")
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

func (mt *MetaTable) resolveRLSCollection(ctx context.Context, dbName string, collectionName string) (*model.Collection, error) {
	if funcutil.IsEmptyString(collectionName) {
		return nil, merr.WrapErrParameterInvalidMsg("collection name is empty")
	}
	if funcutil.IsEmptyString(dbName) {
		dbName = util.DefaultDBName
	}
	collection, err := mt.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	if err != nil {
		return nil, err
	}
	enabled, err := common.IsRLSEnabled(collection.Properties...)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid RLS properties for collection %d", collection.CollectionID)
	}
	if !enabled {
		return nil, merr.WrapErrParameterInvalidMsg(
			"RLS is not enabled for collection %q; set %s=true when creating the collection",
			collection.Name,
			common.RLSEnabledKey,
		)
	}
	return collection, nil
}

func (mt *MetaTable) reloadEnabledCollectionRLSMetadata(ctx context.Context, collection *model.Collection) error {
	policies, err := mt.catalog.ListRLSPolicies(ctx, collection.CollectionID)
	if err != nil {
		return merr.Wrapf(err, "failed to reload RLS policies for collection %d", collection.CollectionID)
	}
	principals, err := mt.catalog.ListRLSPrincipals(ctx, collection.CollectionID)
	if err != nil {
		return merr.Wrapf(err, "failed to reload RLS principals for collection %d", collection.CollectionID)
	}
	collection.RLSPolicies = model.RLSPolicyMapFromSlice(policies)
	collection.RLSPrincipals = model.CloneRLSPrincipals(principals)
	// RLS records are keyed by the globally unique collection ID, so their
	// persisted DB ID may be stale after a cross-database rename. Recover the
	// authoritative DB identity from the owning collection.
	for _, policy := range collection.RLSPolicies {
		if policy != nil {
			policy.DBID = collection.DBID
		}
	}
	for _, principal := range collection.RLSPrincipals {
		if principal != nil {
			principal.DBID = collection.DBID
		}
	}
	return nil
}

func (mt *MetaTable) reloadCollectionsRLSMetadata(ctx context.Context, collections []*model.Collection) error {
	enabledCollections := make([]*model.Collection, 0)
	for _, collection := range collections {
		if collection == nil {
			continue
		}
		enabled, err := common.IsRLSEnabled(collection.Properties...)
		if err != nil {
			return merr.WrapErrDataIntegrity(err, "invalid RLS properties for collection %d", collection.CollectionID)
		}
		if !enabled {
			collection.RLSPolicies = nil
			collection.RLSPrincipals = nil
			continue
		}
		enabledCollections = append(enabledCollections, collection)
	}

	if ctx == nil {
		ctx = context.TODO()
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(rlsRecoveryConcurrency)
	for _, collection := range enabledCollections {
		collection := collection
		group.Go(func() error {
			return mt.reloadEnabledCollectionRLSMetadata(groupCtx, collection)
		})
	}
	return group.Wait()
}

func upsertCollectionRLSPolicy(collection *model.Collection, policy *model.RLSPolicy) {
	if collection == nil || policy == nil {
		return
	}
	if collection.RLSPolicies == nil {
		collection.RLSPolicies = make(map[string]*model.RLSPolicy)
	}
	collection.RLSPolicies[policy.PolicyName] = model.CloneRLSPolicy(policy)
}

func removeCollectionRLSPolicy(collection *model.Collection, policyName string) {
	if collection == nil {
		return
	}
	delete(collection.RLSPolicies, policyName)
}

func upsertCollectionRLSPrincipal(collection *model.Collection, principal *model.RLSPrincipal) {
	if collection == nil || principal == nil {
		return
	}
	cloned := model.CloneRLSPrincipal(principal)
	for i, cached := range collection.RLSPrincipals {
		if cached != nil && cached.PrincipalName == principal.PrincipalName {
			collection.RLSPrincipals[i] = cloned
			return
		}
	}
	collection.RLSPrincipals = append(collection.RLSPrincipals, cloned)
}

func removeCollectionRLSPrincipal(collection *model.Collection, principalName string) {
	if collection == nil {
		return
	}
	collection.RLSPrincipals = lo.Filter(collection.RLSPrincipals, func(principal *model.RLSPrincipal, _ int) bool {
		return principal == nil || principal.PrincipalName != principalName
	})
}

func validateRLSPolicy(policyName string, policyType rlsutil.PolicyType, actions []rlsutil.PolicyAction, usingExpr string, checkExpr string) error {
	return rlsutil.ValidatePolicy(policyName, policyType, actions, usingExpr, checkExpr)
}

func validateRLSPolicyForUpdate(policyName string, policyType rlsutil.PolicyType, actions []rlsutil.PolicyAction, usingExpr string, checkExpr string) error {
	return rlsutil.ValidatePolicyForUpdate(policyName, policyType, actions, usingExpr, checkExpr)
}

func validateRLSPolicyDescription(description string) error {
	return rlsutil.ValidatePolicyDescription(description)
}

func validateRLSCombinedExpressionLength(policies []*model.RLSPolicy) error {
	maxExpressionLength := Params.ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt()
	actionSet := make(map[rlsutil.PolicyAction]struct{})
	for _, policy := range policies {
		if policy == nil {
			continue
		}
		for _, action := range policy.Actions {
			actionSet[action] = struct{}{}
		}
	}
	actions := make([]rlsutil.PolicyAction, 0, len(actionSet))
	for action := range actionSet {
		actions = append(actions, action)
	}
	sort.Slice(actions, func(i, j int) bool {
		return actions[i] < actions[j]
	})

	for _, action := range actions {
		for _, expression := range []struct {
			kind     string
			selector func(*model.RLSPolicy) string
			applies  func(rlsutil.PolicyAction) bool
		}{
			{
				kind:     "using",
				selector: func(policy *model.RLSPolicy) string { return policy.UsingExpr },
				applies:  rlsActionUsesUsingExpression,
			},
			{
				kind:     "check",
				selector: func(policy *model.RLSPolicy) string { return policy.CheckExpr },
				applies:  rlsActionUsesCheckExpression,
			},
		} {
			if !expression.applies(action) {
				continue
			}
			combined := combineRLSPolicyExpressions(policies, action, expression.selector)
			if len(combined) > maxExpressionLength {
				return merr.WrapErrServiceQuotaExceededMsg(
					"combined RLS %s expression for action %s exceeds max length %d",
					expression.kind,
					action.String(),
					maxExpressionLength,
				)
			}
		}
	}
	return nil
}

func rlsActionUsesUsingExpression(action rlsutil.PolicyAction) bool {
	switch action {
	case rlsutil.PolicyActionQuery,
		rlsutil.PolicyActionQueryIterator,
		rlsutil.PolicyActionSearch,
		rlsutil.PolicyActionSearchIterator,
		rlsutil.PolicyActionHybridSearch,
		rlsutil.PolicyActionDelete,
		rlsutil.PolicyActionUpsert:
		return true
	default:
		return false
	}
}

func rlsActionUsesCheckExpression(action rlsutil.PolicyAction) bool {
	return action == rlsutil.PolicyActionInsert || action == rlsutil.PolicyActionUpsert
}

func combineRLSPolicyExpressions(policies []*model.RLSPolicy, action rlsutil.PolicyAction, selector func(*model.RLSPolicy) string) string {
	permissiveExprs := make([]string, 0)
	restrictiveExprs := make([]string, 0)
	for _, policy := range policies {
		if policy == nil || !rlsPolicyMatchesAction(policy, action) {
			continue
		}
		expr := strings.TrimSpace(selector(policy))
		if expr == "" {
			continue
		}
		templateExpr := toRLSCombinedTemplateExpr(expr)
		switch policy.PolicyType {
		case rlsutil.PolicyTypePermissive:
			permissiveExprs = append(permissiveExprs, templateExpr)
		case rlsutil.PolicyTypeRestrictive:
			restrictiveExprs = append(restrictiveExprs, templateExpr)
		}
	}

	if len(permissiveExprs) == 0 {
		if len(restrictiveExprs) > 0 {
			return "false"
		}
		return ""
	}
	groups := []string{joinRLSPolicyExpressions(permissiveExprs, "or")}
	if len(restrictiveExprs) > 0 {
		groups = append(groups, joinRLSPolicyExpressions(restrictiveExprs, "and"))
	}
	return joinRLSPolicyExpressions(groups, "and")
}

func toRLSCombinedTemplateExpr(expr string) string {
	templateExpr, _, _ := funcutil.ConvertRLSTemplateVariables(strings.TrimSpace(expr))
	return templateExpr
}

func rlsPolicyMatchesAction(policy *model.RLSPolicy, action rlsutil.PolicyAction) bool {
	for _, policyAction := range policy.Actions {
		if policyAction == action {
			return true
		}
	}
	return false
}

func joinRLSPolicyExpressions(expressions []string, operator string) string {
	nonEmpty := make([]string, 0, len(expressions))
	for _, expression := range expressions {
		expression = strings.TrimSpace(expression)
		if expression != "" {
			nonEmpty = append(nonEmpty, "("+expression+")")
		}
	}
	return strings.Join(nonEmpty, " "+operator+" ")
}

func upsertRLSPolicyList(policies map[string]*model.RLSPolicy, replacement *model.RLSPolicy) []*model.RLSPolicy {
	prospective := model.RLSPolicyMapToSlice(policies)
	for index, policy := range prospective {
		if policy.PolicyName == replacement.PolicyName {
			prospective[index] = model.CloneRLSPolicy(replacement)
			return prospective
		}
	}
	return append(prospective, model.CloneRLSPolicy(replacement))
}

func validateRLSPolicyExpressions(coll *model.Collection, usingExpr string, checkExpr string) error {
	for _, expr := range []string{usingExpr, checkExpr} {
		_, _, tagVariables := funcutil.ConvertRLSTemplateVariables(expr)
		for tagKey := range tagVariables {
			if err := rlsutil.ValidateTagKeyWithLimit(tagKey); err != nil {
				return err
			}
		}
	}
	return validateRLSPolicyExpressionsWithSchema(coll.ToCollectionSchemaPB(), usingExpr, checkExpr)
}

func validateRLSPolicyExpressionsWithSchema(schema *schemapb.CollectionSchema, usingExpr string, checkExpr string) error {
	return validateRLSPolicyExpressionsWithSchemaOptions(schema, usingExpr, checkExpr, true)
}

func validateRLSPolicyExpressionsWithSchemaOptions(schema *schemapb.CollectionSchema, usingExpr string, checkExpr string, enforceArrayLiteralLimit bool) error {
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return merr.Wrap(err, "failed to build schema helper for RLS policy")
	}
	return validateRLSPolicyExpressionsWithSchemaHelper(schemaHelper, usingExpr, checkExpr, enforceArrayLiteralLimit)
}

func validateRLSPolicyExpressionsWithSchemaHelper(schemaHelper *typeutil.SchemaHelper, usingExpr string, checkExpr string, enforceArrayLiteralLimit bool) error {
	if err := validateRLSPolicyExpression(schemaHelper, "using", usingExpr, enforceArrayLiteralLimit); err != nil {
		return err
	}
	return validateRLSPolicyExpression(schemaHelper, "check", checkExpr, enforceArrayLiteralLimit)
}

func validateRLSPoliciesWithSchema(policies map[string]*model.RLSPolicy, schema *schemapb.CollectionSchema) error {
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return merr.Wrap(err, "failed to build schema helper for RLS policy")
	}
	for _, policy := range model.RLSPolicyMapToSlice(policies) {
		// Existing policies are grandfathered when refreshable creation quotas
		// are lowered. Schema DDL only checks whether their fields and expression
		// shapes remain compatible with the proposed schema.
		if err := validateRLSPolicyExpressionsWithSchemaHelper(schemaHelper, policy.UsingExpr, policy.CheckExpr, false); err != nil {
			return merr.Wrapf(err, "RLS policy %q is incompatible with schema change", policy.PolicyName)
		}
	}
	return nil
}

func collectRLSPolicyFieldRefs(coll *model.Collection) (map[int64][]string, error) {
	fieldRefs := make(map[int64][]string)
	schemaHelper, err := typeutil.CreateSchemaHelper(coll.ToCollectionSchemaPB())
	if err != nil {
		return nil, merr.Wrap(err, "failed to build schema helper for RLS policy dependency check")
	}
	for _, policy := range model.RLSPolicyMapToSlice(coll.RLSPolicies) {
		for _, expr := range []string{policy.UsingExpr, policy.CheckExpr} {
			refs, err := collectRLSPolicyExprFieldRefs(schemaHelper, expr)
			if err != nil {
				return nil, merr.Wrapf(err, "failed to inspect RLS policy %q dependencies", policy.PolicyName)
			}
			for fieldID := range refs {
				fieldRefs[fieldID] = append(fieldRefs[fieldID], policy.PolicyName)
			}
		}
	}
	return fieldRefs, nil
}

func collectRLSPolicyExprFieldRefs(schemaHelper *typeutil.SchemaHelper, expr string) (map[int64]struct{}, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	templateExpr, _, err := toRLSPolicyTemplateExpr(expr)
	if err != nil {
		return nil, err
	}
	visitorArgs := &planparserv2.ParserVisitorArgs{Timezone: schemaHelper.GetTimezone()}
	parsedExpr, err := planparserv2.ParseExprTemplate(schemaHelper, templateExpr, visitorArgs)
	if err != nil {
		return nil, err
	}
	fieldRefs := make(map[int64]struct{})
	collectRLSParsedExprFieldRefs(parsedExpr, fieldRefs)
	return fieldRefs, nil
}

func collectRLSParsedExprFieldRefs(expr *planpb.Expr, fieldRefs map[int64]struct{}) {
	if expr == nil {
		return
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_UnaryExpr:
		collectRLSParsedExprFieldRefs(node.UnaryExpr.GetChild(), fieldRefs)
	case *planpb.Expr_BinaryExpr:
		collectRLSParsedExprFieldRefs(node.BinaryExpr.GetLeft(), fieldRefs)
		collectRLSParsedExprFieldRefs(node.BinaryExpr.GetRight(), fieldRefs)
	case *planpb.Expr_UnaryRangeExpr:
		addRLSColumnFieldRef(node.UnaryRangeExpr.GetColumnInfo(), fieldRefs)
	case *planpb.Expr_TermExpr:
		addRLSColumnFieldRef(node.TermExpr.GetColumnInfo(), fieldRefs)
	case *planpb.Expr_JsonContainsExpr:
		addRLSColumnFieldRef(node.JsonContainsExpr.GetColumnInfo(), fieldRefs)
	case *planpb.Expr_BinaryRangeExpr:
		addRLSColumnFieldRef(node.BinaryRangeExpr.GetColumnInfo(), fieldRefs)
	}
}

func addRLSColumnFieldRef(column *planpb.ColumnInfo, fieldRefs map[int64]struct{}) {
	if column == nil {
		return
	}
	fieldRefs[column.GetFieldId()] = struct{}{}
}

func validateRLSNoReferencedFieldDropped(coll *model.Collection, droppedFieldIDs []int64) error {
	if len(droppedFieldIDs) == 0 {
		return nil
	}
	fieldRefs, err := collectRLSPolicyFieldRefs(coll)
	if err != nil {
		return err
	}
	for _, fieldID := range droppedFieldIDs {
		if policies := fieldRefs[fieldID]; len(policies) > 0 {
			sort.Strings(policies)
			return merr.WrapErrParameterInvalidMsg("field %d is referenced by RLS policies %v and cannot be dropped", fieldID, policies)
		}
	}
	return nil
}

func validateRLSFunctionOutputNotReferenced(coll *model.Collection, fn *model.Function, operation string) error {
	if fn == nil || len(fn.OutputFieldIDs) == 0 {
		return nil
	}
	fieldRefs, err := collectRLSPolicyFieldRefs(coll)
	if err != nil {
		return err
	}
	for _, fieldID := range fn.OutputFieldIDs {
		if policies := fieldRefs[fieldID]; len(policies) > 0 {
			sort.Strings(policies)
			return merr.WrapErrParameterInvalidMsg("function %q cannot be %s because output field %d is referenced by RLS policies %v", fn.Name, operation, fieldID, policies)
		}
	}
	return nil
}

func findRLSFunctionByName(coll *model.Collection, functionName string) *model.Function {
	for _, fn := range coll.Functions {
		if fn.Name == functionName {
			return fn
		}
	}
	return nil
}

func rlsFunctionKeepsOutputShape(oldFn *model.Function, newFn *model.Function) bool {
	if oldFn == nil || newFn == nil {
		return false
	}
	if oldFn.Type != newFn.Type {
		return false
	}
	if len(oldFn.OutputFieldIDs) != len(newFn.OutputFieldIDs) {
		return false
	}
	for i := range oldFn.OutputFieldIDs {
		if oldFn.OutputFieldIDs[i] != newFn.OutputFieldIDs[i] {
			return false
		}
	}
	return true
}

func validateRLSPolicyExpression(schemaHelper *typeutil.SchemaHelper, exprKind string, expr string, enforceArrayLiteralLimit bool) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}
	templateExpr, allowedTemplateVariables, err := toRLSPolicyTemplateExpr(expr)
	if err != nil {
		return err
	}
	visitorArgs := &planparserv2.ParserVisitorArgs{Timezone: schemaHelper.GetTimezone()}
	parsedExpr, err := planparserv2.ParseExprTemplate(schemaHelper, templateExpr, visitorArgs)
	if err != nil {
		return merr.WrapErrParameterInvalidErr(err, "invalid RLS %s expression", exprKind)
	}
	if err := validateRLSParsedExpr(parsedExpr, allowedTemplateVariables); err != nil {
		return merr.Wrapf(err, "invalid RLS %s expression", exprKind)
	}
	if enforceArrayLiteralLimit {
		if err := validateRLSArrayLiteralLimit(parsedExpr); err != nil {
			return merr.Wrapf(err, "invalid RLS %s expression", exprKind)
		}
	}
	return nil
}

func toRLSPolicyTemplateExpr(expr string) (string, map[string]struct{}, error) {
	if err := rejectRawRLSTemplatePlaceholders(expr); err != nil {
		return "", nil, err
	}
	templateExpr, needsPrincipal, tagVariables := funcutil.ConvertRLSTemplateVariables(expr)
	allowedTemplateVariables := make(map[string]struct{}, len(tagVariables)+1)
	if needsPrincipal {
		allowedTemplateVariables[funcutil.RLSPrincipalTemplateName] = struct{}{}
	}
	for tagKey, templateVariable := range tagVariables {
		if err := validateRLSTagKey(tagKey); err != nil {
			return "", nil, err
		}
		allowedTemplateVariables[templateVariable] = struct{}{}
	}
	return templateExpr, allowedTemplateVariables, nil
}

func rejectRawRLSTemplatePlaceholders(expr string) error {
	// RLS adds its generated template variables after this check. Braces in
	// string literals are data; braces outside literals are user-supplied raw
	// template syntax that Proxy cannot populate at runtime.
	var quote byte
	escaped := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}

		switch ch {
		case '\'', '"':
			quote = ch
		case '{', '}':
			return merr.WrapErrParameterInvalidMsg("RLS policy expressions do not support raw template placeholders; use $current_principal or $current_principal_tags['key']")
		}
	}
	return nil
}

func validateRLSParsedExpr(expr *planpb.Expr, allowedTemplateVariables map[string]struct{}) error {
	if expr == nil {
		return merr.WrapErrParameterInvalidMsg("RLS expression is empty")
	}
	if rewriter.IsAlwaysFalseExpr(expr) {
		return nil
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return nil
	case *planpb.Expr_ValueExpr:
		if _, ok := node.ValueExpr.GetValue().GetVal().(*planpb.GenericValue_BoolVal); !ok {
			return merr.WrapErrParameterInvalidMsg("RLS value expression must be boolean")
		}
		return nil
	case *planpb.Expr_UnaryExpr:
		return merr.WrapErrParameterInvalidMsg("compound RLS expressions are not supported for RLS policy validation")
	case *planpb.Expr_UnaryRangeExpr:
		return validateRLSUnaryRangeExpr(node.UnaryRangeExpr, allowedTemplateVariables)
	case *planpb.Expr_TermExpr:
		return validateRLSTermExpr(node.TermExpr, allowedTemplateVariables)
	case *planpb.Expr_JsonContainsExpr:
		return validateRLSJSONContainsExpr(node.JsonContainsExpr, allowedTemplateVariables)
	case *planpb.Expr_BinaryExpr:
		return merr.WrapErrParameterInvalidMsg("compound RLS expressions are not supported for RLS policy validation")
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported RLS policy expression node %T", node)
	}
}

func validateRLSUnaryRangeExpr(expr *planpb.UnaryRangeExpr, allowedTemplateVariables map[string]struct{}) error {
	if expr.GetOp() != planpb.OpType_Equal {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports equality comparison")
	}
	if expr.GetValue() == nil && expr.GetTemplateVariableName() == "" {
		return merr.WrapErrParameterInvalidMsg("RLS equality comparison requires a value or principal variable")
	}
	if err := validateRLSTemplateVariable(expr.GetTemplateVariableName(), allowedTemplateVariables); err != nil {
		return err
	}
	if err := validateRLSScalarColumn(expr.GetColumnInfo()); err != nil {
		return err
	}
	if expr.GetTemplateVariableName() == funcutil.RLSPrincipalTemplateName && !typeutil.IsStringType(expr.GetColumnInfo().GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS current principal can only be compared with string fields")
	}
	return nil
}

func validateRLSTermExpr(expr *planpb.TermExpr, allowedTemplateVariables map[string]struct{}) error {
	if expr.GetIsInField() {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support field-to-field IN")
	}
	if err := validateRLSTemplateVariable(expr.GetTemplateVariableName(), allowedTemplateVariables); err != nil {
		return err
	}
	if expr.GetTemplateVariableName() != "" {
		return merr.WrapErrParameterInvalidMsg("RLS principal variables cannot be used as IN-list templates")
	}
	return validateRLSScalarColumn(expr.GetColumnInfo())
}

func validateRLSJSONContainsExpr(expr *planpb.JSONContainsExpr, allowedTemplateVariables map[string]struct{}) error {
	if err := validateRLSTemplateVariable(expr.GetTemplateVariableName(), allowedTemplateVariables); err != nil {
		return err
	}
	column := expr.GetColumnInfo()
	if err := validateRLSTopLevelColumn(column); err != nil {
		return err
	}
	if !typeutil.IsArrayType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports array_contains on array fields")
	}
	if !typeutil.IsPrimitiveType(column.GetElementType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports primitive array element fields")
	}
	switch expr.GetOp() {
	case planpb.JSONContainsExpr_Contains:
	case planpb.JSONContainsExpr_ContainsAll, planpb.JSONContainsExpr_ContainsAny:
		if expr.GetTemplateVariableName() != "" {
			return merr.WrapErrParameterInvalidMsg("RLS principal variables can only be used with array_contains")
		}
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported RLS array_contains operator %s", expr.GetOp().String())
	}
	if expr.GetTemplateVariableName() == funcutil.RLSPrincipalTemplateName && !typeutil.IsStringType(column.GetElementType()) {
		return merr.WrapErrParameterInvalidMsg("RLS current principal can only be compared with string array fields")
	}
	return nil
}

func validateRLSArrayLiteralLimit(expr *planpb.Expr) error {
	maxElements := Params.ProxyCfg.RLSMaxArrayLiteralElements.GetAsInt()
	var elements int
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_TermExpr:
		elements = len(node.TermExpr.GetValues())
	case *planpb.Expr_JsonContainsExpr:
		elements = len(node.JsonContainsExpr.GetElements())
	}
	if elements > maxElements {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression exceeds max array literal elements %d", maxElements)
	}
	return nil
}

func validateRLSTemplateVariable(templateVariable string, allowedTemplateVariables map[string]struct{}) error {
	if templateVariable == "" {
		return nil
	}
	if _, ok := allowedTemplateVariables[templateVariable]; !ok {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression contains unsupported template variable %q", templateVariable)
	}
	return nil
}

func validateRLSScalarColumn(column *planpb.ColumnInfo) error {
	if err := validateRLSTopLevelColumn(column); err != nil {
		return err
	}
	if !typeutil.IsPrimitiveType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression only supports top-level scalar fields")
	}
	return nil
}

func validateRLSTopLevelColumn(column *planpb.ColumnInfo) error {
	if column == nil {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression has empty column info")
	}
	if common.IsSystemField(column.GetFieldId()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support system fields")
	}
	if len(column.GetNestedPath()) > 0 {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support nested or element-level fields")
	}
	if typeutil.IsVectorType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support vector fields")
	}
	if typeutil.IsJSONType(column.GetDataType()) {
		return merr.WrapErrParameterInvalidMsg("RLS policy expression does not support JSON fields")
	}
	return nil
}

func (mt *MetaTable) PrepareCreateRLSPolicy(ctx context.Context, req *rlsutil.CreateRowPolicyRequest, policyID int64) (*model.RLSPolicy, error) {
	if req == nil {
		return nil, merr.WrapErrParameterInvalidMsg("create RLS policy request is nil")
	}
	if err := rlsutil.ValidateRequestTarget(req.GetDbName(), req.GetCollectionName()); err != nil {
		return nil, err
	}
	// Validate only the stable lookup bound before checking uniqueness. A
	// duplicate name is rejected regardless of whether its definition matches
	// or refreshable creation limits have changed since the original create.
	if err := rlsutil.ValidatePolicyName(req.GetPolicyName()); err != nil {
		return nil, err
	}

	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	if _, ok := coll.RLSPolicies[req.GetPolicyName()]; ok {
		return nil, merr.WrapErrParameterInvalidMsg("RLS policy [%s] already exists", req.GetPolicyName())
	}

	if err := validateRLSPolicy(req.GetPolicyName(), req.GetPolicyType(), req.GetActions(), req.GetUsingExpr(), req.GetCheckExpr()); err != nil {
		return nil, err
	}
	if err := validateRLSPolicyDescription(req.GetDescription()); err != nil {
		return nil, err
	}
	if err := validateRLSPolicyExpressions(coll, req.GetUsingExpr(), req.GetCheckExpr()); err != nil {
		return nil, err
	}

	policies := coll.RLSPolicies
	if len(policies) >= Params.ProxyCfg.RLSMaxPoliciesPerCollection.GetAsInt() {
		return nil, merr.WrapErrServiceQuotaExceeded("unable to create RLS policy because the number of policies has reached the limit")
	}

	policy := &model.RLSPolicy{
		DBID:         coll.DBID,
		CollectionID: coll.CollectionID,
		PolicyID:     policyID,
		PolicyName:   req.GetPolicyName(),
		PolicyType:   req.GetPolicyType(),
		Actions:      slices.Clone(req.GetActions()),
		UsingExpr:    req.GetUsingExpr(),
		CheckExpr:    req.GetCheckExpr(),
		Description:  req.GetDescription(),
	}
	if err := validateRLSCombinedExpressionLength(upsertRLSPolicyList(policies, policy)); err != nil {
		return nil, err
	}
	return policy, nil
}

func (mt *MetaTable) PrepareUpdateRLSPolicy(ctx context.Context, req *rlsutil.UpdateRowPolicyRequest) (*model.RLSPolicy, error) {
	if req == nil {
		return nil, merr.WrapErrParameterInvalidMsg("update RLS policy request is nil")
	}
	if err := validateRLSPolicyForUpdate(req.GetPolicyName(), req.GetPolicyType(), req.GetActions(), req.GetUsingExpr(), req.GetCheckExpr()); err != nil {
		return nil, err
	}
	if err := validateRLSPolicyDescription(req.GetDescription()); err != nil {
		return nil, err
	}

	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}
	if err := validateRLSPolicyExpressions(coll, req.GetUsingExpr(), req.GetCheckExpr()); err != nil {
		return nil, err
	}

	oldPolicy, ok := coll.RLSPolicies[req.GetPolicyName()]
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("RLS policy [%s] does not exist", req.GetPolicyName())
	}

	policy := &model.RLSPolicy{
		DBID:         coll.DBID,
		CollectionID: coll.CollectionID,
		PolicyID:     oldPolicy.PolicyID,
		PolicyName:   req.GetPolicyName(),
		PolicyType:   req.GetPolicyType(),
		Actions:      slices.Clone(req.GetActions()),
		UsingExpr:    req.GetUsingExpr(),
		CheckExpr:    req.GetCheckExpr(),
		Description:  req.GetDescription(),
	}
	return policy, nil
}

func (mt *MetaTable) PrepareDropRLSPolicy(ctx context.Context, req *rlsutil.DropRowPolicyRequest) (*model.RLSPolicy, error) {
	if req == nil {
		return nil, merr.WrapErrParameterInvalidMsg("drop RLS policy request is nil")
	}
	if err := rlsutil.ValidatePolicyName(req.GetPolicyName()); err != nil {
		return nil, err
	}

	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	policy, ok := coll.RLSPolicies[req.GetPolicyName()]
	if !ok {
		return &model.RLSPolicy{
			DBID:         coll.DBID,
			CollectionID: coll.CollectionID,
			PolicyName:   req.GetPolicyName(),
		}, nil
	}
	policy = model.CloneRLSPolicy(policy)
	policy.DBID = coll.DBID
	policy.CollectionID = coll.CollectionID
	return policy, nil
}

func (mt *MetaTable) ApplyAlterRLSPolicy(ctx context.Context, policy *model.RLSPolicy) error {
	if policy == nil {
		return merr.WrapErrServiceInternalMsg("RLS policy is nil")
	}
	if err := mt.catalog.SaveRLSPolicy(ctx, policy); err != nil {
		return merr.Wrap(err, "failed to save RLS policy")
	}
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	upsertCollectionRLSPolicy(mt.collID2Meta[policy.CollectionID], policy)
	return nil
}

func (mt *MetaTable) ApplyDropRLSPolicy(ctx context.Context, collectionID int64, policyName string) error {
	mt.ddLock.RLock()
	collection := mt.collID2Meta[collectionID]
	var policy *model.RLSPolicy
	if collection != nil {
		policy = model.CloneRLSPolicy(collection.RLSPolicies[policyName])
	}
	mt.ddLock.RUnlock()
	if policy == nil {
		return nil
	}

	if err := mt.catalog.DropRLSPolicy(ctx, collectionID, policy.PolicyID); err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		return merr.Wrap(err, "failed to drop RLS policy")
	}
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	collection = mt.collID2Meta[collectionID]
	if collection != nil {
		current := collection.RLSPolicies[policyName]
		if current != nil && current.PolicyID == policy.PolicyID {
			removeCollectionRLSPolicy(collection, policyName)
		}
	}
	return nil
}

func (mt *MetaTable) ListRLSPolicies(ctx context.Context, req *rlsutil.ListRowPoliciesRequest) ([]*rlsutil.RowPolicy, error) {
	if req == nil {
		return nil, merr.WrapErrParameterInvalidMsg("list RLS policies request is nil")
	}
	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	policyModels := model.RLSPolicyMapToSlice(coll.RLSPolicies)
	policies := make([]*rlsutil.RowPolicy, 0, len(policyModels))
	for _, policy := range policyModels {
		policies = append(policies, policy.ToRowPolicy())
	}
	return policies, nil
}

func (mt *MetaTable) GetRLSMetadata(ctx context.Context, collectionID int64, kind rootcoordpb.RLSMetadataKind, principalName string) (*model.RLSMetadata, error) {
	if collectionID == 0 {
		return nil, merr.WrapErrServiceInternalMsg("failed to get RLS metadata with empty collection id")
	}

	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	coll := mt.collID2Meta[collectionID]
	if coll == nil || !coll.Available() {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	metadata := &model.RLSMetadata{
		CollectionID: coll.CollectionID,
	}
	switch kind {
	case rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_ALL:
		if principalName != "" {
			return nil, merr.WrapErrServiceInternalMsg("RLS principal filter is only supported for principal metadata")
		}
		metadata.Policies = model.RLSPolicyMapToSlice(coll.RLSPolicies)
		metadata.Principals = model.CloneRLSPrincipals(coll.RLSPrincipals)
	case rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_POLICIES:
		if principalName != "" {
			return nil, merr.WrapErrServiceInternalMsg("RLS principal filter is only supported for principal metadata")
		}
		metadata.Policies = model.RLSPolicyMapToSlice(coll.RLSPolicies)
	case rootcoordpb.RLSMetadataKind_RLS_METADATA_KIND_PRINCIPALS:
		if principalName == "" {
			metadata.Principals = model.CloneRLSPrincipals(coll.RLSPrincipals)
			break
		}
		for _, principal := range coll.RLSPrincipals {
			if principal.PrincipalName == principalName {
				metadata.Principals = []*model.RLSPrincipal{model.CloneRLSPrincipal(principal)}
				break
			}
		}
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported RLS metadata kind %s", kind.String())
	}
	return metadata, nil
}

func validateRLSPrincipalName(principalName string) error {
	return rlsutil.ValidatePrincipalName(principalName)
}

func validateRLSPrincipalNameForSet(principalName string) error {
	return rlsutil.ValidatePrincipalNameWithLimit(principalName)
}

func validateRLSTagKey(tagKey string) error {
	return rlsutil.ValidateTagKey(tagKey)
}

func validateAndDeduplicateRLSTagKeys(tagKeys []string) ([]string, error) {
	return rlsutil.ValidateAndDeduplicateTagKeys(tagKeys)
}

func cloneRLSTags(tags map[string]rlsutil.TagValue) map[string]rlsutil.TagValue {
	if tags == nil {
		return nil
	}
	cloned := make(map[string]rlsutil.TagValue, len(tags))
	for key, value := range tags {
		cloned[key] = value
	}
	return cloned
}

func (mt *MetaTable) PrepareSetRLSPrincipalTags(ctx context.Context, req *rlsutil.SetRLSPrincipalTagsRequest) (*model.RLSPrincipal, error) {
	if req == nil {
		return nil, merr.WrapErrParameterInvalidMsg("set RLS principal tags request is nil")
	}
	if err := validateRLSPrincipalName(req.GetPrincipalName()); err != nil {
		return nil, err
	}
	if err := rlsutil.ValidateTags(req.GetTags()); err != nil {
		return nil, err
	}

	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	existingPrincipal, err := mt.catalog.GetRLSPrincipal(ctx, coll.CollectionID, req.GetPrincipalName())
	isNew := false
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		isNew = true
		if err := validateRLSPrincipalNameForSet(req.GetPrincipalName()); err != nil {
			return nil, err
		}
		if len(coll.RLSPrincipals) >= Params.ProxyCfg.RLSMaxPrincipalsPerCollection.GetAsInt() {
			return nil, merr.WrapErrServiceQuotaExceeded("unable to create RLS principal because the number of principals has reached the limit")
		}
	} else if err != nil {
		return nil, merr.Wrap(err, "failed to get RLS principal")
	}

	mergedTags := cloneRLSTags(req.GetTags())
	if !isNew {
		mergedTags = cloneRLSTags(existingPrincipal.Tags)
		if mergedTags == nil {
			mergedTags = make(map[string]rlsutil.TagValue, len(req.GetTags()))
		}
		for key, value := range req.GetTags() {
			mergedTags[key] = value
		}
		if len(mergedTags) > Params.ProxyCfg.RLSMaxTagsPerPrincipal.GetAsInt() {
			return nil, merr.WrapErrServiceQuotaExceeded("unable to set RLS principal tags because the number of tags has reached the limit")
		}
	}

	principal := &model.RLSPrincipal{
		DBID:          coll.DBID,
		CollectionID:  coll.CollectionID,
		PrincipalName: req.GetPrincipalName(),
		Tags:          mergedTags,
	}
	return principal, nil
}

func (mt *MetaTable) GetRLSPrincipalTags(ctx context.Context, req *rlsutil.GetRLSPrincipalTagsRequest) (map[string]rlsutil.TagValue, error) {
	if req == nil {
		return nil, merr.WrapErrParameterInvalidMsg("get RLS principal tags request is nil")
	}
	if err := validateRLSPrincipalName(req.GetPrincipalName()); err != nil {
		return nil, err
	}
	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	principal, err := mt.catalog.GetRLSPrincipal(ctx, coll.CollectionID, req.GetPrincipalName())
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, merr.WrapErrParameterInvalidMsg("RLS principal [%s] does not exist", req.GetPrincipalName())
		}
		return nil, merr.Wrap(err, "failed to get RLS principal")
	}
	return cloneRLSTags(principal.Tags), nil
}

func (mt *MetaTable) ListRLSPrincipals(ctx context.Context, req *rlsutil.ListRLSPrincipalsRequest) ([]string, error) {
	if req == nil {
		return nil, merr.WrapErrParameterInvalidMsg("list RLS principals request is nil")
	}
	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, err
	}

	names := lo.Map(coll.RLSPrincipals, func(principal *model.RLSPrincipal, _ int) string {
		return principal.PrincipalName
	})
	sort.Strings(names)
	return names, nil
}

func (mt *MetaTable) PrepareDeleteRLSPrincipalTags(ctx context.Context, req *rlsutil.DeleteRLSPrincipalTagsRequest) (*model.RLSPrincipal, bool, error) {
	if req == nil {
		return nil, false, merr.WrapErrParameterInvalidMsg("delete RLS principal tags request is nil")
	}
	if err := validateRLSPrincipalName(req.GetPrincipalName()); err != nil {
		return nil, false, err
	}
	tagKeys, err := validateAndDeduplicateRLSTagKeys(req.GetTagKeys())
	if err != nil {
		return nil, false, err
	}

	coll, err := mt.resolveRLSCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return nil, false, err
	}

	principal, err := mt.catalog.GetRLSPrincipal(ctx, coll.CollectionID, req.GetPrincipalName())
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return &model.RLSPrincipal{
				DBID:          coll.DBID,
				CollectionID:  coll.CollectionID,
				PrincipalName: req.GetPrincipalName(),
			}, true, nil
		}
		return nil, false, merr.Wrap(err, "failed to get RLS principal")
	}
	if len(tagKeys) == 0 {
		principal = model.CloneRLSPrincipal(principal)
		principal.DBID = coll.DBID
		principal.CollectionID = coll.CollectionID
		return principal, true, nil
	}

	tags := cloneRLSTags(principal.Tags)
	for _, key := range tagKeys {
		delete(tags, key)
	}
	if len(tags) == 0 {
		principal = model.CloneRLSPrincipal(principal)
		principal.DBID = coll.DBID
		principal.CollectionID = coll.CollectionID
		return principal, true, nil
	}
	principal = model.CloneRLSPrincipal(principal)
	principal.DBID = coll.DBID
	principal.CollectionID = coll.CollectionID
	principal.Tags = tags
	return principal, false, nil
}

func (mt *MetaTable) ApplyAlterRLSPrincipal(ctx context.Context, principal *model.RLSPrincipal) error {
	if principal == nil {
		return merr.WrapErrServiceInternalMsg("RLS principal is nil")
	}
	if err := mt.catalog.SaveRLSPrincipal(ctx, principal); err != nil {
		return merr.Wrap(err, "failed to save RLS principal")
	}
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	upsertCollectionRLSPrincipal(mt.collID2Meta[principal.CollectionID], principal)
	return nil
}

func (mt *MetaTable) ApplyDropRLSPrincipal(ctx context.Context, collectionID int64, principalName string) error {
	if err := mt.catalog.DropRLSPrincipal(ctx, collectionID, principalName); err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		return merr.Wrap(err, "failed to drop RLS principal")
	}
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	removeCollectionRLSPrincipal(mt.collID2Meta[collectionID], principalName)
	return nil
}
