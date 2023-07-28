package rootcoord

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/contextutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Catalog struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

func NewTableCatalog(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain) *Catalog {
	return &Catalog{
		txImpl:     txImpl,
		metaDomain: metaDomain,
	}
}

func (tc *Catalog) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	//TODO
	return nil
}

func (tc *Catalog) DropDatabase(ctx context.Context, dbID int64, ts typeutil.Timestamp) error {
	//TODO
	return nil

}

func (tc *Catalog) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	//TODO
	return make([]*model.Database, 0), nil
}

func (tc *Catalog) CreateCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert collection
		var startPositionsStr string
		if collection.StartPositions != nil {
			startPositionsBytes, err := json.Marshal(collection.StartPositions)
			if err != nil {
				log.Error("marshal collection start positions error", zap.Int64("collectionID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
				return err
			}
			startPositionsStr = string(startPositionsBytes)
		}

		properties, err := dbmodel.MarshalProperties(collection.Properties)
		if err != nil {
			return err
		}

		err = tc.metaDomain.CollectionDb(txCtx).Insert(&dbmodel.Collection{
			TenantID:         tenantID,
			CollectionID:     collection.CollectionID,
			CollectionName:   collection.Name,
			Description:      collection.Description,
			AutoID:           collection.AutoID,
			ShardsNum:        collection.ShardsNum,
			StartPosition:    startPositionsStr,
			ConsistencyLevel: int32(collection.ConsistencyLevel),
			Status:           int32(collection.State),
			Ts:               ts,
			Properties:       properties,
		})
		if err != nil {
			return err
		}

		// insert field
		var fields = make([]*dbmodel.Field, 0, len(collection.Fields))
		for _, field := range collection.Fields {
			typeParamsBytes, err := json.Marshal(field.TypeParams)
			if err != nil {
				log.Error("marshal TypeParams of field failed", zap.Error(err))
				return err
			}
			typeParamsStr := string(typeParamsBytes)

			indexParamsBytes, err := json.Marshal(field.IndexParams)
			if err != nil {
				log.Error("marshal IndexParams of field failed", zap.Error(err))
				return err
			}
			indexParamsStr := string(indexParamsBytes)

			f := &dbmodel.Field{
				TenantID:     collection.TenantID,
				FieldID:      field.FieldID,
				FieldName:    field.Name,
				IsPrimaryKey: field.IsPrimaryKey,
				Description:  field.Description,
				DataType:     field.DataType,
				TypeParams:   typeParamsStr,
				IndexParams:  indexParamsStr,
				AutoID:       field.AutoID,
				CollectionID: collection.CollectionID,
				Ts:           ts,
			}

			fields = append(fields, f)
		}

		err = tc.metaDomain.FieldDb(txCtx).Insert(fields)
		if err != nil {
			return err
		}

		// insert partition
		var partitions = make([]*dbmodel.Partition, 0, len(collection.Partitions))
		for _, partition := range collection.Partitions {
			p := &dbmodel.Partition{
				TenantID:                  collection.TenantID,
				PartitionID:               partition.PartitionID,
				PartitionName:             partition.PartitionName,
				PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
				CollectionID:              collection.CollectionID,
				Ts:                        ts,
			}
			partitions = append(partitions, p)
		}

		err = tc.metaDomain.PartitionDb(txCtx).Insert(partitions)
		if err != nil {
			return err
		}

		// insert channel
		var channels = make([]*dbmodel.CollectionChannel, 0, len(collection.VirtualChannelNames))
		for i, vChannelName := range collection.VirtualChannelNames {
			collChannel := &dbmodel.CollectionChannel{
				TenantID:            collection.TenantID,
				CollectionID:        collection.CollectionID,
				VirtualChannelName:  vChannelName,
				PhysicalChannelName: collection.PhysicalChannelNames[i],
				Ts:                  ts,
			}
			channels = append(channels, collChannel)
		}

		err = tc.metaDomain.CollChannelDb(txCtx).Insert(channels)
		if err != nil {
			return err
		}

		return nil
	})
}

func (tc *Catalog) GetCollectionByID(ctx context.Context, dbID int64, ts typeutil.Timestamp, collectionID typeutil.UniqueID) (*model.Collection, error) {
	tenantID := contextutil.TenantID(ctx)

	// get latest timestamp less than or equals to param ts
	cidTsPair, err := tc.metaDomain.CollectionDb(ctx).GetCollectionIDTs(tenantID, collectionID, ts)
	if err != nil {
		return nil, err
	}
	if cidTsPair.IsDeleted {
		log.Error("not found collection", zap.Int64("collectionID", collectionID), zap.Uint64("ts", ts))
		return nil, fmt.Errorf("not found collection, collID=%d, ts=%d", collectionID, ts)
	}

	queryTs := cidTsPair.Ts

	return tc.populateCollection(ctx, collectionID, queryTs)
}

func (tc *Catalog) populateCollection(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	tenantID := contextutil.TenantID(ctx)

	// get collection by collection_id and ts
	collection, err := tc.metaDomain.CollectionDb(ctx).Get(tenantID, collectionID, ts)
	if err != nil {
		return nil, err
	}

	// get fields by collection_id and ts
	fields, err := tc.metaDomain.FieldDb(ctx).GetByCollectionID(tenantID, collectionID, ts)
	if err != nil {
		return nil, err
	}

	// get partitions by collection_id and ts
	partitions, err := tc.metaDomain.PartitionDb(ctx).GetByCollectionID(tenantID, collectionID, ts)
	if err != nil {
		return nil, err
	}

	// get channels by collection_id and ts
	channels, err := tc.metaDomain.CollChannelDb(ctx).GetByCollectionID(tenantID, collectionID, ts)
	if err != nil {
		return nil, err
	}

	// merge as collection attributes

	mCollection, err := dbmodel.UnmarshalCollectionModel(collection)
	if err != nil {
		return nil, err
	}

	mFields, err := dbmodel.UnmarshalFieldModel(fields)
	if err != nil {
		return nil, err
	}

	mCollection.Fields = mFields
	mCollection.Partitions = dbmodel.UnmarshalPartitionModel(partitions)
	mCollection.VirtualChannelNames, mCollection.PhysicalChannelNames = dbmodel.ExtractChannelNames(channels)

	return mCollection, nil
}

func (tc *Catalog) GetCollectionByName(ctx context.Context, dbID int64, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	tenantID := contextutil.TenantID(ctx)

	// Since collection name will not change for different ts
	collectionID, err := tc.metaDomain.CollectionDb(ctx).GetCollectionIDByName(tenantID, collectionName, ts)
	if err != nil {
		return nil, err
	}

	return tc.GetCollectionByID(ctx, dbID, ts, collectionID)
}

// ListCollections For time travel (ts > 0), find only one record respectively for each collection no matter `is_deleted` is true or false
// i.e. there are 3 collections in total,
// [collection1, t1, is_deleted=true]
// [collection2, t2, is_deleted=false]
// [collection3, t3, is_deleted=false]
// t1, t2, t3 are the largest timestamp that less than or equal to @param ts
// the final result will only return collection2 and collection3 since collection1 is deleted
func (tc *Catalog) ListCollections(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Collection, error) {
	tenantID := contextutil.TenantID(ctx)

	// 1. find each collection_id with latest ts <= @param ts
	cidTsPairs, err := tc.metaDomain.CollectionDb(ctx).ListCollectionIDTs(tenantID, ts)
	if err != nil {
		return nil, err
	}
	if len(cidTsPairs) == 0 {
		return make([]*model.Collection, 0), nil
	}

	// 2. populate each collection
	collections := make([]*model.Collection, len(cidTsPairs))

	reloadCollectionByCollectionIDTsFunc := func(idx int) error {
		collIDTsPair := cidTsPairs[idx]
		collection, err := tc.populateCollection(ctx, collIDTsPair.CollectionID, collIDTsPair.Ts)
		if err != nil {
			return err
		}
		collections[idx] = collection
		return nil
	}

	concurrency := len(cidTsPairs)
	if concurrency > runtime.NumCPU() {
		concurrency = runtime.NumCPU()
	}
	err = funcutil.ProcessFuncParallel(len(cidTsPairs), concurrency, reloadCollectionByCollectionIDTsFunc, "ListCollectionByCollectionIDTs")
	if err != nil {
		log.Error("list collections by collection_id & ts pair failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}
	return collections, nil
}

func (tc *Catalog) CollectionExists(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	tenantID := contextutil.TenantID(ctx)

	// get latest timestamp less than or equals to param ts
	cidTsPair, err := tc.metaDomain.CollectionDb(ctx).GetCollectionIDTs(tenantID, collectionID, ts)
	if err != nil {
		return false
	}
	if cidTsPair.IsDeleted {
		return false
	}

	queryTs := cidTsPair.Ts

	col, err := tc.metaDomain.CollectionDb(ctx).Get(tenantID, collectionID, queryTs)
	if err != nil {
		return false
	}

	if col != nil {
		return !col.IsDeleted
	}

	return false
}

func (tc *Catalog) DropCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// 1. insert a mark-deleted record for collections
		coll := &dbmodel.Collection{
			TenantID:     tenantID,
			CollectionID: collection.CollectionID,
			Ts:           ts,
			IsDeleted:    true,
		}
		err := tc.metaDomain.CollectionDb(txCtx).Insert(coll)
		if err != nil {
			log.Error("insert tombstone record for collections failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
			return err
		}

		// 2. insert a mark-deleted record for collection_aliases
		if len(collection.Aliases) > 0 {
			collAliases := make([]*dbmodel.CollectionAlias, 0, len(collection.Aliases))

			for _, alias := range collection.Aliases {
				collAliases = append(collAliases, &dbmodel.CollectionAlias{
					TenantID:        tenantID,
					CollectionID:    collection.CollectionID,
					CollectionAlias: alias,
					Ts:              ts,
					IsDeleted:       true,
				})
			}

			err = tc.metaDomain.CollAliasDb(txCtx).Insert(collAliases)
			if err != nil {
				log.Error("insert tombstone record for collection_aliases failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
				return err
			}
		}

		// 3. insert a mark-deleted record for collection_channels
		collChannel := &dbmodel.CollectionChannel{
			TenantID:     tenantID,
			CollectionID: collection.CollectionID,
			Ts:           ts,
			IsDeleted:    true,
		}
		err = tc.metaDomain.CollChannelDb(txCtx).Insert([]*dbmodel.CollectionChannel{collChannel})
		if err != nil {
			log.Error("insert tombstone record for collection_channels failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
			return err
		}

		// 4. insert a mark-deleted record for field_schemas
		field := &dbmodel.Field{
			TenantID:     tenantID,
			CollectionID: collection.CollectionID,
			Ts:           ts,
			IsDeleted:    true,
		}
		err = tc.metaDomain.FieldDb(txCtx).Insert([]*dbmodel.Field{field})
		if err != nil {
			log.Error("insert tombstone record for field_schemas failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
			return err
		}

		// 5. insert a mark-deleted record for partitions
		partition := &dbmodel.Partition{
			TenantID:     tenantID,
			CollectionID: collection.CollectionID,
			Ts:           ts,
			IsDeleted:    true,
		}
		err = tc.metaDomain.PartitionDb(txCtx).Insert([]*dbmodel.Partition{partition})
		if err != nil {
			log.Error("insert tombstone record for partitions failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
			return err
		}

		return err
	})
}

func (tc *Catalog) alterModifyCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp) error {
	if oldColl.TenantID != newColl.TenantID || oldColl.CollectionID != newColl.CollectionID {
		return fmt.Errorf("altering tenant id or collection id is forbidden")
	}

	var startPositionsStr string
	if newColl.StartPositions != nil {
		startPositionsBytes, err := json.Marshal(newColl.StartPositions)
		if err != nil {
			return fmt.Errorf("failed to marshal start positions: %s", err.Error())
		}
		startPositionsStr = string(startPositionsBytes)
	}

	properties, err := dbmodel.MarshalProperties(newColl.Properties)
	if err != nil {
		return err
	}

	createdAt, _ := tsoutil.ParseTS(newColl.CreateTime)
	tenantID := contextutil.TenantID(ctx)
	coll := &dbmodel.Collection{
		TenantID:         tenantID,
		CollectionID:     newColl.CollectionID,
		CollectionName:   newColl.Name,
		Description:      newColl.Description,
		AutoID:           newColl.AutoID,
		ShardsNum:        newColl.ShardsNum,
		StartPosition:    startPositionsStr,
		ConsistencyLevel: int32(newColl.ConsistencyLevel),
		Status:           int32(newColl.State),
		Ts:               ts,
		CreatedAt:        createdAt,
		UpdatedAt:        time.Now(),
		Properties:       properties,
	}

	return tc.metaDomain.CollectionDb(ctx).Update(coll)
}

func (tc *Catalog) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	if alterType == metastore.MODIFY {
		return tc.alterModifyCollection(ctx, oldColl, newColl, ts)
	}
	return fmt.Errorf("altering collection doesn't support %s", alterType.String())
}

func (tc *Catalog) CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	p := &dbmodel.Partition{
		TenantID:                  tenantID,
		PartitionID:               partition.PartitionID,
		PartitionName:             partition.PartitionName,
		PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
		CollectionID:              partition.CollectionID,
		Status:                    int32(partition.State),
		Ts:                        ts,
	}
	err := tc.metaDomain.PartitionDb(ctx).Insert([]*dbmodel.Partition{p})
	if err != nil {
		log.Error("insert partitions failed", zap.String("tenant", tenantID), zap.Int64("collectionID", partition.CollectionID), zap.Int64("partitionID", partition.PartitionID), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) DropPartition(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	p := &dbmodel.Partition{
		TenantID:     tenantID,
		PartitionID:  partitionID,
		CollectionID: collectionID,
		Ts:           ts,
		IsDeleted:    true,
	}
	err := tc.metaDomain.PartitionDb(ctx).Insert([]*dbmodel.Partition{p})
	if err != nil {
		log.Error("insert tombstone record for partition failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) alterModifyPartition(ctx context.Context, oldPart *model.Partition, newPart *model.Partition, ts typeutil.Timestamp) error {
	createdAt, _ := tsoutil.ParseTS(newPart.PartitionCreatedTimestamp)
	p := &dbmodel.Partition{
		TenantID:                  contextutil.TenantID(ctx),
		PartitionID:               newPart.PartitionID,
		PartitionName:             newPart.PartitionName,
		PartitionCreatedTimestamp: newPart.PartitionCreatedTimestamp,
		CollectionID:              newPart.CollectionID,
		Status:                    int32(newPart.State),
		Ts:                        ts,
		IsDeleted:                 false,
		CreatedAt:                 createdAt,
		UpdatedAt:                 time.Now(),
	}
	return tc.metaDomain.PartitionDb(ctx).Update(p)
}

func (tc *Catalog) AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	if alterType == metastore.MODIFY {
		return tc.alterModifyPartition(ctx, oldPart, newPart, ts)
	}
	return fmt.Errorf("altering partition doesn't support: %s", alterType.String())
}

func (tc *Catalog) CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	collAlias := &dbmodel.CollectionAlias{
		TenantID:        tenantID,
		CollectionID:    alias.CollectionID,
		CollectionAlias: alias.Name,
		Ts:              ts,
	}
	err := tc.metaDomain.CollAliasDb(ctx).Insert([]*dbmodel.CollectionAlias{collAlias})
	if err != nil {
		log.Error("insert collection_aliases failed", zap.Int64("collectionID", alias.CollectionID), zap.String("alias", alias.Name), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) DropAlias(ctx context.Context, dbID int64, alias string, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	collectionID, err := tc.metaDomain.CollAliasDb(ctx).GetCollectionIDByAlias(tenantID, alias, ts)
	if err != nil {
		return err
	}

	collAlias := &dbmodel.CollectionAlias{
		TenantID:        tenantID,
		CollectionID:    collectionID,
		CollectionAlias: alias,
		Ts:              ts,
		IsDeleted:       true,
	}
	err = tc.metaDomain.CollAliasDb(ctx).Insert([]*dbmodel.CollectionAlias{collAlias})
	if err != nil {
		log.Error("insert tombstone record for collection_aliases failed", zap.Int64("collectionID", collectionID), zap.String("collAlias", alias), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	//if ts == 0 {
	//	tenantID := contextutil.TenantID(ctx)
	//	alias := collection.Aliases[0]
	//
	//	return tc.metaDomain.CollAliasDb(ctx).Update(tenantID, collection.CollectionID, alias, ts)
	//}

	return tc.CreateAlias(ctx, alias, ts)
}

// ListAliases query collection ID and aliases only, other information are not needed
func (tc *Catalog) ListAliases(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error) {
	tenantID := contextutil.TenantID(ctx)

	// 1. find each collection with latest ts
	cidTsPairs, err := tc.metaDomain.CollAliasDb(ctx).ListCollectionIDTs(tenantID, ts)
	if err != nil {
		log.Error("list latest ts and corresponding collectionID in collection_aliases failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}
	if len(cidTsPairs) == 0 {
		return []*model.Alias{}, nil
	}

	// 2. select with IN clause
	collAliases, err := tc.metaDomain.CollAliasDb(ctx).List(tenantID, cidTsPairs)
	if err != nil {
		log.Error("list collection alias failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	r := make([]*model.Alias, 0, len(collAliases))
	for _, record := range collAliases {
		r = append(r, &model.Alias{
			CollectionID: record.CollectionID,
			Name:         record.CollectionAlias,
		})
	}

	return r, nil
}

func (tc *Catalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	tenantID := contextutil.TenantID(ctx)

	user, err := tc.metaDomain.UserDb(ctx).GetByUsername(tenantID, username)
	if err != nil {
		return nil, err
	}

	return dbmodel.UnmarshalUserModel(user), nil
}

func (tc *Catalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	tenantID := contextutil.TenantID(ctx)

	user := &dbmodel.User{
		TenantID:          tenantID,
		Username:          credential.Username,
		EncryptedPassword: credential.EncryptedPassword,
	}

	err := tc.metaDomain.UserDb(ctx).Insert(user)
	if err != nil {
		return err
	}

	return nil
}

func (tc *Catalog) AlterCredential(ctx context.Context, credential *model.Credential) error {
	tenantID := contextutil.TenantID(ctx)

	err := tc.metaDomain.UserDb(ctx).UpdatePassword(tenantID, credential.Username, credential.EncryptedPassword)
	if err != nil {
		return err
	}

	return nil
}

func (tc *Catalog) DropCredential(ctx context.Context, username string) error {
	tenantID := contextutil.TenantID(ctx)

	err := tc.metaDomain.UserDb(ctx).MarkDeletedByUsername(tenantID, username)
	if err != nil {
		return err
	}

	return nil
}

func (tc *Catalog) ListCredentials(ctx context.Context) ([]string, error) {
	tenantID := contextutil.TenantID(ctx)

	users, err := tc.metaDomain.UserDb(ctx).ListUser(tenantID)
	if err != nil {
		return nil, err
	}
	var usernames []string
	for _, user := range users {
		usernames = append(usernames, user.Username)
	}
	return usernames, nil
}

func (tc *Catalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	var err error
	if _, err = tc.GetRoleIDByName(ctx, tenant, entity.Name); err != nil && !common.IsKeyNotExistError(err) {
		return err
	}
	if err == nil {
		return common.NewIgnorableError(fmt.Errorf("the role[%s] has existed", entity.Name))
	}
	return tc.metaDomain.RoleDb(ctx).Insert(&dbmodel.Role{
		Base: dbmodel.Base{TenantID: tenant},
		Name: entity.Name,
	})
}

func (tc *Catalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	return tc.metaDomain.RoleDb(ctx).Delete(tenant, roleName)
}

func (tc *Catalog) GetRoleIDByName(ctx context.Context, tenant string, name string) (int64, error) {
	var (
		roles []*dbmodel.Role
		err   error
	)

	if roles, err = tc.metaDomain.RoleDb(ctx).GetRoles(tenant, name); err != nil {
		return 0, err
	}
	if len(roles) < 1 {
		return 0, common.NewKeyNotExistError(fmt.Sprintf("%s/%s", tenant, name))
	}
	return roles[0].ID, nil
}

func (tc *Catalog) AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	var (
		user      *dbmodel.User
		roleID    int64
		userRole  *dbmodel.UserRole
		userRoles []*dbmodel.UserRole
		err       error
	)
	if user, err = tc.metaDomain.UserDb(ctx).GetByUsername(tenant, userEntity.Name); err != nil {
		log.Error("fail to get userID by the username", zap.String("username", userEntity.Name), zap.Error(err))
		return err
	}
	if roleID, err = tc.GetRoleIDByName(ctx, tenant, roleEntity.Name); err != nil {
		log.Error("fail to get roleID by the role name", zap.String("role_name", roleEntity.Name), zap.Error(err))
		return err
	}
	userRole = &dbmodel.UserRole{Base: dbmodel.Base{TenantID: tenant}, UserID: user.ID, RoleID: roleID}
	userRoles, err = tc.metaDomain.UserRoleDb(ctx).GetUserRoles(userRole.TenantID, userRole.UserID, userRole.RoleID)
	if err != nil {
		return err
	}
	switch operateType {
	case milvuspb.OperateUserRoleType_AddUserToRole:
		if len(userRoles) > 0 {
			return common.NewIgnorableError(fmt.Errorf("the user-role[%s-%s] is existed", userEntity.Name, roleEntity.Name))
		}
		return tc.metaDomain.UserRoleDb(ctx).Insert(userRole)
	case milvuspb.OperateUserRoleType_RemoveUserFromRole:
		if len(userRoles) < 1 {
			return common.NewIgnorableError(fmt.Errorf("the user-role[%s-%s] isn't existed", userEntity.Name, roleEntity.Name))
		}
		return tc.metaDomain.UserRoleDb(ctx).Delete(userRole.TenantID, userRole.UserID, userRole.RoleID)
	default:
		err = fmt.Errorf("invalid operate type: %d", operateType)
		log.Error("error: ", zap.Error(err))
		return err
	}
}

func (tc *Catalog) ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	var (
		roleName string
		roles    []*dbmodel.Role
		results  []*milvuspb.RoleResult
		err      error
	)
	if entity != nil {
		roleName = entity.Name
	}
	roles, err = tc.metaDomain.RoleDb(ctx).GetRoles(tenant, roleName)
	if err != nil {
		return nil, err
	}
	for _, role := range roles {
		var users []*milvuspb.UserEntity
		var userRoles []*dbmodel.UserRole
		if includeUserInfo {
			if userRoles, err = tc.metaDomain.UserRoleDb(ctx).GetUserRoles(tenant, 0, role.ID); err != nil {
				return nil, err
			}
			for _, userRole := range userRoles {
				users = append(users, &milvuspb.UserEntity{Name: userRole.User.Username})
			}
		}
		results = append(results, &milvuspb.RoleResult{
			Role:  role.Unmarshal(),
			Users: users,
		})
	}
	if !funcutil.IsEmptyString(roleName) && len(results) == 0 {
		return nil, common.NewKeyNotExistError(fmt.Sprintf("%s/%s", tenant, roleName))
	}
	return results, nil
}

func (tc *Catalog) ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	var (
		users    []*dbmodel.User
		results  []*milvuspb.UserResult
		username string
		err      error
	)
	if entity != nil {
		var user *dbmodel.User
		username = entity.Name
		if user, err = tc.metaDomain.UserDb(ctx).GetByUsername(tenant, username); err != nil {
			return nil, err
		}
		users = append(users, user)
	} else {
		if users, err = tc.metaDomain.UserDb(ctx).ListUser(tenant); err != nil {
			return nil, err
		}
	}
	for _, user := range users {
		var roles []*milvuspb.RoleEntity
		var userRoles []*dbmodel.UserRole
		if includeRoleInfo {
			if userRoles, err = tc.metaDomain.UserRoleDb(ctx).GetUserRoles(tenant, user.ID, 0); err != nil {
				return nil, err
			}
			for _, userRole := range userRoles {
				roles = append(roles, &milvuspb.RoleEntity{Name: userRole.Role.Name})
			}
		}
		results = append(results, &milvuspb.UserResult{
			User:  &milvuspb.UserEntity{Name: user.Username},
			Roles: roles,
		})
	}
	return results, nil
}

func (tc Catalog) grant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) error {
	var (
		roleID      int64
		grants      []*dbmodel.Grant
		grantID     int64
		grantIDObjs []*dbmodel.GrantID
		err         error
	)
	if roleID, err = tc.GetRoleIDByName(ctx, tenant, entity.Role.Name); err != nil {
		return err
	}

	if grants, err = tc.metaDomain.GrantDb(ctx).GetGrants(tenant, roleID, entity.Object.Name, entity.ObjectName); err != nil {
		return err
	}
	if len(grants) == 0 {
		var grant = &dbmodel.Grant{
			Base:       dbmodel.Base{TenantID: tenant},
			RoleID:     roleID,
			Object:     entity.Object.Name,
			ObjectName: entity.ObjectName,
		}
		if err = tc.metaDomain.GrantDb(ctx).Insert(grant); err != nil {
			return err
		}
		log.Debug("grant id", zap.Int64("id", grant.ID))
		grantID = grant.ID
	} else {
		grantID = grants[0].ID
	}
	if grantIDObjs, err = tc.metaDomain.GrantIDDb(ctx).GetGrantIDs(tenant, grantID, entity.Grantor.Privilege.Name, false, false); err != nil {
		return err
	}
	if len(grantIDObjs) > 0 {
		log.Warn("the grant id has existed", zap.Any("entity", entity))
		return common.NewIgnorableError(fmt.Errorf("the privilege [%s] has been grantd for the role [%s]", entity.Grantor.Privilege.Name, entity.Role.Name))
	}
	var user *dbmodel.User
	if user, err = tc.metaDomain.UserDb(ctx).GetByUsername(tenant, entity.Grantor.User.Name); err != nil {
		return err
	}
	return tc.metaDomain.GrantIDDb(ctx).Insert(&dbmodel.GrantID{
		Base:      dbmodel.Base{TenantID: tenant},
		GrantID:   grantID,
		Privilege: entity.Grantor.Privilege.Name,
		GrantorID: user.ID,
	})
}

func (tc Catalog) revoke(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) error {
	var (
		roleID      int64
		grants      []*dbmodel.Grant
		grantID     int64
		grantIDObjs []*dbmodel.GrantID
		err         error
	)

	if roleID, err = tc.GetRoleIDByName(ctx, tenant, entity.Role.Name); err != nil {
		return err
	}

	if grants, err = tc.metaDomain.GrantDb(ctx).GetGrants(tenant, roleID, entity.Object.Name, entity.ObjectName); err != nil {
		return err
	}
	if len(grants) == 0 {
		log.Warn("the grant isn't existed", zap.Any("entity", entity))
		return common.NewIgnorableError(fmt.Errorf("the privilege [%s] isn't grantd for the role [%s]", entity.Grantor.Privilege.Name, entity.Role.Name))
	}
	grantID = grants[0].ID
	if grantIDObjs, err = tc.metaDomain.GrantIDDb(ctx).GetGrantIDs(tenant, grantID, entity.Grantor.Privilege.Name, false, false); err != nil {
		return err
	}
	if len(grantIDObjs) == 0 {
		log.Error("the grant-id isn't existed", zap.Any("entity", entity))
		return common.NewIgnorableError(fmt.Errorf("the privilege [%s] isn't grantd for the role [%s]", entity.Grantor.Privilege.Name, entity.Role.Name))
	}
	return tc.metaDomain.GrantIDDb(ctx).Delete(tenant, grantID, entity.Grantor.Privilege.Name)
}

func (tc *Catalog) AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	switch operateType {
	case milvuspb.OperatePrivilegeType_Grant:
		return tc.grant(ctx, tenant, entity)
	case milvuspb.OperatePrivilegeType_Revoke:
		return tc.revoke(ctx, tenant, entity)
	default:
		err := fmt.Errorf("invalid operate type: %d", operateType)
		log.Error("error: ", zap.Error(err))
		return err
	}
}

func (tc *Catalog) ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var (
		roleID        int64
		object        string
		objectName    string
		grants        []*dbmodel.Grant
		grantEntities []*milvuspb.GrantEntity
		grantIDDb     dbmodel.IGrantIDDb
		grantIDs      []*dbmodel.GrantID
		privilegeName string
		err           error
	)
	if !funcutil.IsEmptyString(entity.ObjectName) && entity.Object != nil && !funcutil.IsEmptyString(entity.Object.Name) {
		object = entity.Object.Name
		objectName = entity.ObjectName
	}
	if roleID, err = tc.GetRoleIDByName(ctx, tenant, entity.Role.Name); err != nil {
		log.Error("fail to get roleID by the role name", zap.String("role_name", entity.Role.Name), zap.Error(err))
		return nil, err
	}
	if grants, err = tc.metaDomain.GrantDb(ctx).GetGrants(tenant, roleID, object, objectName); err != nil {
		return nil, err
	}
	grantIDDb = tc.metaDomain.GrantIDDb(ctx)
	for _, grant := range grants {
		if grantIDs, err = grantIDDb.GetGrantIDs(tenant, grant.ID, "", false, true); err != nil {
			return nil, err
		}
		for _, grantID := range grantIDs {
			privilegeName = util.PrivilegeNameForAPI(grantID.Privilege)
			if grantID.Privilege == util.AnyWord {
				privilegeName = util.AnyWord
			}
			grantEntities = append(grantEntities, &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: grant.Role.Name},
				Object:     &milvuspb.ObjectEntity{Name: grant.Object},
				ObjectName: grant.ObjectName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: grantID.Grantor.Username},
					Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
				},
			})
		}
	}
	if !funcutil.IsEmptyString(object) && !funcutil.IsEmptyString(objectName) && len(grantEntities) == 0 {
		return nil, common.NewKeyNotExistError(fmt.Sprintf("%s/%s/%s/%s", tenant, entity.Role.Name, object, objectName))
	}
	return grantEntities, nil
}

func (tc *Catalog) DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	var (
		roleID int64
		err    error
	)

	if roleID, err = tc.GetRoleIDByName(ctx, tenant, role.Name); err != nil {
		log.Error("fail to get roleID by the role name", zap.String("role_name", role.Name), zap.Error(err))
		return err
	}
	return tc.metaDomain.GrantDb(ctx).Delete(tenant, roleID, "", "")
}

func (tc *Catalog) ListPolicy(ctx context.Context, tenant string) ([]string, error) {
	var (
		grants    []*dbmodel.Grant
		grantIDDb dbmodel.IGrantIDDb
		grantIDs  []*dbmodel.GrantID
		policies  []string
		err       error
	)
	if grants, err = tc.metaDomain.GrantDb(ctx).GetGrants(tenant, 0, "", ""); err != nil {
		return nil, err
	}
	grantIDDb = tc.metaDomain.GrantIDDb(ctx)
	for _, grant := range grants {
		if grantIDs, err = grantIDDb.GetGrantIDs(tenant, grant.ID, "", false, false); err != nil {
			return nil, err
		}
		for _, grantID := range grantIDs {
			policies = append(policies,
				funcutil.PolicyForPrivilege(grant.Role.Name, grant.Object, grant.ObjectName, grantID.Privilege, "default"))
		}
	}

	return policies, nil
}

func (tc *Catalog) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	var (
		userRoleStrs []string
		userRoles    []*dbmodel.UserRole
		err          error
	)

	if userRoles, err = tc.metaDomain.UserRoleDb(ctx).GetUserRoles(tenant, 0, 0); err != nil {
		return nil, err
	}
	for _, userRole := range userRoles {
		userRoleStrs = append(userRoleStrs, funcutil.EncodeUserRoleCache(userRole.User.Username, userRole.Role.Name))
	}

	return userRoleStrs, nil
}

func (tc *Catalog) Close() {

}
