package rootcoord

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	maxTxnNum = 64
)

// prefix/collection/collection_id 					-> CollectionInfo
// prefix/partitions/collection_id/partition_id		-> PartitionInfo
// prefix/aliases/alias_name						-> AliasInfo
// prefix/fields/collection_id/field_id				-> FieldSchema
type Catalog struct {
	Txn      kv.TxnKV
	Snapshot kv.SnapShotKV
}

func BuildCollectionKey(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d", CollectionMetaPrefix, collectionID)
}

func BuildPartitionPrefix(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d", PartitionMetaPrefix, collectionID)
}

func BuildPartitionKey(collectionID, partitionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d", BuildPartitionPrefix(collectionID), partitionID)
}

func BuildFieldPrefix(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d", FieldMetaPrefix, collectionID)
}

func BuildFieldKey(collectionID typeutil.UniqueID, fieldID int64) string {
	return fmt.Sprintf("%s/%d", BuildFieldPrefix(collectionID), fieldID)
}

func BuildAliasKey210(alias string) string {
	return fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix210, alias)
}

func BuildAliasKey(aliasName string) string {
	return fmt.Sprintf("%s/%s", AliasMetaPrefix, aliasName)
}

func batchMultiSaveAndRemoveWithPrefix(snapshot kv.SnapShotKV, maxTxnNum int, saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	saveFn := func(partialKvs map[string]string) error {
		return snapshot.MultiSave(partialKvs, ts)
	}
	if err := etcd.SaveByBatchWithLimit(saves, maxTxnNum/2, saveFn); err != nil {
		return err
	}

	removeFn := func(partialKeys []string) error {
		return snapshot.MultiSaveAndRemoveWithPrefix(nil, partialKeys, ts)
	}
	return etcd.RemoveByBatch(removals, removeFn)
}

func (kc *Catalog) CreateCollection(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	if coll.State != pb.CollectionState_CollectionCreating {
		return fmt.Errorf("cannot create collection with state: %s, collection: %s", coll.State.String(), coll.Name)
	}

	k1 := BuildCollectionKey(coll.CollectionID)
	collInfo := model.MarshalCollectionModel(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal collection info: %s", err.Error())
	}

	// Due to the limit of etcd txn number, we must split these kvs into several batches.
	// Save collection key first, and the state of collection is creating.
	// If we save collection key with error, then no garbage will be generated and error will be raised.
	// If we succeeded to save collection but failed to save other related keys, the garbage meta can be removed
	// outside and the collection won't be seen by any others (since it's of creating state).
	// However, if we save other keys first, there is no chance to remove the intermediate meta.
	if err := kc.Snapshot.Save(k1, string(v1), ts); err != nil {
		return err
	}

	kvs := map[string]string{}

	// save partition info to newly path.
	for _, partition := range coll.Partitions {
		k := BuildPartitionKey(coll.CollectionID, partition.PartitionID)
		partitionInfo := model.MarshalPartitionModel(partition)
		v, err := proto.Marshal(partitionInfo)
		if err != nil {
			return err
		}
		kvs[k] = string(v)
	}

	// no default aliases will be created.

	// save fields info to newly path.
	for _, field := range coll.Fields {
		k := BuildFieldKey(coll.CollectionID, field.FieldID)
		fieldInfo := model.MarshalFieldModel(field)
		v, err := proto.Marshal(fieldInfo)
		if err != nil {
			return err
		}
		kvs[k] = string(v)
	}

	// Though batchSave is not atomic enough, we can promise the atomicity outside.
	// Recovering from failure, if we found collection is creating, we should removing all these related meta.
	return etcd.SaveByBatchWithLimit(kvs, maxTxnNum/2, func(partialKvs map[string]string) error {
		return kc.Snapshot.MultiSave(partialKvs, ts)
	})
}

func (kc *Catalog) loadCollection(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
	collKey := BuildCollectionKey(collectionID)
	collVal, err := kc.Snapshot.Load(collKey, ts)
	if err != nil {
		return nil, common.NewCollectionNotExistError(fmt.Sprintf("collection not found: %d", collectionID))
	}

	collMeta := &pb.CollectionInfo{}
	err = proto.Unmarshal([]byte(collVal), collMeta)
	return collMeta, err
}

func partitionVersionAfter210(collMeta *pb.CollectionInfo) bool {
	return len(collMeta.GetPartitionIDs()) <= 0 &&
		len(collMeta.GetPartitionNames()) <= 0 &&
		len(collMeta.GetPartitionCreatedTimestamps()) <= 0
}

func partitionExistByID(collMeta *pb.CollectionInfo, partitionID typeutil.UniqueID) bool {
	return funcutil.SliceContain(collMeta.GetPartitionIDs(), partitionID)
}

func partitionExistByName(collMeta *pb.CollectionInfo, partitionName string) bool {
	return funcutil.SliceContain(collMeta.GetPartitionNames(), partitionName)
}

func (kc *Catalog) CreatePartition(ctx context.Context, partition *model.Partition, ts typeutil.Timestamp) error {
	collMeta, err := kc.loadCollection(ctx, partition.CollectionID, ts)
	if err != nil {
		return err
	}

	if partitionVersionAfter210(collMeta) {
		// save to newly path.
		k := BuildPartitionKey(partition.CollectionID, partition.PartitionID)
		partitionInfo := model.MarshalPartitionModel(partition)
		v, err := proto.Marshal(partitionInfo)
		if err != nil {
			return err
		}
		return kc.Snapshot.Save(k, string(v), ts)
	}

	if partitionExistByID(collMeta, partition.PartitionID) {
		return fmt.Errorf("partition already exist: %d", partition.PartitionID)
	}

	if partitionExistByName(collMeta, partition.PartitionName) {
		return fmt.Errorf("partition already exist: %s", partition.PartitionName)
	}

	// keep consistent with older version, otherwise it's hard to judge where to find partitions.
	collMeta.PartitionIDs = append(collMeta.PartitionIDs, partition.PartitionID)
	collMeta.PartitionNames = append(collMeta.PartitionNames, partition.PartitionName)
	collMeta.PartitionCreatedTimestamps = append(collMeta.PartitionCreatedTimestamps, partition.PartitionCreatedTimestamp)

	k := BuildCollectionKey(partition.CollectionID)
	v, err := proto.Marshal(collMeta)
	if err != nil {
		return err
	}
	return kc.Snapshot.Save(k, string(v), ts)
}

func (kc *Catalog) CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	oldKBefore210 := BuildAliasKey210(alias.Name)
	k := BuildAliasKey(alias.Name)
	aliasInfo := model.MarshalAliasModel(alias)
	v, err := proto.Marshal(aliasInfo)
	if err != nil {
		return err
	}
	kvs := map[string]string{k: string(v)}
	return kc.Snapshot.MultiSaveAndRemoveWithPrefix(kvs, []string{oldKBefore210}, ts)
}

func (kc *Catalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	k := fmt.Sprintf("%s/%s", CredentialPrefix, credential.Username)
	v, err := json.Marshal(&internalpb.CredentialInfo{EncryptedPassword: credential.EncryptedPassword})
	if err != nil {
		log.Error("create credential marshal fail", zap.String("key", k), zap.Error(err))
		return err
	}

	err = kc.Txn.Save(k, string(v))
	if err != nil {
		log.Error("create credential persist meta fail", zap.String("key", k), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) AlterCredential(ctx context.Context, credential *model.Credential) error {
	return kc.CreateCredential(ctx, credential)
}

func (kc *Catalog) listPartitionsAfter210(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*model.Partition, error) {
	prefix := BuildPartitionPrefix(collectionID)
	_, values, err := kc.Snapshot.LoadWithPrefix(prefix, ts)
	if err != nil {
		return nil, err
	}
	partitions := make([]*model.Partition, 0, len(values))
	for _, v := range values {
		partitionMeta := &pb.PartitionInfo{}
		err := proto.Unmarshal([]byte(v), partitionMeta)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, model.UnmarshalPartitionModel(partitionMeta))
	}
	return partitions, nil
}

func fieldVersionAfter210(collMeta *pb.CollectionInfo) bool {
	return len(collMeta.GetSchema().GetFields()) <= 0
}

func (kc *Catalog) listFieldsAfter210(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*model.Field, error) {
	prefix := BuildFieldPrefix(collectionID)
	_, values, err := kc.Snapshot.LoadWithPrefix(prefix, ts)
	if err != nil {
		return nil, err
	}
	fields := make([]*model.Field, 0, len(values))
	for _, v := range values {
		partitionMeta := &schemapb.FieldSchema{}
		err := proto.Unmarshal([]byte(v), partitionMeta)
		if err != nil {
			return nil, err
		}
		fields = append(fields, model.UnmarshalFieldModel(partitionMeta))
	}
	return fields, nil
}

func (kc *Catalog) appendPartitionAndFieldsInfo(ctx context.Context, collMeta *pb.CollectionInfo,
	ts typeutil.Timestamp) (*model.Collection, error) {

	collection := model.UnmarshalCollectionModel(collMeta)

	if !partitionVersionAfter210(collMeta) && !fieldVersionAfter210(collMeta) {
		return collection, nil
	}

	partitions, err := kc.listPartitionsAfter210(ctx, collection.CollectionID, ts)
	if err != nil {
		return nil, err
	}
	collection.Partitions = partitions

	fields, err := kc.listFieldsAfter210(ctx, collection.CollectionID, ts)
	if err != nil {
		return nil, err
	}
	collection.Fields = fields

	return collection, nil
}

func (kc *Catalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID,
	ts typeutil.Timestamp) (*model.Collection, error) {
	collMeta, err := kc.loadCollection(ctx, collectionID, ts)
	if err != nil {
		return nil, err
	}

	return kc.appendPartitionAndFieldsInfo(ctx, collMeta, ts)
}

func (kc *Catalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	_, err := kc.GetCollectionByID(ctx, collectionID, ts)
	return err == nil
}

func (kc *Catalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	k := fmt.Sprintf("%s/%s", CredentialPrefix, username)
	v, err := kc.Txn.Load(k)
	if err != nil {
		if common.IsKeyNotExistError(err) {
			log.Debug("not found the user", zap.String("key", k))
		} else {
			log.Warn("get credential meta fail", zap.String("key", k), zap.Error(err))
		}
		return nil, err
	}

	credentialInfo := internalpb.CredentialInfo{}
	err = json.Unmarshal([]byte(v), &credentialInfo)
	if err != nil {
		return nil, fmt.Errorf("unmarshal credential info err:%w", err)
	}

	return &model.Credential{Username: username, EncryptedPassword: credentialInfo.EncryptedPassword}, nil
}

func (kc *Catalog) AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	return kc.CreateAlias(ctx, alias, ts)
}

func (kc *Catalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	collectionKey := BuildCollectionKey(collectionInfo.CollectionID)

	var delMetakeysSnap []string
	for _, alias := range collectionInfo.Aliases {
		delMetakeysSnap = append(delMetakeysSnap,
			BuildAliasKey210(alias),
		)
	}
	// Snapshot will list all (k, v) pairs and then use Txn.MultiSave to save tombstone for these keys when it prepares
	// to remove a prefix, so though we have very few prefixes, the final operations may exceed the max txn number.
	// TODO(longjiquan): should we list all partitions & fields in KV anyway?
	for _, partition := range collectionInfo.Partitions {
		delMetakeysSnap = append(delMetakeysSnap, BuildPartitionKey(collectionInfo.CollectionID, partition.PartitionID))
	}
	for _, field := range collectionInfo.Fields {
		delMetakeysSnap = append(delMetakeysSnap, BuildFieldKey(collectionInfo.CollectionID, field.FieldID))
	}
	// delMetakeysSnap = append(delMetakeysSnap, buildPartitionPrefix(collectionInfo.CollectionID))
	// delMetakeysSnap = append(delMetakeysSnap, buildFieldPrefix(collectionInfo.CollectionID))

	// Though batchMultiSaveAndRemoveWithPrefix is not atomic enough, we can promise atomicity outside.
	// If we found collection under dropping state, we'll know that gc is not completely on this collection.
	// However, if we remove collection first, we cannot remove other metas.
	if err := batchMultiSaveAndRemoveWithPrefix(kc.Snapshot, maxTxnNum, nil, delMetakeysSnap, ts); err != nil {
		return err
	}

	// if we found collection dropping, we should try removing related resources.
	return kc.Snapshot.MultiSaveAndRemoveWithPrefix(nil, []string{collectionKey}, ts)
}

func (kc *Catalog) alterModifyCollection(oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp) error {
	if oldColl.TenantID != newColl.TenantID || oldColl.CollectionID != newColl.CollectionID {
		return fmt.Errorf("altering tenant id or collection id is forbidden")
	}
	oldCollClone := oldColl.Clone()
	oldCollClone.Name = newColl.Name
	oldCollClone.Description = newColl.Description
	oldCollClone.AutoID = newColl.AutoID
	oldCollClone.VirtualChannelNames = newColl.VirtualChannelNames
	oldCollClone.PhysicalChannelNames = newColl.PhysicalChannelNames
	oldCollClone.StartPositions = newColl.StartPositions
	oldCollClone.ShardsNum = newColl.ShardsNum
	oldCollClone.CreateTime = newColl.CreateTime
	oldCollClone.ConsistencyLevel = newColl.ConsistencyLevel
	oldCollClone.State = newColl.State
	key := BuildCollectionKey(oldColl.CollectionID)
	value, err := proto.Marshal(model.MarshalCollectionModel(oldCollClone))
	if err != nil {
		return err
	}
	return kc.Snapshot.Save(key, string(value), ts)
}

func (kc *Catalog) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	if alterType == metastore.MODIFY {
		return kc.alterModifyCollection(oldColl, newColl, ts)
	}
	return fmt.Errorf("altering collection doesn't support %s", alterType.String())
}

func (kc *Catalog) alterModifyPartition(oldPart *model.Partition, newPart *model.Partition, ts typeutil.Timestamp) error {
	if oldPart.CollectionID != newPart.CollectionID || oldPart.PartitionID != newPart.PartitionID {
		return fmt.Errorf("altering collection id or partition id is forbidden")
	}
	oldPartClone := oldPart.Clone()
	newPartClone := newPart.Clone()
	oldPartClone.PartitionName = newPartClone.PartitionName
	oldPartClone.PartitionCreatedTimestamp = newPartClone.PartitionCreatedTimestamp
	oldPartClone.State = newPartClone.State
	key := BuildPartitionKey(oldPart.CollectionID, oldPart.PartitionID)
	value, err := proto.Marshal(model.MarshalPartitionModel(oldPartClone))
	if err != nil {
		return err
	}
	return kc.Snapshot.Save(key, string(value), ts)
}

func (kc *Catalog) AlterPartition(ctx context.Context, oldPart *model.Partition, newPart *model.Partition, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	if alterType == metastore.MODIFY {
		return kc.alterModifyPartition(oldPart, newPart, ts)
	}
	return fmt.Errorf("altering partition doesn't support %s", alterType.String())
}

func dropPartition(collMeta *pb.CollectionInfo, partitionID typeutil.UniqueID) {
	if collMeta == nil {
		return
	}

	{
		loc := -1
		for idx, pid := range collMeta.GetPartitionIDs() {
			if pid == partitionID {
				loc = idx
				break
			}
		}
		if loc != -1 {
			collMeta.PartitionIDs = append(collMeta.GetPartitionIDs()[:loc], collMeta.GetPartitionIDs()[loc+1:]...)
			collMeta.PartitionNames = append(collMeta.GetPartitionNames()[:loc], collMeta.GetPartitionNames()[loc+1:]...)
			collMeta.PartitionCreatedTimestamps = append(collMeta.GetPartitionCreatedTimestamps()[:loc], collMeta.GetPartitionCreatedTimestamps()[loc+1:]...)
		}
	}
}

func (kc *Catalog) DropPartition(ctx context.Context, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta, err := kc.loadCollection(ctx, collectionID, ts)
	if err != nil {
		return err
	}

	if partitionVersionAfter210(collMeta) {
		k := BuildPartitionKey(collectionID, partitionID)
		return kc.Snapshot.MultiSaveAndRemoveWithPrefix(nil, []string{k}, ts)
	}

	k := BuildCollectionKey(collectionID)
	dropPartition(collMeta, partitionID)
	v, err := proto.Marshal(collMeta)
	if err != nil {
		return err
	}
	return kc.Snapshot.Save(k, string(v), ts)
}

func (kc *Catalog) DropCredential(ctx context.Context, username string) error {
	k := fmt.Sprintf("%s/%s", CredentialPrefix, username)
	err := kc.Txn.Remove(k)
	if err != nil {
		log.Error("drop credential update meta fail", zap.String("key", k), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) DropAlias(ctx context.Context, alias string, ts typeutil.Timestamp) error {
	oldKBefore210 := BuildAliasKey210(alias)
	k := BuildAliasKey(alias)
	return kc.Snapshot.MultiSaveAndRemoveWithPrefix(nil, []string{k, oldKBefore210}, ts)
}

func (kc *Catalog) GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	_, vals, err := kc.Snapshot.LoadWithPrefix(CollectionMetaPrefix, ts)
	if err != nil {
		log.Warn("get collection meta fail", zap.String("collectionName", collectionName), zap.Error(err))
		return nil, err
	}

	for _, val := range vals {
		colMeta := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(val), &colMeta)
		if err != nil {
			log.Warn("get collection meta unmarshal fail", zap.String("collectionName", collectionName), zap.Error(err))
			continue
		}
		if colMeta.Schema.Name == collectionName {
			// compatibility handled by kc.GetCollectionByID.
			return kc.GetCollectionByID(ctx, colMeta.GetID(), ts)
		}
	}

	return nil, common.NewCollectionNotExistError(fmt.Sprintf("can't find collection: %s, at timestamp = %d", collectionName, ts))
}

func (kc *Catalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	_, vals, err := kc.Snapshot.LoadWithPrefix(CollectionMetaPrefix, ts)
	if err != nil {
		log.Error("get collections meta fail",
			zap.String("prefix", CollectionMetaPrefix),
			zap.Uint64("timestamp", ts),
			zap.Error(err))
		return nil, err
	}

	colls := make(map[string]*model.Collection)
	for _, val := range vals {
		collMeta := pb.CollectionInfo{}
		err := proto.Unmarshal([]byte(val), &collMeta)
		if err != nil {
			log.Warn("unmarshal collection info failed", zap.Error(err))
			continue
		}
		collection, err := kc.appendPartitionAndFieldsInfo(ctx, &collMeta, ts)
		if err != nil {
			return nil, err
		}
		colls[collMeta.Schema.Name] = collection
	}

	return colls, nil
}

func (kc *Catalog) listAliasesBefore210(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error) {
	_, values, err := kc.Snapshot.LoadWithPrefix(CollectionAliasMetaPrefix210, ts)
	if err != nil {
		return nil, err
	}
	// aliases before 210 stored by CollectionInfo.
	aliases := make([]*model.Alias, 0, len(values))
	for _, value := range values {
		coll := &pb.CollectionInfo{}
		err := proto.Unmarshal([]byte(value), coll)
		if err != nil {
			return nil, err
		}
		aliases = append(aliases, &model.Alias{
			Name:         coll.GetSchema().GetName(),
			CollectionID: coll.GetID(),
			CreatedTime:  0, // not accurate.
		})
	}
	return aliases, nil
}

func (kc *Catalog) listAliasesAfter210(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error) {
	_, values, err := kc.Snapshot.LoadWithPrefix(AliasMetaPrefix, ts)
	if err != nil {
		return nil, err
	}
	// aliases after 210 stored by AliasInfo.
	aliases := make([]*model.Alias, 0, len(values))
	for _, value := range values {
		info := &pb.AliasInfo{}
		err := proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, err
		}
		aliases = append(aliases, &model.Alias{
			Name:         info.GetAliasName(),
			CollectionID: info.GetCollectionId(),
			CreatedTime:  info.GetCreatedTime(),
		})
	}
	return aliases, nil
}

func (kc *Catalog) ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error) {
	aliases1, err := kc.listAliasesBefore210(ctx, ts)
	if err != nil {
		return nil, err
	}
	aliases2, err := kc.listAliasesAfter210(ctx, ts)
	if err != nil {
		return nil, err
	}
	aliases := append(aliases1, aliases2...)
	return aliases, nil
}

func (kc *Catalog) ListCredentials(ctx context.Context) ([]string, error) {
	keys, _, err := kc.Txn.LoadWithPrefix(CredentialPrefix)
	if err != nil {
		log.Error("list all credential usernames fail", zap.String("prefix", CredentialPrefix), zap.Error(err))
		return nil, err
	}

	var usernames []string
	for _, path := range keys {
		username := typeutil.After(path, UserSubPrefix+"/")
		if len(username) == 0 {
			log.Warn("no username extract from path:", zap.String("path", path))
			continue
		}
		usernames = append(usernames, username)
	}

	return usernames, nil
}

func (kc *Catalog) save(k string) error {
	var err error
	if _, err = kc.Txn.Load(k); err != nil && !common.IsKeyNotExistError(err) {
		return err
	}
	if err == nil {
		log.Debug("the key has existed", zap.String("key", k))
		return common.NewIgnorableError(fmt.Errorf("the key[%s] is existed", k))
	}
	return kc.Txn.Save(k, "")
}

func (kc *Catalog) remove(k string) error {
	var err error
	if _, err = kc.Txn.Load(k); err != nil && !common.IsKeyNotExistError(err) {
		return err
	}
	if err != nil && common.IsKeyNotExistError(err) {
		return common.NewIgnorableError(fmt.Errorf("the key[%s] isn't existed", k))
	}
	return kc.Txn.Remove(k)
}

func (kc *Catalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	k := funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, entity.Name)
	err := kc.save(k)
	if err != nil && !common.IsIgnorableError(err) {
		log.Warn("fail to save the role", zap.String("key", k), zap.Error(err))
	}
	return err
}

func (kc *Catalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	k := funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, roleName)
	err := kc.Txn.Remove(k)
	if err != nil {
		log.Error("fail to drop role", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	k := funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, fmt.Sprintf("%s/%s", userEntity.Name, roleEntity.Name))
	var err error
	if operateType == milvuspb.OperateUserRoleType_AddUserToRole {
		err = kc.save(k)
		if err != nil {
			log.Error("fail to save the user-role", zap.String("key", k), zap.Error(err))
		}
	} else if operateType == milvuspb.OperateUserRoleType_RemoveUserFromRole {
		err = kc.remove(k)
		if err != nil {
			log.Error("fail to remove the user-role", zap.String("key", k), zap.Error(err))
		}
	} else {
		err = fmt.Errorf("invalid operate user role type, operate type: %d", operateType)
	}
	return err
}

func (kc *Catalog) ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	var results []*milvuspb.RoleResult

	roleToUsers := make(map[string][]string)
	if includeUserInfo {
		roleMappingKey := funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, "")
		keys, _, err := kc.Txn.LoadWithPrefix(roleMappingKey)
		if err != nil {
			log.Error("fail to load role mappings", zap.String("key", roleMappingKey), zap.Error(err))
			return results, err
		}

		for _, key := range keys {
			roleMappingInfos := typeutil.AfterN(key, roleMappingKey+"/", "/")
			if len(roleMappingInfos) != 2 {
				log.Warn("invalid role mapping key", zap.String("string", key), zap.String("sub_string", roleMappingKey))
				continue
			}
			username := roleMappingInfos[0]
			roleName := roleMappingInfos[1]
			roleToUsers[roleName] = append(roleToUsers[roleName], username)
		}
	}

	appendRoleResult := func(roleName string) {
		var users []*milvuspb.UserEntity
		for _, username := range roleToUsers[roleName] {
			users = append(users, &milvuspb.UserEntity{Name: username})
		}
		results = append(results, &milvuspb.RoleResult{
			Role:  &milvuspb.RoleEntity{Name: roleName},
			Users: users,
		})
	}

	if entity == nil {
		roleKey := funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, "")
		keys, _, err := kc.Txn.LoadWithPrefix(roleKey)
		if err != nil {
			log.Error("fail to load roles", zap.String("key", roleKey), zap.Error(err))
			return results, err
		}
		for _, key := range keys {
			infoArr := typeutil.AfterN(key, roleKey+"/", "/")
			if len(infoArr) != 1 || len(infoArr[0]) == 0 {
				log.Warn("invalid role key", zap.String("string", key), zap.String("sub_string", roleKey))
				continue
			}
			appendRoleResult(infoArr[0])
		}
	} else {
		if funcutil.IsEmptyString(entity.Name) {
			return results, fmt.Errorf("role name in the role entity is empty")
		}
		roleKey := funcutil.HandleTenantForEtcdKey(RolePrefix, tenant, entity.Name)
		_, err := kc.Txn.Load(roleKey)
		if err != nil {
			log.Warn("fail to load a role", zap.String("key", roleKey), zap.Error(err))
			return results, err
		}
		appendRoleResult(entity.Name)
	}

	return results, nil
}

func (kc *Catalog) getRolesByUsername(tenant string, username string) ([]string, error) {
	var roles []string
	k := funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, username)
	keys, _, err := kc.Txn.LoadWithPrefix(k)
	if err != nil {
		log.Error("fail to load role mappings by the username", zap.String("key", k), zap.Error(err))
		return roles, err
	}
	for _, key := range keys {
		roleMappingInfos := typeutil.AfterN(key, k+"/", "/")
		if len(roleMappingInfos) != 1 {
			log.Warn("invalid role mapping key", zap.String("string", key), zap.String("sub_string", k))
			continue
		}
		roles = append(roles, roleMappingInfos[0])
	}
	return roles, nil
}

// getUserResult get the user result by the username. And never return the error because the error means the user isn't added to a role.
func (kc *Catalog) getUserResult(tenant string, username string, includeRoleInfo bool) (*milvuspb.UserResult, error) {
	result := &milvuspb.UserResult{User: &milvuspb.UserEntity{Name: username}}
	if !includeRoleInfo {
		return result, nil
	}
	roleNames, err := kc.getRolesByUsername(tenant, username)
	if err != nil {
		log.Warn("fail to get roles by the username", zap.Error(err))
		return result, err
	}
	var roles []*milvuspb.RoleEntity
	for _, roleName := range roleNames {
		roles = append(roles, &milvuspb.RoleEntity{Name: roleName})
	}
	result.Roles = roles
	return result, nil
}

func (kc *Catalog) ListUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	var (
		usernames []string
		err       error
		results   []*milvuspb.UserResult
	)

	appendUserResult := func(username string) error {
		result, err := kc.getUserResult(tenant, username, includeRoleInfo)
		if err != nil {
			return err
		}
		results = append(results, result)
		return nil
	}

	if entity == nil {
		usernames, err = kc.ListCredentials(ctx)
		if err != nil {
			return results, err
		}
	} else {
		if funcutil.IsEmptyString(entity.Name) {
			return results, fmt.Errorf("username in the user entity is empty")
		}
		_, err = kc.GetCredential(ctx, entity.Name)
		if err != nil {
			return results, err
		}
		usernames = append(usernames, entity.Name)
	}
	for _, username := range usernames {
		err = appendUserResult(username)
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (kc *Catalog) AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	var (
		privilegeName = entity.Grantor.Privilege.Name
		k             = funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", entity.Role.Name, entity.Object.Name, entity.ObjectName))
		idStr         string
		v             string
		err           error
	)

	v, err = kc.Txn.Load(k)
	if err != nil {
		if common.IsKeyNotExistError(err) {
			log.Debug("not found the privilege entity", zap.String("key", k), zap.Any("type", operateType))
		}
		if funcutil.IsRevoke(operateType) {
			if common.IsKeyNotExistError(err) {
				return common.NewIgnorableError(fmt.Errorf("the grant[%s] isn't existed", k))
			}
			log.Warn("fail to load grant privilege entity", zap.String("key", k), zap.Any("type", operateType), zap.Error(err))
			return err
		}
		if !common.IsKeyNotExistError(err) {
			log.Warn("fail to load grant privilege entity", zap.String("key", k), zap.Any("type", operateType), zap.Error(err))
			return err
		}

		idStr = crypto.MD5(k)
		err = kc.Txn.Save(k, idStr)
		if err != nil {
			log.Error("fail to allocate id when altering the grant", zap.Error(err))
			return err
		}
	} else {
		idStr = v
	}
	k = funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, fmt.Sprintf("%s/%s", idStr, privilegeName))
	_, err = kc.Txn.Load(k)
	if err != nil {
		log.Warn("fail to load the grantee id", zap.String("key", k), zap.Error(err))
		if !common.IsKeyNotExistError(err) {
			log.Warn("fail to load the grantee id", zap.String("key", k), zap.Error(err))
			return err
		}
		log.Debug("not found the grantee id", zap.String("key", k))
		if funcutil.IsRevoke(operateType) {
			return common.NewIgnorableError(fmt.Errorf("the grantee-id[%s] isn't existed", k))
		}
		if funcutil.IsGrant(operateType) {
			if err = kc.Txn.Save(k, entity.Grantor.User.Name); err != nil {
				log.Error("fail to save the grantee id", zap.String("key", k), zap.Error(err))
			}
			return err
		}
		return nil
	}
	if funcutil.IsRevoke(operateType) {
		if err = kc.Txn.Remove(k); err != nil {
			log.Error("fail to remove the grantee id", zap.String("key", k), zap.Error(err))
			return err
		}
		return err
	}
	return common.NewIgnorableError(fmt.Errorf("the privilege[%s] has been granted", privilegeName))
}

func (kc *Catalog) ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var entities []*milvuspb.GrantEntity

	var granteeKey string
	appendGrantEntity := func(v string, object string, objectName string) error {
		granteeIDKey := funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, v)
		keys, values, err := kc.Txn.LoadWithPrefix(granteeIDKey)
		if err != nil {
			log.Error("fail to load the grantee ids", zap.String("key", granteeIDKey), zap.Error(err))
			return err
		}
		for i, key := range keys {
			granteeIDInfos := typeutil.AfterN(key, granteeIDKey+"/", "/")
			if len(granteeIDInfos) != 1 {
				log.Warn("invalid grantee id", zap.String("string", key), zap.String("sub_string", granteeIDKey))
				continue
			}
			privilegeName := util.PrivilegeNameForAPI(granteeIDInfos[0])
			if granteeIDInfos[0] == util.AnyWord {
				privilegeName = util.AnyWord
			}
			entities = append(entities, &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: entity.Role.Name},
				Object:     &milvuspb.ObjectEntity{Name: object},
				ObjectName: objectName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: values[i]},
					Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
				},
			})
		}
		return nil
	}

	if !funcutil.IsEmptyString(entity.ObjectName) && entity.Object != nil && !funcutil.IsEmptyString(entity.Object.Name) {
		granteeKey = funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, fmt.Sprintf("%s/%s/%s", entity.Role.Name, entity.Object.Name, entity.ObjectName))
		v, err := kc.Txn.Load(granteeKey)
		if err != nil {
			log.Error("fail to load the grant privilege entity", zap.String("key", granteeKey), zap.Error(err))
			return entities, err
		}
		err = appendGrantEntity(v, entity.Object.Name, entity.ObjectName)
		if err != nil {
			return entities, err
		}
	} else {
		granteeKey = funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, entity.Role.Name)
		keys, values, err := kc.Txn.LoadWithPrefix(granteeKey)
		if err != nil {
			log.Error("fail to load grant privilege entities", zap.String("key", granteeKey), zap.Error(err))
			return entities, err
		}
		for i, key := range keys {
			grantInfos := typeutil.AfterN(key, granteeKey+"/", "/")
			if len(grantInfos) != 2 {
				log.Warn("invalid grantee key", zap.String("string", key), zap.String("sub_string", granteeKey))
				continue
			}
			err = appendGrantEntity(values[i], grantInfos[0], grantInfos[1])
			if err != nil {
				return entities, err
			}
		}
	}

	return entities, nil
}

func (kc *Catalog) DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	var (
		k   = funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, role.Name+"/")
		err error
	)

	if err = kc.Txn.RemoveWithPrefix(k); err != nil {
		log.Error("fail to remove with the prefix", zap.String("key", k), zap.Error(err))
	}
	return err
}

func (kc *Catalog) ListPolicy(ctx context.Context, tenant string) ([]string, error) {
	var grantInfoStrs []string
	granteeKey := funcutil.HandleTenantForEtcdKey(GranteePrefix, tenant, "")
	keys, values, err := kc.Txn.LoadWithPrefix(granteeKey)
	if err != nil {
		log.Error("fail to load all grant privilege entities", zap.String("key", granteeKey), zap.Error(err))
		return []string{}, err
	}

	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey+"/", "/")
		if len(grantInfos) != 3 {
			log.Warn("invalid grantee key", zap.String("string", key), zap.String("sub_string", granteeKey))
			continue
		}
		granteeIDKey := funcutil.HandleTenantForEtcdKey(GranteeIDPrefix, tenant, values[i])
		idKeys, _, err := kc.Txn.LoadWithPrefix(granteeIDKey)
		if err != nil {
			log.Error("fail to load the grantee ids", zap.String("key", granteeIDKey), zap.Error(err))
			return []string{}, err
		}
		for _, idKey := range idKeys {
			granteeIDInfos := typeutil.AfterN(idKey, granteeIDKey+"/", "/")
			if len(granteeIDInfos) != 1 {
				log.Warn("invalid grantee id", zap.String("string", idKey), zap.String("sub_string", granteeIDKey))
				continue
			}
			grantInfoStrs = append(grantInfoStrs,
				funcutil.PolicyForPrivilege(grantInfos[0], grantInfos[1], grantInfos[2], granteeIDInfos[0]))
		}
	}
	return grantInfoStrs, nil
}

func (kc *Catalog) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	var userRoles []string
	k := funcutil.HandleTenantForEtcdKey(RoleMappingPrefix, tenant, "")
	keys, _, err := kc.Txn.LoadWithPrefix(k)
	if err != nil {
		log.Error("fail to load all user-role mappings", zap.String("key", k), zap.Error(err))
		return []string{}, err
	}

	for _, key := range keys {
		userRolesInfos := typeutil.AfterN(key, k+"/", "/")
		if len(userRolesInfos) != 2 {
			log.Warn("invalid user-role key", zap.String("string", key), zap.String("sub_string", k))
			continue
		}
		userRoles = append(userRoles, funcutil.EncodeUserRoleCache(userRolesInfos[0], userRolesInfos[1]))
	}
	return userRoles, nil
}

func (kc *Catalog) Close() {
	// do nothing
}
