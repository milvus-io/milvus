package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Catalog struct {
	Txn      kv.TxnKV
	Snapshot kv.SnapShotKV
}

func (kc *Catalog) CreateCollection(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, coll.CollectionID)
	collInfo := model.ConvertToCollectionPB(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		log.Error("create collection marshal fail", zap.String("key", k1), zap.Error(err))
		return err
	}

	// save ddOpStr into etcd
	kvs := map[string]string{k1: string(v1)}
	for k, v := range coll.Extra {
		kvs[k] = v
	}

	err = kc.Snapshot.MultiSave(kvs, ts)
	if err != nil {
		log.Error("create collection persist meta fail", zap.String("key", k1), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) CreatePartition(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	k1 := fmt.Sprintf("%s/%d", CollectionMetaPrefix, coll.CollectionID)
	collInfo := model.ConvertToCollectionPB(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		log.Error("create partition marshal fail", zap.String("key", k1), zap.Error(err))
		return err
	}

	kvs := map[string]string{k1: string(v1)}
	err = kc.Snapshot.MultiSave(kvs, ts)
	if err != nil {
		log.Error("create partition persist meta fail", zap.String("key", k1), zap.Error(err))
		return err
	}

	// save ddOpStr into etcd
	err = kc.Txn.MultiSave(coll.Extra)
	if err != nil {
		// will not panic, missing create msg
		log.Warn("create partition persist ddop meta fail", zap.Int64("collectionID", coll.CollectionID), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) CreateIndex(ctx context.Context, col *model.Collection, index *model.Index) error {
	k1 := path.Join(CollectionMetaPrefix, strconv.FormatInt(col.CollectionID, 10))
	v1, err := proto.Marshal(model.ConvertToCollectionPB(col))
	if err != nil {
		log.Error("create index marshal fail", zap.String("key", k1), zap.Error(err))
		return err
	}

	k2 := path.Join(IndexMetaPrefix, strconv.FormatInt(index.IndexID, 10))
	v2, err := proto.Marshal(model.ConvertToIndexPB(index))
	if err != nil {
		log.Error("create index marshal fail", zap.String("key", k2), zap.Error(err))
		return err
	}
	meta := map[string]string{k1: string(v1), k2: string(v2)}

	err = kc.Txn.MultiSave(meta)
	if err != nil {
		log.Error("create index persist meta fail", zap.String("key", k1), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index) error {
	kvs := make(map[string]string, len(newIndex.SegmentIndexes))
	for _, segmentIndex := range newIndex.SegmentIndexes {
		segment := segmentIndex.Segment
		k := fmt.Sprintf("%s/%d/%d/%d/%d", SegmentIndexMetaPrefix, newIndex.CollectionID, newIndex.IndexID, segment.PartitionID, segment.SegmentID)
		segIdxInfo := &pb.SegmentIndexInfo{
			CollectionID: newIndex.CollectionID,
			PartitionID:  segment.PartitionID,
			SegmentID:    segment.SegmentID,
			BuildID:      segmentIndex.BuildID,
			EnableIndex:  segmentIndex.EnableIndex,
			FieldID:      newIndex.FieldID,
			IndexID:      newIndex.IndexID,
		}

		v, err := proto.Marshal(segIdxInfo)
		if err != nil {
			log.Error("alter index marshal fail", zap.String("key", k), zap.Error(err))
			return err
		}

		kvs[k] = string(v)
	}

	err := kc.Txn.MultiSave(kvs)
	if err != nil {
		log.Error("alter index persist meta fail", zap.Any("segmentIndex", newIndex.SegmentIndexes), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) CreateAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	k := fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, collection.Aliases[0])
	v, err := proto.Marshal(&pb.CollectionInfo{ID: collection.CollectionID, Schema: &schemapb.CollectionSchema{Name: collection.Aliases[0]}})
	if err != nil {
		log.Error("create alias marshal fail", zap.String("key", k), zap.Error(err))
		return err
	}

	err = kc.Snapshot.Save(k, string(v), ts)
	if err != nil {
		log.Error("create alias persist meta fail", zap.String("key", k), zap.Error(err))
		return err
	}

	return nil
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

func (kc *Catalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	collKey := fmt.Sprintf("%s/%d", CollectionMetaPrefix, collectionID)
	collVal, err := kc.Snapshot.Load(collKey, ts)
	if err != nil {
		log.Error("get collection meta fail", zap.String("key", collKey), zap.Error(err))
		return nil, err
	}

	collMeta := &pb.CollectionInfo{}
	err = proto.Unmarshal([]byte(collVal), collMeta)
	if err != nil {
		log.Error("collection meta marshal fail", zap.String("key", collKey), zap.Error(err))
		return nil, err
	}

	return model.ConvertCollectionPBToModel(collMeta, map[string]string{}), nil
}

func (kc *Catalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	_, err := kc.GetCollectionByID(ctx, collectionID, ts)
	return err == nil
}

func (kc *Catalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	k := fmt.Sprintf("%s/%s", CredentialPrefix, username)
	v, err := kc.Txn.Load(k)
	if err != nil {
		log.Warn("get credential meta fail", zap.String("key", k), zap.Error(err))
		return nil, err
	}

	credentialInfo := internalpb.CredentialInfo{}
	err = json.Unmarshal([]byte(v), &credentialInfo)
	if err != nil {
		return nil, fmt.Errorf("unmarshal credential info err:%w", err)
	}

	return &model.Credential{Username: username, EncryptedPassword: credentialInfo.EncryptedPassword}, nil
}

func (kc *Catalog) AlterAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return kc.CreateAlias(ctx, collection, ts)
}

func (kc *Catalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	delMetakeysSnap := []string{
		fmt.Sprintf("%s/%d", CollectionMetaPrefix, collectionInfo.CollectionID),
	}
	for _, alias := range collectionInfo.Aliases {
		delMetakeysSnap = append(delMetakeysSnap,
			fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, alias),
		)
	}

	err := kc.Snapshot.MultiSaveAndRemoveWithPrefix(map[string]string{}, delMetakeysSnap, ts)
	if err != nil {
		log.Error("drop collection update meta fail", zap.Int64("collectionID", collectionInfo.CollectionID), zap.Error(err))
		return err
	}

	// Txn operation
	kvs := map[string]string{}
	for k, v := range collectionInfo.Extra {
		kvs[k] = v
	}

	delMetaKeysTxn := []string{
		fmt.Sprintf("%s/%d", SegmentIndexMetaPrefix, collectionInfo.CollectionID),
		fmt.Sprintf("%s/%d", IndexMetaPrefix, collectionInfo.CollectionID),
	}

	err = kc.Txn.MultiSaveAndRemoveWithPrefix(kvs, delMetaKeysTxn)
	if err != nil {
		log.Warn("drop collection update meta fail", zap.Int64("collectionID", collectionInfo.CollectionID), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) DropPartition(ctx context.Context, collectionInfo *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta := model.ConvertToCollectionPB(collectionInfo)
	k := path.Join(CollectionMetaPrefix, strconv.FormatInt(collectionInfo.CollectionID, 10))
	v, err := proto.Marshal(collMeta)
	if err != nil {
		log.Error("drop partition marshal fail", zap.String("key", k), zap.Error(err))
		return err
	}

	err = kc.Snapshot.Save(k, string(v), ts)
	if err != nil {
		log.Error("drop partition update collection meta fail",
			zap.Int64("collectionID", collectionInfo.CollectionID),
			zap.Int64("partitionID", partitionID),
			zap.Error(err))
		return err
	}

	var delMetaKeys []string
	for _, idxInfo := range collMeta.FieldIndexes {
		k := fmt.Sprintf("%s/%d/%d/%d", SegmentIndexMetaPrefix, collMeta.ID, idxInfo.IndexID, partitionID)
		delMetaKeys = append(delMetaKeys, k)
	}

	// Txn operation
	metaTxn := map[string]string{}
	for k, v := range collectionInfo.Extra {
		metaTxn[k] = v
	}
	err = kc.Txn.MultiSaveAndRemoveWithPrefix(metaTxn, delMetaKeys)
	if err != nil {
		log.Warn("drop partition update meta fail",
			zap.Int64("collectionID", collectionInfo.CollectionID),
			zap.Int64("partitionID", partitionID),
			zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta := model.ConvertToCollectionPB(collectionInfo)
	k := path.Join(CollectionMetaPrefix, strconv.FormatInt(collectionInfo.CollectionID, 10))
	v, err := proto.Marshal(collMeta)
	if err != nil {
		log.Error("drop index marshal fail", zap.String("key", k), zap.Error(err))
		return err
	}

	saveMeta := map[string]string{k: string(v)}

	delMeta := []string{
		fmt.Sprintf("%s/%d/%d", SegmentIndexMetaPrefix, collectionInfo.CollectionID, dropIdxID),
		fmt.Sprintf("%s/%d/%d", IndexMetaPrefix, collectionInfo.CollectionID, dropIdxID),
	}

	err = kc.Txn.MultiSaveAndRemoveWithPrefix(saveMeta, delMeta)
	if err != nil {
		log.Error("drop partition update meta fail",
			zap.Int64("collectionID", collectionInfo.CollectionID),
			zap.Int64("indexID", dropIdxID),
			zap.Error(err))
		return err
	}

	return nil
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

func (kc *Catalog) DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error {
	delMetakeys := []string{
		fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix, alias),
	}

	meta := make(map[string]string)
	err := kc.Snapshot.MultiSaveAndRemoveWithPrefix(meta, delMetakeys, ts)
	if err != nil {
		log.Error("drop alias update meta fail", zap.String("alias", alias), zap.Error(err))
		return err
	}

	return nil
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
			return model.ConvertCollectionPBToModel(&colMeta, map[string]string{}), nil
		}
	}

	return nil, fmt.Errorf("can't find collection: %s, at timestamp = %d", collectionName, ts)
}

func (kc *Catalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	_, vals, err := kc.Snapshot.LoadWithPrefix(CollectionMetaPrefix, ts)
	if err != nil {
		log.Error("get collections meta fail",
			zap.String("prefix", CollectionMetaPrefix),
			zap.Uint64("timestamp", ts),
			zap.Error(err))
		return nil, nil
	}

	colls := make(map[string]*model.Collection)
	for _, val := range vals {
		collMeta := pb.CollectionInfo{}
		err := proto.Unmarshal([]byte(val), &collMeta)
		if err != nil {
			log.Warn("unmarshal collection info failed", zap.Error(err))
			continue
		}
		colls[collMeta.Schema.Name] = model.ConvertCollectionPBToModel(&collMeta, map[string]string{})
	}

	return colls, nil
}

func (kc *Catalog) ListAliases(ctx context.Context) ([]*model.Collection, error) {
	_, values, err := kc.Snapshot.LoadWithPrefix(CollectionAliasMetaPrefix, 0)
	if err != nil {
		log.Error("get aliases meta fail", zap.String("prefix", CollectionAliasMetaPrefix), zap.Error(err))
		return nil, err
	}

	var colls []*model.Collection
	for _, value := range values {
		aliasInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &aliasInfo)
		if err != nil {
			log.Warn("unmarshal aliases failed", zap.Error(err))
			continue
		}
		colls = append(colls, model.ConvertCollectionPBToModel(&aliasInfo, map[string]string{}))
	}

	return colls, nil
}

func (kc *Catalog) listSegmentIndexes(ctx context.Context) (map[int64]*model.Index, error) {
	_, values, err := kc.Txn.LoadWithPrefix(SegmentIndexMetaPrefix)
	if err != nil {
		log.Error("list segment index meta fail", zap.String("prefix", SegmentIndexMetaPrefix), zap.Error(err))
		return nil, err
	}

	indexes := make(map[int64]*model.Index, len(values))
	for _, value := range values {
		if bytes.Equal([]byte(value), SuffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		segmentIndexInfo := pb.SegmentIndexInfo{}
		err = proto.Unmarshal([]byte(value), &segmentIndexInfo)
		if err != nil {
			log.Warn("unmarshal segment index info failed", zap.Error(err))
			continue
		}

		newIndex := model.ConvertSegmentIndexPBToModel(&segmentIndexInfo)
		oldIndex, ok := indexes[segmentIndexInfo.IndexID]
		if ok {
			for segID, segmentIdxInfo := range newIndex.SegmentIndexes {
				oldIndex.SegmentIndexes[segID] = segmentIdxInfo
			}
		} else {
			indexes[segmentIndexInfo.IndexID] = newIndex
		}
	}

	return indexes, nil
}

func (kc *Catalog) listIndexMeta(ctx context.Context) (map[int64]*model.Index, error) {
	_, values, err := kc.Txn.LoadWithPrefix(IndexMetaPrefix)
	if err != nil {
		log.Error("list index meta fail", zap.String("prefix", IndexMetaPrefix), zap.Error(err))
		return nil, err
	}

	indexes := make(map[int64]*model.Index, len(values))
	for _, value := range values {
		if bytes.Equal([]byte(value), SuffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		meta := pb.IndexInfo{}
		err = proto.Unmarshal([]byte(value), &meta)
		if err != nil {
			log.Warn("unmarshal index info failed", zap.Error(err))
			continue
		}

		index := model.ConvertIndexPBToModel(&meta)
		if _, ok := indexes[meta.IndexID]; ok {
			log.Warn("duplicated index id exists in index meta", zap.Int64("index id", meta.IndexID))
		}

		indexes[meta.IndexID] = index
	}

	return indexes, nil
}

func (kc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	indexMeta, err := kc.listIndexMeta(ctx)
	if err != nil {
		return nil, err
	}

	segmentIndexMeta, err := kc.listSegmentIndexes(ctx)
	if err != nil {
		return nil, err
	}

	var indexes []*model.Index
	//merge index and segment index
	for indexID, index := range indexMeta {
		segmentIndex, ok := segmentIndexMeta[indexID]
		if ok {
			index = model.MergeIndexModel(index, segmentIndex)
			delete(segmentIndexMeta, indexID)
		}
		indexes = append(indexes, index)
	}

	// add remain segmentIndexMeta
	for _, index := range segmentIndexMeta {
		indexes = append(indexes, index)
	}

	return indexes, nil
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

func (kc *Catalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	k := fmt.Sprintf("%s/%s/%s", RolePrefix, tenant, entity.Name)
	k = model.RemoveInvalidSeparator(k)
	err := kc.Txn.Save(k, "")
	if err != nil {
		log.Error("fail to create role", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	k := fmt.Sprintf("%s/%s/%s", RolePrefix, tenant, roleName)
	k = model.RemoveInvalidSeparator(k)
	err := kc.Txn.Remove(k)
	if err != nil {
		log.Error("fail to drop role", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	k := fmt.Sprintf("%s/%s/%s/%s", RoleMappingPrefix, tenant, userEntity.Name, roleEntity.Name)
	k = model.RemoveInvalidSeparator(k)
	var err error
	if operateType == milvuspb.OperateUserRoleType_AddUserToRole {
		err = kc.Txn.Save(k, "")
		if err != nil {
			log.Error("fail to add user to role", zap.String("key", k), zap.Error(err))
		}
	} else if operateType == milvuspb.OperateUserRoleType_RemoveUserFromRole {
		err = kc.Txn.Remove(k)
		if err != nil {
			log.Error("fail to remove user from role", zap.String("key", k), zap.Error(err))
		}
	} else {
		err = fmt.Errorf("invalid operate user role type, operate type: %d", operateType)
	}
	return err
}

func (kc *Catalog) SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	var results []*milvuspb.RoleResult

	roleToUsers := make(map[string][]string)
	if includeUserInfo {
		roleMappingKey := fmt.Sprintf("%s/%s", RoleMappingPrefix, tenant)
		roleMappingKey = model.RemoveInvalidSeparator(roleMappingKey)
		keys, _, err := kc.Txn.LoadWithPrefix(roleMappingKey)
		if err != nil {
			log.Error("fail to load role mappings", zap.Error(err))
			return results, err
		}

		for _, key := range keys {
			roleMappingInfos := typeutil.AfterN(key, roleMappingKey+"/", "/")
			if len(roleMappingInfos) != 2 {
				log.Warn("invalid role mapping key", zap.String("role_mapping_key", key))
				continue
			}
			username := roleMappingInfos[0]
			roleName := roleMappingInfos[1]
			roleToUsers[roleName] = append(roleToUsers[roleName], username)
		}
	}

	if entity == nil {
		roleKey := fmt.Sprintf("%s/%s", RolePrefix, tenant)
		roleKey = model.RemoveInvalidSeparator(roleKey)
		keys, _, err := kc.Txn.LoadWithPrefix(roleKey)
		if err != nil {
			log.Error("fail to load roles", zap.String("key", roleKey), zap.Error(err))
			return results, err
		}
		for _, key := range keys {
			infoArr := typeutil.AfterN(key, roleKey+"/", "/")
			if len(infoArr) != 1 || len(infoArr[0]) == 0 {
				log.Warn("invalid role key", zap.String("key", key))
				continue
			}
			var users []*milvuspb.UserEntity
			for _, username := range roleToUsers[infoArr[0]] {
				users = append(users, &milvuspb.UserEntity{Name: username})
			}
			results = append(results, &milvuspb.RoleResult{
				Role:  &milvuspb.RoleEntity{Name: infoArr[0]},
				Users: users,
			})
		}
	} else {
		roleKey := fmt.Sprintf("%s/%s/%s", RolePrefix, tenant, entity.Name)
		roleKey = model.RemoveInvalidSeparator(roleKey)
		if model.IsEmptyString(entity.Name) {
			return results, fmt.Errorf("role name in the role entity is empty")
		}
		_, err := kc.Txn.Load(roleKey)
		if err != nil {
			log.Error("fail to load a role", zap.String("key", roleKey), zap.Error(err))
			return results, err
		}
		var users []*milvuspb.UserEntity
		for _, username := range roleToUsers[entity.Name] {
			users = append(users, &milvuspb.UserEntity{Name: username})
		}
		results = append(results, &milvuspb.RoleResult{
			Role:  &milvuspb.RoleEntity{Name: entity.Name},
			Users: users,
		})
	}

	return results, nil
}

func (kc *Catalog) getRolesByUsername(tenant string, username string) ([]string, error) {
	var roles []string
	k := fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, tenant, username)
	k = model.RemoveInvalidSeparator(k)
	keys, _, err := kc.Txn.LoadWithPrefix(k)
	if err != nil {
		log.Error("fail to load role mappings by the username", zap.String("key", k), zap.Error(err))
		return roles, err
	}
	for _, key := range keys {
		roleMappingInfos := typeutil.AfterN(key, k+"/", "/")
		if len(roleMappingInfos) != 1 {
			log.Warn("invalid role mapping key", zap.String("key", key))
			continue
		}
		roles = append(roles, roleMappingInfos[0])
	}
	return roles, nil
}

// getUserResult get the user result by the username. And never return the error because the error means the user isn't added to a role.
func (kc *Catalog) getUserResult(tenant string, username string, includeRoleInfo bool) *milvuspb.UserResult {
	result := &milvuspb.UserResult{User: &milvuspb.UserEntity{Name: username}}
	if !includeRoleInfo {
		return result
	}
	roleNames, err := kc.getRolesByUsername(tenant, username)
	if err != nil {
		log.Warn("fail to get roles by the username", zap.Error(err))
		return result
	}
	var roles []*milvuspb.RoleEntity
	for _, roleName := range roleNames {
		roles = append(roles, &milvuspb.RoleEntity{Name: roleName})
	}
	result.Roles = roles
	return result
}

func (kc *Catalog) SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	var results []*milvuspb.UserResult

	if entity == nil {
		usernames, err := kc.ListCredentials(ctx)
		if err != nil {
			return results, err
		}
		for _, username := range usernames {
			result := kc.getUserResult(tenant, username, includeRoleInfo)
			results = append(results, result)
		}
	} else {
		if model.IsEmptyString(entity.Name) {
			return results, fmt.Errorf("username in the user entity is empty")
		}
		_, err := kc.GetCredential(ctx, entity.Name)
		if err != nil {
			return results, err
		}
		result := kc.getUserResult(tenant, entity.Name, includeRoleInfo)
		results = append(results, result)
	}
	return results, nil
}

func (kc *Catalog) OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	principalName := model.GetPrincipalName(entity)
	privilegeName := util.PrivilegeNameForDb(entity.Grantor.Privilege.Name)
	k := fmt.Sprintf("%s/%s/%s/%s/%s/%s", GranteePrefix, tenant, entity.Principal.PrincipalType,
		principalName, entity.Resource.Type, entity.ResourceName)
	k = model.RemoveInvalidSeparator(k)

	curGrantPrivilegeEntity := &milvuspb.GrantPrivilegeEntity{}
	v, err := kc.Txn.Load(k)
	if err != nil {
		log.Warn("fail to load grant privilege entity", zap.String("key", k), zap.Any("type", operateType), zap.Error(err))
		if model.IsRevoke(operateType) {
			return err
		}
		curGrantPrivilegeEntity.Entities = append(curGrantPrivilegeEntity.Entities, &milvuspb.GrantorEntity{
			Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
			User:      &milvuspb.UserEntity{Name: entity.Grantor.User.Name},
		})
	} else {
		err = proto.Unmarshal([]byte(v), curGrantPrivilegeEntity)
		if err != nil {
			log.Error("fail to unmarshal the grant privilege entity", zap.String("key", k), zap.Any("type", operateType), zap.Error(err))
			return err
		}
		isExisted := false
		dropIndex := -1

		for entityIndex, grantorEntity := range curGrantPrivilegeEntity.Entities {
			if grantorEntity.User.Name == entity.Grantor.User.Name && grantorEntity.Privilege.Name == privilegeName {
				isExisted = true
				dropIndex = entityIndex
				break
			}
		}
		if !isExisted && model.IsGrant(operateType) {
			curGrantPrivilegeEntity.Entities = append(curGrantPrivilegeEntity.Entities, &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
				User:      &milvuspb.UserEntity{Name: entity.Grantor.User.Name},
			})
		} else if isExisted && model.IsGrant(operateType) {
			return nil
		} else if !isExisted && model.IsRevoke(operateType) {
			return fmt.Errorf("fail to revoke the privilege because the privilege isn't granted for the principal, key: /%s", k)
		} else if isExisted && model.IsRevoke(operateType) {
			curGrantPrivilegeEntity.Entities = append(curGrantPrivilegeEntity.Entities[:dropIndex], curGrantPrivilegeEntity.Entities[dropIndex+1:]...)
		}
	}

	if model.IsRevoke(operateType) && len(curGrantPrivilegeEntity.Entities) == 0 {
		err = kc.Txn.Remove(k)
		if err != nil {
			log.Error("fail to remove the grant privilege entity", zap.String("key", k), zap.Error(err))
			return err
		}
		return nil
	}

	saveValue, err := proto.Marshal(curGrantPrivilegeEntity)
	if err != nil {
		log.Error("fail to marshal the grant privilege entity", zap.String("key", k), zap.Any("type", operateType), zap.Error(err))
		return fmt.Errorf("fail to marshal grant info, key:%s, err:%w", k, err)
	}
	err = kc.Txn.Save(k, string(saveValue))
	if err != nil {
		log.Error("fail to save the grant privilege entity", zap.String("key", k), zap.Any("type", operateType), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var entities []*milvuspb.GrantEntity
	if entity.Principal == nil || model.IsEmptyString(entity.Principal.PrincipalType) {
		return entities, fmt.Errorf("the principal type in the grant entity is empty")
	}
	principalName := model.GetPrincipalName(entity)
	if model.IsEmptyString(principalName) {
		return entities, fmt.Errorf("the principal name in the grant entity is empty")
	}

	if !model.IsEmptyString(entity.ResourceName) && entity.Resource != nil && !model.IsEmptyString(entity.Resource.Type) {
		k := fmt.Sprintf("%s/%s/%s/%s/%s/%s", GranteePrefix, tenant, entity.Principal.PrincipalType,
			principalName, entity.Resource.Type, entity.ResourceName)
		k = model.RemoveInvalidSeparator(k)
		v, err := kc.Txn.Load(k)
		if err != nil {
			log.Error("fail to load the grant privilege entity", zap.String("key", k), zap.Error(err))
			return entities, err
		}
		grantPrivilegeEntity := &milvuspb.GrantPrivilegeEntity{}
		err = proto.Unmarshal([]byte(v), grantPrivilegeEntity)
		if err != nil {
			log.Error("fail to unmarshal the grant privilege entity", zap.String("key", k), zap.Error(err))
			return entities, err
		}
		for _, grantorEntity := range grantPrivilegeEntity.Entities {
			entities = append(entities, &milvuspb.GrantEntity{
				Principal:    model.GetPrincipalEntity(entity.Principal.PrincipalType, principalName),
				Resource:     &milvuspb.ResourceEntity{Type: entity.Resource.Type},
				ResourceName: entity.ResourceName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: grantorEntity.User.Name},
					Privilege: &milvuspb.PrivilegeEntity{Name: util.PrivilegeNameForAPI(grantorEntity.Privilege.Name)},
				},
			})
		}
	} else {
		k := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, tenant, entity.Principal.PrincipalType,
			principalName)
		k = model.RemoveInvalidSeparator(k)
		keys, values, err := kc.Txn.LoadWithPrefix(k)
		if err != nil {
			log.Error("fail to load grant privilege entities", zap.String("key", k), zap.Error(err))
			return entities, err
		}
		for i, key := range keys {
			grantInfos := typeutil.AfterN(key, k+"/", "/")
			if len(grantInfos) != 2 {
				log.Warn("invalid grant key", zap.String("key", key))
				continue
			}
			grantPrivilegeEntity := &milvuspb.GrantPrivilegeEntity{}
			err = proto.Unmarshal([]byte(values[i]), grantPrivilegeEntity)
			if err != nil {
				log.Warn("fail to the grant privilege entity", zap.String("key", key), zap.Error(err))
				continue
			}
			for _, grantorEntity := range grantPrivilegeEntity.Entities {
				entities = append(entities, &milvuspb.GrantEntity{
					Principal:    model.GetPrincipalEntity(entity.Principal.PrincipalType, principalName),
					Resource:     &milvuspb.ResourceEntity{Type: grantInfos[0]},
					ResourceName: grantInfos[1],
					Grantor: &milvuspb.GrantorEntity{
						User:      &milvuspb.UserEntity{Name: grantorEntity.User.Name},
						Privilege: &milvuspb.PrivilegeEntity{Name: grantorEntity.Privilege.Name},
					},
				})
			}
		}
	}

	return entities, nil
}

func (kc *Catalog) ListPolicy(ctx context.Context, tenant string) []string {
	var grantInfoStrs []string
	k := fmt.Sprintf("%s/%s", GranteePrefix, tenant)
	k = model.RemoveInvalidSeparator(k)
	keys, values, err := kc.Txn.LoadWithPrefix(k)
	if err != nil {
		log.Error("fail to load all grant privilege entities", zap.String("key", k), zap.Error(err))
	} else {
		for i, key := range keys {
			grantInfos := typeutil.AfterN(key, k+"/", "/")
			if len(grantInfos) != 4 {
				log.Warn("invalid grant key", zap.String("grant_key", key))
				continue
			}
			grantPrivilegeEntity := &milvuspb.GrantPrivilegeEntity{}
			err = proto.Unmarshal([]byte(values[i]), grantPrivilegeEntity)
			if err != nil {
				log.Warn("fail to unmarshal the grant privilege entity", zap.String("key", key), zap.Error(err))
				continue
			}
			for _, grantorInfo := range grantPrivilegeEntity.Entities {
				grantInfoStrs = append(grantInfoStrs,
					funcutil.PolicyForPrivilege(grantInfos[1], grantInfos[2], grantInfos[3], grantorInfo.Privilege.Name))
			}
		}
	}

	var roleMappingInfoStrs []string
	results, err := kc.SelectRole(ctx, tenant, nil, true)
	if err != nil {
		log.Error("fail to load the role info", zap.Error(err))
	} else {
		for _, result := range results {
			for _, user := range result.Users {
				roleMappingInfoStrs = append(roleMappingInfoStrs, funcutil.PolicyForRole(user.Name, result.Role.Name))
			}
		}
	}

	return append(grantInfoStrs, roleMappingInfoStrs...)
}

func (kc *Catalog) Close() {
	// do nothing
}
