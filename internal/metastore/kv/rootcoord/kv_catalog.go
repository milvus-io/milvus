package rootcoord

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/log"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// prefix/collection/collection_id 					-> CollectionInfo
// prefix/partitions/collection_id/partition_id		-> PartitionInfo
// prefix/aliases/alias_name						-> AliasInfo
// prefix/fields/collection_id/field_id				-> FieldSchema
// prefix/file_resource/resource_id             -> Resource

type Catalog struct {
	Txn kv.TxnKV

	pool *conc.Pool[any]
}

func NewCatalog(metaKV kv.TxnKV) metastore.RootCoordCatalog {
	ioPool := conc.NewPool[any](paramtable.Get().MetaStoreCfg.ReadConcurrency.GetAsInt())
	return &Catalog{Txn: metaKV, pool: ioPool}
}

// grantMigrationToIDKey returns the tenant-scoped flag key for grant migration completion.
// For empty tenant (current default), it returns the same key as the old global constant
// for backward compatibility.
func grantMigrationToIDKey(tenant string) string {
	if tenant == "" {
		return GrantMigrationToIDKeyPrefix
	}
	return GrantMigrationToIDKeyPrefix + "/" + tenant
}

func BuildCollectionKey(dbID typeutil.UniqueID, collectionID typeutil.UniqueID) string {
	if dbID != util.NonDBID {
		return BuildCollectionKeyWithDBID(dbID, collectionID)
	}
	return fmt.Sprintf("%s/%d", CollectionMetaPrefix, collectionID)
}

func BuildPartitionPrefix(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/", PartitionMetaPrefix, collectionID)
}

func BuildPartitionKey(collectionID, partitionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d", PartitionMetaPrefix, collectionID, partitionID)
}

func BuildFieldPrefix(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/", FieldMetaPrefix, collectionID)
}

func BuildFieldKey(collectionID typeutil.UniqueID, fieldID int64) string {
	return fmt.Sprintf("%s/%d/%d", FieldMetaPrefix, collectionID, fieldID)
}

func BuildFunctionPrefix(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/", FunctionMetaPrefix, collectionID)
}

func BuildFunctionKey(collectionID typeutil.UniqueID, functionID int64) string {
	return fmt.Sprintf("%s/%d/%d", FunctionMetaPrefix, collectionID, functionID)
}

func BuildStructArrayFieldPrefix(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/", StructArrayFieldMetaPrefix, collectionID)
}

func BuildStructArrayFieldKey(collectionId typeutil.UniqueID, fieldId int64) string {
	return fmt.Sprintf("%s/%d/%d", StructArrayFieldMetaPrefix, collectionId, fieldId)
}

func BuildAliasKey210(alias string) string {
	return fmt.Sprintf("%s/%s", CollectionAliasMetaPrefix210, alias)
}

func BuildAliasKey(aliasName string) string {
	return fmt.Sprintf("%s/%s", AliasMetaPrefix, aliasName)
}

func BuildAliasKeyWithDB(dbID int64, aliasName string) string {
	k := BuildAliasKey(aliasName)
	if dbID == util.NonDBID {
		return k
	}
	return fmt.Sprintf("%s/%s/%d/%s", DatabaseMetaPrefix, Aliases, dbID, aliasName)
}

func BuildAliasPrefixWithDB(dbID int64) string {
	if dbID == util.NonDBID {
		return AliasMetaPrefix + "/"
	}
	return fmt.Sprintf("%s/%s/%d/", DatabaseMetaPrefix, Aliases, dbID)
}

func batchMultiSaveAndRemove(ctx context.Context, txn kv.TxnKV, limit int, saves map[string]string, removals []string) error {
	saveFn := func(partialKvs map[string]string) error {
		return txn.MultiSave(ctx, partialKvs)
	}
	if err := etcd.SaveByBatchWithLimit(saves, limit, saveFn); err != nil {
		return err
	}

	removeFn := func(partialKeys []string) error {
		return txn.MultiSaveAndRemove(ctx, nil, partialKeys)
	}
	return etcd.RemoveByBatchWithLimit(removals, limit, removeFn)
}

func (kc *Catalog) CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error {
	key := BuildDatabaseKey(db.ID)
	dbInfo := model.MarshalDatabaseModel(db)
	v, err := proto.Marshal(dbInfo)
	if err != nil {
		return err
	}
	return kc.Txn.Save(ctx, key, string(v))
}

func (kc *Catalog) AlterDatabase(ctx context.Context, newColl *model.Database, ts typeutil.Timestamp) error {
	key := BuildDatabaseKey(newColl.ID)
	dbInfo := model.MarshalDatabaseModel(newColl)
	v, err := proto.Marshal(dbInfo)
	if err != nil {
		return err
	}
	return kc.Txn.Save(ctx, key, string(v))
}

func (kc *Catalog) DropDatabase(ctx context.Context, dbID int64, ts typeutil.Timestamp) error {
	key := BuildDatabaseKey(dbID)
	return kc.Txn.MultiSaveAndRemove(ctx, nil, []string{key})
}

func (kc *Catalog) ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error) {
	_, vals, err := kc.Txn.LoadWithPrefix(ctx, DBInfoMetaPrefix+"/")
	if err != nil {
		return nil, err
	}

	dbs := make([]*model.Database, 0, len(vals))
	for _, val := range vals {
		// Skip tombstone values left by old SuffixSnapshot (DropDatabase with ts!=0
		// wrote a 3-byte tombstone instead of deleting the key).
		if IsTombstone(val) {
			continue
		}
		dbMeta := &pb.DatabaseInfo{}
		err := proto.Unmarshal([]byte(val), dbMeta)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, model.UnmarshalDatabaseModel(dbMeta))
	}
	return dbs, nil
}

func (kc *Catalog) CreateCollection(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	if coll.State != pb.CollectionState_CollectionCreated {
		return fmt.Errorf("collection state should be created, collection name: %s, collection id: %d, state: %s", coll.Name, coll.CollectionID, coll.State)
	}

	k1 := BuildCollectionKey(coll.DBID, coll.CollectionID)
	collInfo := model.MarshalCollectionModel(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal collection info: %s", err.Error())
	}

	kvs := map[string]string{}

	// save partition info to new path.
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
	// save fields info to new path.
	for _, field := range coll.Fields {
		k := BuildFieldKey(coll.CollectionID, field.FieldID)
		fieldInfo := model.MarshalFieldModel(field)
		v, err := proto.Marshal(fieldInfo)
		if err != nil {
			return err
		}
		kvs[k] = string(v)
	}

	// save struct array fields to new path
	for _, structArrayField := range coll.StructArrayFields {
		k := BuildStructArrayFieldKey(coll.CollectionID, structArrayField.FieldID)
		structArrayFieldInfo := model.MarshalStructArrayFieldModel(structArrayField)
		v, err := proto.Marshal(structArrayFieldInfo)
		if err != nil {
			return err
		}
		kvs[k] = string(v)
	}

	// save functions info to new path.
	for _, function := range coll.Functions {
		k := BuildFunctionKey(coll.CollectionID, function.ID)
		functionInfo := model.MarshalFunctionModel(function)
		v, err := proto.Marshal(functionInfo)
		if err != nil {
			return err
		}
		kvs[k] = string(v)
	}

	// Due to the limit of etcd txn number, we must split these kvs into several batches.
	// Save fields/partitions/functions first, then save the collection key last.
	// If we crash after saving fields but before the collection key, the collection won't be
	// loaded on restart (no collection key = not in collID2Meta), so the DDL ack callback will
	// retry and complete the full write. The orphan field keys will be overwritten on retry.
	// If we crash after saving the collection key, all data is persisted — no issue.
	// This ordering avoids the case where the collection key exists without fields, which would
	// cause the idempotency check in AddCollection to short-circuit and skip saving fields.
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if err := etcd.SaveByBatchWithLimit(kvs, maxTxnNum, func(partialKvs map[string]string) error {
		return kc.Txn.MultiSave(ctx, partialKvs)
	}); err != nil {
		return err
	}

	// Save the collection key last — this is the "commit point" that makes the collection visible.
	return kc.Txn.Save(ctx, k1, string(v1))
}

func (kc *Catalog) loadCollectionFromDb(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
	collKey := BuildCollectionKey(dbID, collectionID)
	collVal, err := kc.Txn.Load(ctx, collKey)
	if err != nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID, err.Error())
	}

	// Legacy SuffixSnapshot deletion with ts!=0 overwrote the plain key with a
	// 3-byte tombstone marker instead of removing it. Treat a tombstone value
	// as "collection logically deleted" to keep restart safe against stale data.
	if IsTombstone(collVal) {
		return nil, merr.WrapErrCollectionNotFound(collectionID, "tombstone")
	}

	collMeta := &pb.CollectionInfo{}
	err = proto.Unmarshal([]byte(collVal), collMeta)
	return collMeta, err
}

func (kc *Catalog) loadCollectionFromDefaultDb(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
	if info, err := kc.loadCollectionFromDb(ctx, util.DefaultDBID, collectionID, ts); err == nil {
		return info, nil
	}
	// get collection from older version.
	return kc.loadCollectionFromDb(ctx, util.NonDBID, collectionID, ts)
}

func (kc *Catalog) loadCollection(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*pb.CollectionInfo, error) {
	if isDefaultDB(dbID) {
		info, err := kc.loadCollectionFromDefaultDb(ctx, collectionID, ts)
		if err != nil {
			return nil, err
		}
		kc.fixDefaultDBIDConsistency(ctx, info, ts)
		return info, nil
	}
	return kc.loadCollectionFromDb(ctx, dbID, collectionID, ts)
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

func (kc *Catalog) CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts typeutil.Timestamp) error {
	collMeta, err := kc.loadCollection(ctx, dbID, partition.CollectionID, ts)
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
		return kc.Txn.Save(ctx, k, string(v))
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

	// this partition exists in older version, should be also changed in place.
	k := BuildCollectionKey(util.NonDBID, partition.CollectionID)
	v, err := proto.Marshal(collMeta)
	if err != nil {
		return err
	}
	return kc.Txn.Save(ctx, k, string(v))
}

func (kc *Catalog) CreateAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	oldKBefore210 := BuildAliasKey210(alias.Name)
	oldKeyWithoutDb := BuildAliasKey(alias.Name)
	k := BuildAliasKeyWithDB(alias.DbID, alias.Name)
	aliasInfo := model.MarshalAliasModel(alias)
	v, err := proto.Marshal(aliasInfo)
	if err != nil {
		return err
	}
	kvs := map[string]string{k: string(v)}
	return kc.Txn.MultiSaveAndRemove(ctx, kvs, []string{oldKBefore210, oldKeyWithoutDb})
}

func (kc *Catalog) AlterCredential(ctx context.Context, credential *model.Credential) error {
	k := fmt.Sprintf("%s/%s", CredentialPrefix, credential.Username)
	credentialInfo := model.MarshalCredentialModel(credential)
	credentialInfo.Username = "" // Username is already save in the key, remove it from the value.
	v, err := json.Marshal(credentialInfo)
	if err != nil {
		log.Ctx(ctx).Error("create credential marshal fail", zap.String("key", k), zap.Error(err))
		return err
	}

	err = kc.Txn.Save(ctx, k, string(v))
	if err != nil {
		log.Ctx(ctx).Error("create credential persist meta fail", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) listPartitionsAfter210(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*model.Partition, error) {
	prefix := BuildPartitionPrefix(collectionID)
	_, values, err := kc.Txn.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	partitions := make([]*model.Partition, 0, len(values))
	for _, v := range values {
		if IsTombstone(v) {
			continue
		}
		partitionMeta := &pb.PartitionInfo{}
		err := proto.Unmarshal([]byte(v), partitionMeta)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, model.UnmarshalPartitionModel(partitionMeta))
	}
	return partitions, nil
}

func (kc *Catalog) batchListPartitionsAfter210(ctx context.Context, ts typeutil.Timestamp) (map[int64][]*model.Partition, error) {
	_, values, err := kc.Txn.LoadWithPrefix(ctx, PartitionMetaPrefix+"/")
	if err != nil {
		return nil, err
	}

	ret := make(map[int64][]*model.Partition)
	for i := 0; i < len(values); i++ {
		if IsTombstone(values[i]) {
			continue
		}
		partitionMeta := &pb.PartitionInfo{}
		err := proto.Unmarshal([]byte(values[i]), partitionMeta)
		if err != nil {
			return nil, err
		}
		collectionID := partitionMeta.GetCollectionId()
		if ret[collectionID] == nil {
			ret[collectionID] = make([]*model.Partition, 0)
		}
		ret[collectionID] = append(ret[collectionID], model.UnmarshalPartitionModel(partitionMeta))
	}
	return ret, nil
}

func fieldVersionAfter210(collMeta *pb.CollectionInfo) bool {
	return len(collMeta.GetSchema().GetFields()) <= 0 && len(collMeta.GetSchema().GetStructArrayFields()) <= 0
}

func (kc *Catalog) listFieldsAfter210(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*model.Field, error) {
	prefix := BuildFieldPrefix(collectionID)
	_, values, err := kc.Txn.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	fields := make([]*model.Field, 0, len(values))
	for _, v := range values {
		if IsTombstone(v) {
			continue
		}
		partitionMeta := &schemapb.FieldSchema{}
		err := proto.Unmarshal([]byte(v), partitionMeta)
		if err != nil {
			return nil, err
		}
		fields = append(fields, model.UnmarshalFieldModel(partitionMeta))
	}
	return fields, nil
}

func (kc *Catalog) batchListFieldsAfter210(ctx context.Context, ts typeutil.Timestamp) (map[int64][]*model.Field, error) {
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, FieldMetaPrefix+"/")
	if err != nil {
		return nil, err
	}

	ret := make(map[int64][]*model.Field)
	for i := 0; i < len(values); i++ {
		if IsTombstone(values[i]) {
			continue
		}
		fieldMeta := &schemapb.FieldSchema{}
		err := proto.Unmarshal([]byte(values[i]), fieldMeta)
		if err != nil {
			return nil, err
		}

		collectionID, err := strconv.ParseInt(strings.Split(keys[i], "/")[2], 10, 64)
		if err != nil {
			return nil, err
		}
		if ret[collectionID] == nil {
			ret[collectionID] = make([]*model.Field, 0)
		}
		ret[collectionID] = append(ret[collectionID], model.UnmarshalFieldModel(fieldMeta))
	}
	return ret, nil
}

func (kc *Catalog) listStructArrayFieldsAfter210(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*model.StructArrayField, error) {
	prefix := BuildStructArrayFieldPrefix(collectionID)
	_, values, err := kc.Txn.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	structFields := make([]*model.StructArrayField, 0, len(values))
	for _, v := range values {
		if IsTombstone(v) {
			continue
		}
		partitionMeta := &schemapb.StructArrayFieldSchema{}
		err := proto.Unmarshal([]byte(v), partitionMeta)
		if err != nil {
			return nil, err
		}
		structFields = append(structFields, model.UnmarshalStructArrayFieldModel(partitionMeta))
	}
	return structFields, nil
}

func (kc *Catalog) listFunctions(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*model.Function, error) {
	prefix := BuildFunctionPrefix(collectionID)
	_, values, err := kc.Txn.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	functions := make([]*model.Function, 0, len(values))
	for _, v := range values {
		if IsTombstone(v) {
			continue
		}
		functionSchema := &schemapb.FunctionSchema{}
		err := proto.Unmarshal([]byte(v), functionSchema)
		if err != nil {
			return nil, err
		}
		functions = append(functions, model.UnmarshalFunctionModel(functionSchema))
	}
	return functions, nil
}

func (kc *Catalog) batchListFunctions(ctx context.Context, ts typeutil.Timestamp) (map[int64][]*model.Function, error) {
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, FunctionMetaPrefix+"/")
	if err != nil {
		return nil, err
	}
	ret := make(map[int64][]*model.Function)
	for i := 0; i < len(values); i++ {
		if IsTombstone(values[i]) {
			continue
		}
		functionSchema := &schemapb.FunctionSchema{}
		err := proto.Unmarshal([]byte(values[i]), functionSchema)
		if err != nil {
			return nil, err
		}
		collectionID, err := strconv.ParseInt(strings.Split(keys[i], "/")[2], 10, 64)
		if err != nil {
			return nil, err
		}
		if ret[collectionID] == nil {
			ret[collectionID] = make([]*model.Function, 0)
		}
		ret[collectionID] = append(ret[collectionID], model.UnmarshalFunctionModel(functionSchema))
	}
	return ret, nil
}

func (kc *Catalog) appendPartitionAndFieldsInfo(ctx context.Context, collMeta *pb.CollectionInfo,
	ts typeutil.Timestamp,
) (*model.Collection, error) {
	collection := model.UnmarshalCollectionModel(collMeta)

	if !partitionVersionAfter210(collMeta) && !fieldVersionAfter210(collMeta) {
		return collection, nil
	}

	var (
		partitions        []*model.Partition
		fields            []*model.Field
		structArrayFields []*model.StructArrayField
		functions         []*model.Function
	)

	g, gCtx := errgroup.WithContext(ctx)
	collectionID := collection.CollectionID

	g.Go(func() error {
		var err error
		partitions, err = kc.listPartitionsAfter210(gCtx, collectionID, ts)
		return err
	})

	g.Go(func() error {
		var err error
		fields, err = kc.listFieldsAfter210(gCtx, collectionID, ts)
		return err
	})

	g.Go(func() error {
		var err error
		structArrayFields, err = kc.listStructArrayFieldsAfter210(gCtx, collectionID, ts)
		return err
	})

	g.Go(func() error {
		var err error
		functions, err = kc.listFunctions(gCtx, collectionID, ts)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	collection.Partitions = partitions
	collection.Fields = fields
	collection.StructArrayFields = structArrayFields
	collection.Functions = functions
	return collection, nil
}

func (kc *Catalog) batchAppendPartitionAndFieldsInfo(ctx context.Context, collMeta []*pb.CollectionInfo,
	ts typeutil.Timestamp,
) ([]*model.Collection, error) {
	var partitionMetaMap map[int64][]*model.Partition
	var fieldMetaMap map[int64][]*model.Field
	var functionMetaMap map[int64][]*model.Function
	ret := make([]*model.Collection, 0)
	for _, coll := range collMeta {
		collection := model.UnmarshalCollectionModel(coll)
		if partitionVersionAfter210(coll) || fieldVersionAfter210(coll) {
			if len(partitionMetaMap) == 0 {
				var err error
				partitionMetaMap, err = kc.batchListPartitionsAfter210(ctx, ts)
				if err != nil {
					return nil, err
				}

				fieldMetaMap, err = kc.batchListFieldsAfter210(ctx, ts)
				if err != nil {
					return nil, err
				}

				functionMetaMap, err = kc.batchListFunctions(ctx, ts)
				if err != nil {
					return nil, err
				}
			}

			if partitionMetaMap[collection.CollectionID] != nil {
				collection.Partitions = partitionMetaMap[collection.CollectionID]
			}
			if fieldMetaMap[collection.CollectionID] != nil {
				collection.Fields = fieldMetaMap[collection.CollectionID]
			}
			if functionMetaMap[collection.CollectionID] != nil {
				collection.Functions = functionMetaMap[collection.CollectionID]
			}
		}
		ret = append(ret, collection)
	}

	return ret, nil
}

func (kc *Catalog) GetCollectionByID(ctx context.Context, dbID int64, ts typeutil.Timestamp, collectionID typeutil.UniqueID) (*model.Collection, error) {
	collMeta, err := kc.loadCollection(ctx, dbID, collectionID, ts)
	if err != nil {
		return nil, err
	}

	return kc.appendPartitionAndFieldsInfo(ctx, collMeta, ts)
}

func (kc *Catalog) CollectionExists(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	_, err := kc.GetCollectionByID(ctx, dbID, ts, collectionID)
	return err == nil
}

func (kc *Catalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	k := fmt.Sprintf("%s/%s", CredentialPrefix, username)
	v, err := kc.Txn.Load(ctx, k)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			log.Ctx(ctx).Debug("not found the user", zap.String("key", k))
		} else {
			log.Ctx(ctx).Warn("get credential meta fail", zap.String("key", k), zap.Error(err))
		}
		return nil, err
	}

	credentialInfo := internalpb.CredentialInfo{}
	err = json.Unmarshal([]byte(v), &credentialInfo)
	if err != nil {
		return nil, fmt.Errorf("unmarshal credential info err:%w", err)
	}
	// we don't save the username in the credential info, so we need to set it manually from path.
	credentialInfo.Username = username
	return model.UnmarshalCredentialModel(&credentialInfo), nil
}

func (kc *Catalog) AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	return kc.CreateAlias(ctx, alias, ts)
}

func (kc *Catalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	collectionKeys := []string{BuildCollectionKey(collectionInfo.DBID, collectionInfo.CollectionID)}

	var delMetakeysSnap []string
	for _, alias := range collectionInfo.Aliases {
		delMetakeysSnap = append(delMetakeysSnap,
			BuildAliasKey210(alias),
			BuildAliasKey(alias),
			BuildAliasKeyWithDB(collectionInfo.DBID, alias),
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
	for _, structArrayField := range collectionInfo.StructArrayFields {
		delMetakeysSnap = append(delMetakeysSnap, BuildStructArrayFieldKey(collectionInfo.CollectionID, structArrayField.FieldID))
	}
	for _, function := range collectionInfo.Functions {
		delMetakeysSnap = append(delMetakeysSnap, BuildFunctionKey(collectionInfo.CollectionID, function.ID))
	}
	// delMetakeysSnap = append(delMetakeysSnap, buildPartitionPrefix(collectionInfo.CollectionID))
	// delMetakeysSnap = append(delMetakeysSnap, buildFieldPrefix(collectionInfo.CollectionID))

	// Remove related metadata first, then the collection key itself.
	// If RootCoord crashes in between, the collection stays in Dropping state
	// and the tombstone sweeper will retry on next startup.
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if err := batchMultiSaveAndRemove(ctx, kc.Txn, maxTxnNum, nil, delMetakeysSnap); err != nil {
		return err
	}

	// if we found collection dropping, we should try removing related resources.
	return kc.Txn.MultiSaveAndRemove(ctx, nil, collectionKeys)
}

func (kc *Catalog) alterModifyCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp, fieldModify bool) error {
	if oldColl.TenantID != newColl.TenantID || oldColl.CollectionID != newColl.CollectionID {
		return errors.New("altering tenant id or collection id is forbidden")
	}
	if oldColl.DBID != newColl.DBID {
		return errors.New("altering dbID should use `AlterCollectionDB` interface")
	}
	oldCollClone := oldColl.Clone()
	oldCollClone.DBID = newColl.DBID
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
	oldCollClone.Properties = newColl.Properties
	oldCollClone.Fields = newColl.Fields
	oldCollClone.StructArrayFields = newColl.StructArrayFields
	oldCollClone.UpdateTimestamp = newColl.UpdateTimestamp
	oldCollClone.EnableDynamicField = newColl.EnableDynamicField
	oldCollClone.SchemaVersion = newColl.SchemaVersion
	oldCollClone.ShardInfos = newColl.ShardInfos
	oldCollClone.ExternalSource = newColl.ExternalSource
	oldCollClone.ExternalSpec = newColl.ExternalSpec

	newKey := BuildCollectionKey(newColl.DBID, oldColl.CollectionID)
	value, err := proto.Marshal(model.MarshalCollectionModel(oldCollClone))
	if err != nil {
		return err
	}
	saves := map[string]string{newKey: string(value)}
	// no default aliases will be created.
	// save fields info to new path.
	if fieldModify {
		for _, field := range newColl.Fields {
			k := BuildFieldKey(newColl.CollectionID, field.FieldID)
			fieldInfo := model.MarshalFieldModel(field)
			v, err := proto.Marshal(fieldInfo)
			if err != nil {
				return err
			}
			saves[k] = string(v)
		}

		for _, structArrayField := range newColl.StructArrayFields {
			k := BuildStructArrayFieldKey(newColl.CollectionID, structArrayField.FieldID)
			structArrayFieldInfo := model.MarshalStructArrayFieldModel(structArrayField)
			v, err := proto.Marshal(structArrayFieldInfo)
			if err != nil {
				return err
			}
			saves[k] = string(v)
		}
		for _, function := range newColl.Functions {
			k := BuildFunctionKey(newColl.CollectionID, function.ID)
			functionInfo := model.MarshalFunctionModel(function)
			v, err := proto.Marshal(functionInfo)
			if err != nil {
				return err
			}
			saves[k] = string(v)
		}
	}

	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	return etcd.SaveByBatchWithLimit(saves, maxTxnNum, func(partialKvs map[string]string) error {
		return kc.Txn.MultiSave(ctx, partialKvs)
	})
}

func (kc *Catalog) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp, fieldModify bool) error {
	switch alterType {
	case metastore.MODIFY:
		return kc.alterModifyCollection(ctx, oldColl, newColl, ts, fieldModify)
	default:
		return fmt.Errorf("altering collection doesn't support %s", alterType.String())
	}
}

func (kc *Catalog) AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp) error {
	if oldColl.TenantID != newColl.TenantID || oldColl.CollectionID != newColl.CollectionID {
		return errors.New("altering tenant id or collection id is forbidden")
	}
	oldKey := BuildCollectionKey(oldColl.DBID, oldColl.CollectionID)
	newKey := BuildCollectionKey(newColl.DBID, newColl.CollectionID)

	value, err := proto.Marshal(model.MarshalCollectionModel(newColl))
	if err != nil {
		return err
	}
	saves := map[string]string{newKey: string(value)}

	return kc.Txn.MultiSaveAndRemove(ctx, saves, []string{oldKey})
}

func (kc *Catalog) alterModifyPartition(ctx context.Context, oldPart *model.Partition, newPart *model.Partition, ts typeutil.Timestamp) error {
	if oldPart.CollectionID != newPart.CollectionID || oldPart.PartitionID != newPart.PartitionID {
		return errors.New("altering collection id or partition id is forbidden")
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
	return kc.Txn.Save(ctx, key, string(value))
}

func (kc *Catalog) AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType metastore.AlterType, ts typeutil.Timestamp) error {
	if alterType == metastore.MODIFY {
		return kc.alterModifyPartition(ctx, oldPart, newPart, ts)
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

func (kc *Catalog) DropPartition(ctx context.Context, dbID int64, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta, err := kc.loadCollection(ctx, dbID, collectionID, ts)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		// collection's gc happened before partition's.
		return nil
	}

	if err != nil {
		return err
	}

	if partitionVersionAfter210(collMeta) {
		k := BuildPartitionKey(collectionID, partitionID)
		return kc.Txn.MultiSaveAndRemove(ctx, nil, []string{k})
	}

	k := BuildCollectionKey(util.NonDBID, collectionID)
	dropPartition(collMeta, partitionID)
	v, err := proto.Marshal(collMeta)
	if err != nil {
		return err
	}
	return kc.Txn.Save(ctx, k, string(v))
}

func (kc *Catalog) DropCredential(ctx context.Context, username string) error {
	k := fmt.Sprintf("%s/%s", CredentialPrefix, username)
	userResults, err := kc.ListUser(ctx, util.DefaultTenant, &milvuspb.UserEntity{Name: username}, true)
	if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		log.Ctx(ctx).Warn("fail to list user", zap.String("key", k), zap.Error(err))
		return err
	}
	deleteKeys := make([]string, 0, len(userResults)+1)
	deleteKeys = append(deleteKeys, k)
	for _, userResult := range userResults {
		if userResult.User.Name == username {
			for _, role := range userResult.Roles {
				userRoleKey := fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, username, role.Name)
				deleteKeys = append(deleteKeys, userRoleKey)
			}
		}
	}
	err = kc.Txn.MultiRemove(ctx, deleteKeys)
	if err != nil {
		log.Ctx(ctx).Warn("fail to drop credential", zap.String("key", k), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) DropAlias(ctx context.Context, dbID int64, alias string, ts typeutil.Timestamp) error {
	oldKBefore210 := BuildAliasKey210(alias)
	oldKeyWithoutDb := BuildAliasKey(alias)
	k := BuildAliasKeyWithDB(dbID, alias)
	return kc.Txn.MultiSaveAndRemove(ctx, nil, []string{k, oldKeyWithoutDb, oldKBefore210})
}

func (kc *Catalog) GetCollectionByName(ctx context.Context, dbID int64, dbName string, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	prefix := getDatabasePrefix(dbID)
	_, vals, err := kc.Txn.LoadWithPrefix(ctx, prefix)
	if err != nil {
		log.Ctx(ctx).Warn("get collection meta fail", zap.String("collectionName", collectionName), zap.Error(err))
		return nil, err
	}

	for _, val := range vals {
		if IsTombstone(val) {
			continue
		}
		colMeta := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(val), &colMeta)
		if err != nil {
			log.Ctx(ctx).Warn("get collection meta unmarshal fail", zap.String("collectionName", collectionName), zap.Error(err))
			continue
		}
		if colMeta.Schema.Name == collectionName {
			// compatibility handled by kc.GetCollectionByID.
			return kc.GetCollectionByID(ctx, dbID, ts, colMeta.GetID())
		}
	}

	return nil, merr.WrapErrCollectionNotFoundWithDB(dbName, collectionName, fmt.Sprintf("timestamp = %d", ts))
}

func (kc *Catalog) ListCollections(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Collection, error) {
	prefix := getDatabasePrefix(dbID)
	_, rawVals, err := kc.Txn.LoadWithPrefix(ctx, prefix)
	if err != nil {
		log.Ctx(ctx).Error("get collections meta fail",
			zap.String("prefix", prefix),
			zap.Uint64("timestamp", ts),
			zap.Error(err))
		return nil, err
	}

	// Drop legacy SuffixSnapshot tombstone-valued entries before unmarshaling.
	// Prior to commit e0873a65d4, deletions at ts!=0 overwrote the plain key with
	// a 3-byte tombstone marker instead of removing it; those stale values can
	// still exist in etcd and would break proto.Unmarshal on restart.
	vals := make([]string, 0, len(rawVals))
	for _, val := range rawVals {
		if IsTombstone(val) {
			continue
		}
		vals = append(vals, val)
	}

	start := time.Now()
	colls := make([]*model.Collection, len(vals))
	futures := make([]*conc.Future[any], 0, len(vals))
	for i, val := range vals {
		i := i
		val := val
		futures = append(futures, kc.pool.Submit(func() (any, error) {
			collMeta := &pb.CollectionInfo{}
			err := proto.Unmarshal([]byte(val), collMeta)
			if err != nil {
				log.Ctx(ctx).Warn("unmarshal collection info failed", zap.Error(err))
				return nil, err
			}
			kc.fixDefaultDBIDConsistency(ctx, collMeta, ts)
			collection, err := kc.appendPartitionAndFieldsInfo(ctx, collMeta, ts)
			if err != nil {
				return nil, err
			}
			colls[i] = collection
			return nil, nil
		}))
	}
	err = conc.AwaitAll(futures...)
	if err != nil {
		return nil, err
	}
	log.Ctx(ctx).Info("unmarshal all collection details cost", zap.Int64("db", dbID), zap.Duration("cost", time.Since(start)))
	return colls, nil
}

// fixDefaultDBIDConsistency fix dbID consistency for collectionInfo.
// We have two versions of default databaseID (0 at legacy path, 1 at new path), we should keep consistent view when user use default database.
// all collections in default database should be marked with dbID 1.
// this method also update dbid in meta store when dbid is 0
// see also: https://github.com/milvus-io/milvus/issues/33608
func (kc *Catalog) fixDefaultDBIDConsistency(ctx context.Context, collMeta *pb.CollectionInfo, ts typeutil.Timestamp) {
	if collMeta.DbId == util.NonDBID {
		coll := model.UnmarshalCollectionModel(collMeta)
		cloned := coll.Clone()
		cloned.DBID = util.DefaultDBID
		kc.AlterCollectionDB(ctx, coll, cloned, ts)

		collMeta.DbId = util.DefaultDBID
	}
}

func (kc *Catalog) listAliasesBefore210(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error) {
	_, values, err := kc.Txn.LoadWithPrefix(ctx, CollectionAliasMetaPrefix210+"/")
	if err != nil {
		return nil, err
	}
	// aliases before 210 stored by CollectionInfo.
	aliases := make([]*model.Alias, 0, len(values))
	for _, value := range values {
		if IsTombstone(value) {
			continue
		}
		coll := &pb.CollectionInfo{}
		err := proto.Unmarshal([]byte(value), coll)
		if err != nil {
			return nil, err
		}
		aliases = append(aliases, &model.Alias{
			Name:         coll.GetSchema().GetName(),
			CollectionID: coll.GetID(),
			CreatedTime:  0, // not accurate.
			DbID:         coll.DbId,
		})
	}
	return aliases, nil
}

func (kc *Catalog) listAliasesAfter210WithDb(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error) {
	prefix := BuildAliasPrefixWithDB(dbID)
	_, values, err := kc.Txn.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	// aliases after 210 stored by AliasInfo.
	aliases := make([]*model.Alias, 0, len(values))
	for _, value := range values {
		if IsTombstone(value) {
			continue
		}
		info := &pb.AliasInfo{}
		err := proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, err
		}
		aliases = append(aliases, &model.Alias{
			Name:         info.GetAliasName(),
			CollectionID: info.GetCollectionId(),
			CreatedTime:  info.GetCreatedTime(),
			DbID:         dbID,
		})
	}
	return aliases, nil
}

func (kc *Catalog) listAliasesInDefaultDb(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error) {
	aliases1, err := kc.listAliasesBefore210(ctx, ts)
	if err != nil {
		return nil, err
	}
	aliases2, err := kc.listAliasesAfter210WithDb(ctx, util.DefaultDBID, ts)
	if err != nil {
		return nil, err
	}
	aliases3, err := kc.listAliasesAfter210WithDb(ctx, util.NonDBID, ts)
	if err != nil {
		return nil, err
	}
	aliases := append(aliases1, aliases2...)
	aliases = append(aliases, aliases3...)
	return aliases, nil
}

func (kc *Catalog) ListAliases(ctx context.Context, dbID int64, ts typeutil.Timestamp) ([]*model.Alias, error) {
	if !isDefaultDB(dbID) {
		return kc.listAliasesAfter210WithDb(ctx, dbID, ts)
	}
	return kc.listAliasesInDefaultDb(ctx, ts)
}

func (kc *Catalog) ListCredentials(ctx context.Context) ([]string, error) {
	users, err := kc.ListCredentialsWithPasswd(ctx)
	if err != nil {
		return nil, err
	}
	return lo.Keys(users), nil
}

func (kc *Catalog) ListCredentialsWithPasswd(ctx context.Context) (map[string]string, error) {
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, CredentialPrefix+"/")
	if err != nil {
		log.Ctx(ctx).Error("list all credential usernames fail", zap.String("prefix", CredentialPrefix), zap.Error(err))
		return nil, err
	}

	users := make(map[string]string)
	for i := range keys {
		username := typeutil.After(keys[i], UserSubPrefix+"/")
		if len(username) == 0 {
			log.Ctx(ctx).Warn("no username extract from path:", zap.String("path", keys[i]))
			continue
		}
		credential := &internalpb.CredentialInfo{}
		err := json.Unmarshal([]byte(values[i]), credential)
		if err != nil {
			log.Ctx(ctx).Error("credential unmarshal fail", zap.String("key", keys[i]), zap.Error(err))
			return nil, err
		}
		users[username] = credential.EncryptedPassword
	}

	return users, nil
}

func (kc *Catalog) remove(ctx context.Context, k string) error {
	var err error
	if _, err = kc.Txn.Load(ctx, k); err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		return err
	}
	if err != nil && errors.Is(err, merr.ErrIoKeyNotFound) {
		log.Ctx(ctx).Debug("the key isn't existed", zap.String("key", k))
		return common.NewIgnorableError(fmt.Errorf("the key[%s] isn't existed", k))
	}
	return kc.Txn.Remove(ctx, k)
}

func (kc *Catalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	k := RolePrefix + "/" + entity.Name
	return kc.Txn.Save(ctx, k, "")
}

func (kc *Catalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	k := RolePrefix + "/" + roleName
	roleResults, err := kc.ListRole(ctx, tenant, &milvuspb.RoleEntity{Name: roleName}, true)
	if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		log.Ctx(ctx).Warn("fail to list role", zap.String("key", k), zap.Error(err))
		return err
	}

	deleteKeys := make([]string, 0, len(roleResults)+1)
	deleteKeys = append(deleteKeys, k)
	for _, roleResult := range roleResults {
		if roleResult.Role.Name == roleName {
			for _, userInfo := range roleResult.Users {
				userRoleKey := fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, userInfo.Name, roleName)
				deleteKeys = append(deleteKeys, userRoleKey)
			}
		}
	}

	err = kc.Txn.MultiRemove(ctx, deleteKeys)
	if err != nil {
		log.Ctx(ctx).Warn("fail to drop role", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) AlterUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	k := fmt.Sprintf("%s/%s/%s", RoleMappingPrefix, userEntity.Name, roleEntity.Name)
	switch operateType {
	case milvuspb.OperateUserRoleType_AddUserToRole:
		return kc.Txn.Save(ctx, k, "")
	case milvuspb.OperateUserRoleType_RemoveUserFromRole:
		return kc.Txn.Remove(ctx, k)
	}
	return fmt.Errorf("invalid operate user role type, operate type: %d", operateType)
}

func (kc *Catalog) ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	var results []*milvuspb.RoleResult

	roleToUsers := make(map[string][]string)
	if includeUserInfo {
		roleMappingKey := funcutil.HandleTenantForEtcdPrefix(RoleMappingPrefix, tenant)
		keys, _, err := kc.Txn.LoadWithPrefix(ctx, roleMappingKey)
		if err != nil {
			log.Ctx(ctx).Error("fail to load role mappings", zap.String("key", roleMappingKey), zap.Error(err))
			return results, err
		}

		for _, key := range keys {
			roleMappingInfos := typeutil.AfterN(key, roleMappingKey, "/")
			if len(roleMappingInfos) != 2 {
				log.Ctx(ctx).Warn("invalid role mapping key", zap.String("string", key), zap.String("sub_string", roleMappingKey))
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
		roleKey := funcutil.HandleTenantForEtcdPrefix(RolePrefix, tenant)
		keys, _, err := kc.Txn.LoadWithPrefix(ctx, roleKey)
		if err != nil {
			log.Ctx(ctx).Error("fail to load roles", zap.String("key", roleKey), zap.Error(err))
			return results, err
		}
		for _, key := range keys {
			infoArr := typeutil.AfterN(key, roleKey, "/")
			if len(infoArr) != 1 || len(infoArr[0]) == 0 {
				log.Ctx(ctx).Warn("invalid role key", zap.String("string", key), zap.String("sub_string", roleKey))
				continue
			}
			appendRoleResult(infoArr[0])
		}
	} else {
		if funcutil.IsEmptyString(entity.Name) {
			return results, errors.New("role name in the role entity is empty")
		}
		roleKey := RolePrefix + "/" + entity.Name
		_, err := kc.Txn.Load(ctx, roleKey)
		if err != nil {
			log.Ctx(ctx).Warn("fail to load a role", zap.String("key", roleKey), zap.Error(err))
			return results, err
		}
		appendRoleResult(entity.Name)
	}

	return results, nil
}

func (kc *Catalog) getRolesByUsername(ctx context.Context, tenant string, username string) ([]string, error) {
	var roles []string
	k := funcutil.HandleTenantForEtcdPrefix(RoleMappingPrefix, tenant, username)
	keys, _, err := kc.Txn.LoadWithPrefix(ctx, k)
	if err != nil {
		log.Ctx(ctx).Error("fail to load role mappings by the username", zap.String("key", k), zap.Error(err))
		return roles, err
	}
	for _, key := range keys {
		roleMappingInfos := typeutil.AfterN(key, k, "/")
		if len(roleMappingInfos) != 1 {
			log.Ctx(ctx).Warn("invalid role mapping key", zap.String("string", key), zap.String("sub_string", k))
			continue
		}
		roles = append(roles, roleMappingInfos[0])
	}
	return roles, nil
}

// getUserResult get the user result by the username. And never return the error because the error means the user isn't added to a role.
func (kc *Catalog) getUserResult(ctx context.Context, tenant string, username string, includeRoleInfo bool) (*milvuspb.UserResult, error) {
	result := &milvuspb.UserResult{User: &milvuspb.UserEntity{Name: username}}
	if !includeRoleInfo {
		return result, nil
	}
	roleNames, err := kc.getRolesByUsername(ctx, tenant, username)
	if err != nil {
		log.Ctx(ctx).Warn("fail to get roles by the username", zap.Error(err))
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
		result, err := kc.getUserResult(ctx, tenant, username, includeRoleInfo)
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
			return results, errors.New("username in the user entity is empty")
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

func (kc *Catalog) AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType, dbID int64, collectionID int64) error {
	privilegeName := entity.Grantor.Privilege.Name
	// Dual-write when we have an ID-based alternative:
	// - specific collection: dbID > 0 && collectionID > 0
	// - wildcard collection (*): dbID > 0 && collectionID == 0
	isDualWrite := entity.Object.Name == "Collection" && dbID > 0

	// buildGranteeKey constructs the first-level grantee-privileges key.
	buildGranteeKey := func(dbPart, objPart string) string {
		return fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, funcutil.CombineObjectName(dbPart, objPart))
	}

	// Collect all save/remove operations for atomic commit
	saves := make(map[string]string)
	var removes []string
	var ignorableErr error // track "already granted" / "not found" for caller

	// getOrCreateGranteeID gets the existing grantee ID or creates a new one for a given key.
	// It populates saves map for new grantee keys.
	// Returns (granteeID, error). If the grant doesn't exist for revoke, returns ignorable error.
	getOrCreateGranteeID := func(granteeKey string, allowLegacyFallback bool) (string, error) {
		if v, err := kc.Txn.Load(ctx, granteeKey); err == nil {
			return v, nil
		} else {
			// Compatible with old grants that didn't include dbName prefix
			if allowLegacyFallback && entity.DbName == util.DefaultDBName {
				if v2, err2 := kc.Txn.Load(ctx, fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, entity.ObjectName)); err2 == nil {
					return v2, nil
				}
			}
			if funcutil.IsRevoke(operateType) {
				if errors.Is(err, merr.ErrIoKeyNotFound) {
					return "", common.NewIgnorableError(fmt.Errorf("the grant[%s] isn't existed", granteeKey))
				}
				return "", err
			}
			if !errors.Is(err, merr.ErrIoKeyNotFound) {
				return "", err
			}
			// Grant: create new grantee ID
			granteeID := crypto.MD5(granteeKey)
			saves[granteeKey] = granteeID
			return granteeID, nil
		}
	}

	// collectGrantOps resolves grantee ID and collects the grant/revoke operation for a single key.
	collectGrantOps := func(granteeKey string, allowLegacyFallback bool) error {
		granteeID, err := getOrCreateGranteeID(granteeKey, allowLegacyFallback)
		if err != nil {
			return err
		}

		// Operate on grantee-id sub-key
		idKey := fmt.Sprintf("%s/%s/%s", GranteeIDPrefix, granteeID, privilegeName)
		_, err = kc.Txn.Load(ctx, idKey)
		if err != nil {
			if !errors.Is(err, merr.ErrIoKeyNotFound) {
				return err
			}
			if funcutil.IsRevoke(operateType) {
				return common.NewIgnorableError(fmt.Errorf("the grantee-id[%s] isn't existed", idKey))
			}
			if funcutil.IsGrant(operateType) {
				saves[idKey] = entity.Grantor.User.Name
				return nil
			}
			return nil
		}
		if funcutil.IsRevoke(operateType) {
			removes = append(removes, idKey)
			return nil
		}
		return common.NewIgnorableError(fmt.Errorf("the privilege[%s] has been granted", privilegeName))
	}

	// Collect operations for primary key
	var primaryKey string
	if isDualWrite {
		objPart := entity.ObjectName // wildcard "*" stays as-is
		if collectionID > 0 {
			objPart = funcutil.FormatCollectionID(collectionID)
		}
		primaryKey = buildGranteeKey(funcutil.FormatDatabaseID(dbID), objPart)
	} else {
		primaryKey = buildGranteeKey(entity.DbName, entity.ObjectName)
	}

	if err := collectGrantOps(primaryKey, !isDualWrite); err != nil {
		if !common.IsIgnorableError(err) {
			return err
		}
		ignorableErr = err
	}

	// Collect operations for secondary (name-based) key
	if isDualWrite {
		nameKey := buildGranteeKey(entity.DbName, entity.ObjectName)
		if err := collectGrantOps(nameKey, true); err != nil {
			if !common.IsIgnorableError(err) {
				return err
			}
			if ignorableErr == nil {
				ignorableErr = err
			}
		}
	}

	// Atomic commit: apply all saves and removes in one transaction
	if len(saves) > 0 || len(removes) > 0 {
		if err := kc.Txn.MultiSaveAndRemove(ctx, saves, removes); err != nil {
			log.Ctx(ctx).Error("fail to commit grant operations atomically", zap.Error(err))
			return err
		}
		return nil
	}

	return ignorableErr
}

func (kc *Catalog) ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var entities []*milvuspb.GrantEntity

	var granteeKey string
	appendGrantEntity := func(v string, object string, objectName string) error {
		dbName := ""
		dbName, objectName = funcutil.SplitObjectName(objectName)
		// Backward compat: old-format keys have no db prefix, treat as DefaultDBName
		if dbName == "" {
			dbName = util.DefaultDBName
		}
		if dbName != entity.DbName && dbName != util.AnyWord && entity.DbName != util.AnyWord {
			return nil
		}
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, v)
		keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeIDKey)
		if err != nil {
			log.Ctx(ctx).Error("fail to load the grantee ids", zap.String("key", granteeIDKey), zap.Error(err))
			return err
		}
		for i, key := range keys {
			granteeIDInfos := typeutil.AfterN(key, granteeIDKey, "/")
			if len(granteeIDInfos) != 1 {
				log.Ctx(ctx).Warn("invalid grantee id", zap.String("string", key), zap.String("sub_string", granteeIDKey))
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
				DbName:     dbName,
				Grantor: &milvuspb.GrantorEntity{
					User:      &milvuspb.UserEntity{Name: values[i]},
					Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
				},
			})
		}
		return nil
	}

	if !funcutil.IsEmptyString(entity.ObjectName) && entity.Object != nil && !funcutil.IsEmptyString(entity.Object.Name) {
		if entity.DbName == util.DefaultDBName {
			granteeKey = fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, entity.ObjectName)
			v, err := kc.Txn.Load(ctx, granteeKey)
			if err == nil {
				err = appendGrantEntity(v, entity.Object.Name, entity.ObjectName)
				if err == nil {
					return entities, nil
				}
			}
		}

		if entity.DbName != util.AnyWord {
			granteeKey = fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, funcutil.CombineObjectName(util.AnyWord, entity.ObjectName))
			v, err := kc.Txn.Load(ctx, granteeKey)
			if err == nil {
				_ = appendGrantEntity(v, entity.Object.Name, funcutil.CombineObjectName(util.AnyWord, entity.ObjectName))
			}
		}

		granteeKey = fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, funcutil.CombineObjectName(entity.DbName, entity.ObjectName))
		v, err := kc.Txn.Load(ctx, granteeKey)
		if err != nil {
			log.Ctx(ctx).Error("fail to load the grant privilege entity", zap.String("key", granteeKey), zap.Error(err))
			return entities, err
		}
		err = appendGrantEntity(v, entity.Object.Name, funcutil.CombineObjectName(entity.DbName, entity.ObjectName))
		if err != nil {
			return entities, err
		}
	} else {
		granteeKey = funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, entity.Role.Name)
		keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
		if err != nil {
			log.Ctx(ctx).Error("fail to load grant privilege entities", zap.String("key", granteeKey), zap.Error(err))
			return entities, err
		}
		for i, key := range keys {
			grantInfos := typeutil.AfterN(key, granteeKey, "/")
			if len(grantInfos) != 2 {
				log.Ctx(ctx).Warn("invalid grantee key", zap.String("string", key), zap.String("sub_string", granteeKey))
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

func (kc *Catalog) DeleteGrantByCollectionID(ctx context.Context, tenant string, collectionID int64, dbName string, collectionName string) error {
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		log.Ctx(ctx).Warn("fail to load grant privilege entities for collection cleanup",
			zap.String("key", granteeKey), zap.Error(err))
		return err
	}

	idObjectName := funcutil.FormatCollectionID(collectionID)
	var exactRemoveKeys []string
	var prefixRemoveKeys []string
	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 {
			continue
		}
		if grantInfos[1] != "Collection" {
			continue
		}
		grantDB, grantObj := funcutil.SplitObjectName(grantInfos[2])

		// Match both dual-write formats:
		// 1. ID-based: dbID:{dbID}.colID:{collID}  — match by colID part
		// 2. Name-based: {dbName}.{collName}        — match by exact db+collection name
		isIDMatch := grantObj == idObjectName
		isNameMatch := collectionName != "" && grantObj == collectionName && grantDB == dbName
		if !isIDMatch && !isNameMatch {
			continue
		}

		logicalKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2])
		exactRemoveKeys = append(exactRemoveKeys, logicalKey)
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, values[i])
		prefixRemoveKeys = append(prefixRemoveKeys, granteeIDKey)
	}

	if len(exactRemoveKeys) == 0 && len(prefixRemoveKeys) == 0 {
		return nil
	}

	// Delete grantee pointer keys first so ListPolicy stops seeing the grant immediately.
	// Then delete grantee-id sub-keys. On partial failure, orphaned sub-keys are harmless
	// (no pointer references them), whereas orphaned pointers would cause empty grant entries.
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if len(exactRemoveKeys) > 0 {
		if err = etcd.RemoveByBatchWithLimit(exactRemoveKeys, maxTxnNum, func(partialKeys []string) error {
			return kc.Txn.MultiSaveAndRemove(ctx, nil, partialKeys)
		}); err != nil {
			log.Ctx(ctx).Warn("fail to remove grantee entries for collection",
				zap.Int64("collectionID", collectionID), zap.Error(err))
			return err
		}
	}

	if len(prefixRemoveKeys) > 0 {
		if err = etcd.RemoveByBatchWithLimit(prefixRemoveKeys, maxTxnNum, func(partialKeys []string) error {
			return kc.Txn.MultiSaveAndRemoveWithPrefix(ctx, nil, partialKeys)
		}); err != nil {
			log.Ctx(ctx).Warn("fail to remove granteeID entries for collection",
				zap.Int64("collectionID", collectionID), zap.Error(err))
			return err
		}
	}

	return nil
}

// DeleteGrantByDatabaseID deletes all Collection grants keyed under the dropped
// database — both name-based ({dbName}.col / {dbName}.*) and ID-based
// (dbID:X.colID:Y / dbID:X.*) — together with their grantee-id sub-keys.
//
// All-database wildcard grants (*.*), grants for other databases, and
// non-Collection grants are left untouched. Intended for DropDatabase to keep
// the RBAC catalog in sync, symmetric with DeleteGrantByCollectionID.
func (kc *Catalog) DeleteGrantByDatabaseID(ctx context.Context, tenant string, dbID int64, dbName string) error {
	if dbName == "" || dbName == util.AnyWord {
		return nil
	}
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		log.Ctx(ctx).Warn("fail to load grant privilege entities for database cleanup",
			zap.String("key", granteeKey), zap.Error(err))
		return err
	}

	idDBStr := funcutil.FormatDatabaseID(dbID)
	var exactRemoveKeys []string
	var prefixRemoveKeys []string
	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 || grantInfos[1] != "Collection" {
			continue
		}
		grantDB, _ := funcutil.SplitObjectName(grantInfos[2])

		// Match both dual-write formats for the dropped database:
		// 1. Name-based: {dbName}.*  (covers both {dbName}.col and {dbName}.*)
		// 2. ID-based:   {dbID:X}.*  (covers both dbID:X.colID:Y and dbID:X.*)
		// We deliberately do NOT match "*" (all-databases wildcard) — those grants
		// survive individual database drops.
		if grantDB != dbName && grantDB != idDBStr {
			continue
		}

		logicalKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2])
		exactRemoveKeys = append(exactRemoveKeys, logicalKey)
		prefixRemoveKeys = append(prefixRemoveKeys, funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, values[i]))
	}

	if len(exactRemoveKeys) == 0 {
		return nil
	}

	// Pointer keys first, sub-keys second — same ordering as DeleteGrantByCollectionID.
	// On partial failure, orphaned sub-keys are harmless (no pointer references
	// them), whereas orphaned pointers would cause empty grant entries.
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if err = etcd.RemoveByBatchWithLimit(exactRemoveKeys, maxTxnNum, func(partialKeys []string) error {
		return kc.Txn.MultiSaveAndRemove(ctx, nil, partialKeys)
	}); err != nil {
		log.Ctx(ctx).Warn("fail to remove grantee entries for database",
			zap.Int64("dbID", dbID), zap.String("dbName", dbName), zap.Error(err))
		return err
	}
	if err = etcd.RemoveByBatchWithLimit(prefixRemoveKeys, maxTxnNum, func(partialKeys []string) error {
		return kc.Txn.MultiSaveAndRemoveWithPrefix(ctx, nil, partialKeys)
	}); err != nil {
		log.Ctx(ctx).Warn("fail to remove granteeID entries for database",
			zap.Int64("dbID", dbID), zap.String("dbName", dbName), zap.Error(err))
		return err
	}

	log.Ctx(ctx).Info("removed grant keys for dropped database",
		zap.Int64("dbID", dbID), zap.String("dbName", dbName),
		zap.Int("count", len(exactRemoveKeys)))
	return nil
}

// DeleteStaleNameBasedGrant removes name-based grant pointer keys that exactly match
// {dbName}.{collectionName}. ID-based keys (dbID:X.colID:Y / dbID:X.*) and name-based
// keys for other collections are left untouched. Intended for RenameCollection to
// clean up stale pointers from the pre-rename name; drop+recreate with the same old
// name would otherwise hit the stale pointer via the proxy name-based fallback path.
func (kc *Catalog) DeleteStaleNameBasedGrant(ctx context.Context, tenant string, dbName string, collectionName string) error {
	if collectionName == "" || collectionName == util.AnyWord {
		return nil
	}
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		log.Ctx(ctx).Warn("fail to load grant privilege entities for stale cleanup",
			zap.String("key", granteeKey), zap.Error(err))
		return err
	}

	var exactRemoveKeys []string
	var prefixRemoveKeys []string
	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 || grantInfos[1] != "Collection" {
			continue
		}
		grantDB, grantObj := funcutil.SplitObjectName(grantInfos[2])
		// Skip ID-based keys — dbID:X.* or dbID:X.colID:Y
		if funcutil.IsIDBasedDBName(grantDB) || funcutil.IsIDBasedObjectName(grantObj) {
			continue
		}
		// Exact match required: same db and same collection name
		if grantDB != dbName || grantObj != collectionName {
			continue
		}
		logicalKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2])
		exactRemoveKeys = append(exactRemoveKeys, logicalKey)
		prefixRemoveKeys = append(prefixRemoveKeys, funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, values[i]))
	}

	if len(exactRemoveKeys) == 0 {
		return nil
	}

	// Same ordering as DeleteGrantByCollectionID: pointer keys first, sub-keys second.
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if err = etcd.RemoveByBatchWithLimit(exactRemoveKeys, maxTxnNum, func(partialKeys []string) error {
		return kc.Txn.MultiSaveAndRemove(ctx, nil, partialKeys)
	}); err != nil {
		log.Ctx(ctx).Warn("fail to remove stale name-based grantee keys",
			zap.String("dbName", dbName), zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}
	if err = etcd.RemoveByBatchWithLimit(prefixRemoveKeys, maxTxnNum, func(partialKeys []string) error {
		return kc.Txn.MultiSaveAndRemoveWithPrefix(ctx, nil, partialKeys)
	}); err != nil {
		log.Ctx(ctx).Warn("fail to remove stale name-based granteeID sub-keys",
			zap.String("dbName", dbName), zap.String("collectionName", collectionName), zap.Error(err))
		return err
	}

	log.Ctx(ctx).Info("removed stale name-based grant keys after rename",
		zap.String("dbName", dbName), zap.String("collectionName", collectionName),
		zap.Int("count", len(exactRemoveKeys)))
	return nil
}

func (kc *Catalog) MigrateGrantsToEntityID(ctx context.Context, tenant string,
	collectionNameToID func(dbName, collName string) (int64, error),
	dbNameToID func(dbName string) (int64, error),
) error {
	// Check if migration is already done
	_, err := kc.Txn.Load(ctx, grantMigrationToIDKey(tenant))
	if err == nil {
		return kc.reconcileOrphanedIDGrants(ctx, tenant, collectionNameToID, dbNameToID)
	}
	if !errors.Is(err, merr.ErrIoKeyNotFound) {
		log.Ctx(ctx).Warn("fail to check migration flag, aborting migration", zap.Error(err))
		return err
	}

	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		log.Ctx(ctx).Warn("fail to load grant privilege entities for migration", zap.Error(err))
		return err
	}

	saves := make(map[string]string)
	var removeKeys []string
	var removeIDPrefixes []string

	// migrateGranteeSubKeys copies grantee-id sub-keys from old ID to new ID.
	migrateGranteeSubKeys := func(oldID, newID string) error {
		oldIDPrefix := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, oldID)
		idKeys, idValues, idErr := kc.Txn.LoadWithPrefix(ctx, oldIDPrefix)
		if idErr != nil {
			return idErr
		}
		for j, idKey := range idKeys {
			privilegeName := typeutil.After(idKey, oldIDPrefix)
			if privilegeName == "" {
				continue
			}
			newIDKey := fmt.Sprintf("%s/%s/%s", GranteeIDPrefix, newID, privilegeName)
			saves[newIDKey] = idValues[j]
		}
		return nil
	}

	removeStaleGrant := func(i int, grantInfos []string) {
		logicalKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2])
		removeKeys = append(removeKeys, logicalKey)
		oldID := values[i]
		removeIDPrefixes = append(removeIDPrefixes, funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, oldID))
	}

	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 {
			continue
		}
		objType := grantInfos[1]

		// Only migrate Collection grants
		if objType != "Collection" {
			continue
		}
		dbName, objectName := funcutil.SplitObjectName(grantInfos[2])

		// Skip grants that are already fully ID-based
		if funcutil.IsIDBasedObjectName(objectName) && funcutil.IsIDBasedDBName(dbName) {
			continue
		}
		// Skip if only dbName is already ID-based (wildcard with ID-based db)
		if objectName == util.AnyWord && funcutil.IsIDBasedDBName(dbName) {
			continue
		}
		// Skip non-wildcard grants that already have ID-based objectName
		if funcutil.IsIDBasedObjectName(objectName) {
			continue
		}
		// Skip all-database wildcard grants (*.*) — no dbName to migrate
		if dbName == util.AnyWord {
			continue
		}

		// Resolve dbName → dbID
		dbID, dbResolveErr := dbNameToID(dbName)
		if dbResolveErr != nil {
			if !errors.Is(dbResolveErr, merr.ErrDatabaseNotFound) {
				return dbResolveErr // transient error — abort migration
			}
			log.Ctx(ctx).Warn("database not found during grant migration, removing stale grant",
				zap.String("dbName", dbName), zap.String("collectionName", objectName), zap.Error(dbResolveErr))
			removeStaleGrant(i, grantInfos)
			continue
		}

		// Wildcard grant: only migrate the dbName part (default.* → dbID:1.*)
		if objectName == util.AnyWord {
			newGranteeKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1],
				funcutil.CombineObjectName(funcutil.FormatDatabaseID(dbID), util.AnyWord))
			newID := crypto.MD5(newGranteeKey)
			saves[newGranteeKey] = newID
			if err := migrateGranteeSubKeys(values[i], newID); err != nil {
				return err
			}
			continue
		}

		// Specific grant: migrate both dbName and objectName
		collID, resolveErr := collectionNameToID(dbName, objectName)
		if resolveErr != nil {
			if !errors.Is(resolveErr, merr.ErrCollectionNotFound) {
				return resolveErr // transient error — abort migration
			}
			log.Ctx(ctx).Warn("collection not found during grant migration, removing stale grant",
				zap.String("dbName", dbName), zap.String("collectionName", objectName), zap.Error(resolveErr))
			removeStaleGrant(i, grantInfos)
			continue
		}

		newGranteeKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1],
			funcutil.CombineObjectName(funcutil.FormatDatabaseID(dbID), funcutil.FormatCollectionID(collID)))
		newID := crypto.MD5(newGranteeKey)
		saves[newGranteeKey] = newID
		if err := migrateGranteeSubKeys(values[i], newID); err != nil {
			return err
		}
	}

	// Apply changes in crash-safe order:
	// Step 1: Save new ID-based keys + remove stale grantee pointer keys (atomic).
	// Step 2: Remove stale grantee-id sub-keys (orphaned sub-keys are harmless
	//         since no pointer references them after step 1).
	// Step 3: Mark migration complete.
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if len(saves) > 0 {
		if err = etcd.SaveByBatchWithLimit(saves, maxTxnNum, func(partialKvs map[string]string) error {
			return kc.Txn.MultiSave(ctx, partialKvs)
		}); err != nil {
			log.Ctx(ctx).Warn("fail to save migrated grants", zap.Error(err))
			return err
		}
	}
	if len(removeKeys) > 0 {
		if err = etcd.RemoveByBatchWithLimit(removeKeys, maxTxnNum, func(partialKeys []string) error {
			return kc.Txn.MultiSaveAndRemove(ctx, nil, partialKeys)
		}); err != nil {
			log.Ctx(ctx).Warn("fail to remove stale grantee keys during migration", zap.Error(err))
			return err
		}
	}
	if len(removeIDPrefixes) > 0 {
		if err = etcd.RemoveByBatchWithLimit(removeIDPrefixes, maxTxnNum, func(partialKeys []string) error {
			return kc.Txn.MultiSaveAndRemoveWithPrefix(ctx, nil, partialKeys)
		}); err != nil {
			log.Ctx(ctx).Warn("fail to remove stale grantee-id prefixes during migration", zap.Error(err))
			return err
		}
	}

	// Mark migration as complete
	if err = kc.Txn.Save(ctx, grantMigrationToIDKey(tenant), "done"); err != nil {
		log.Ctx(ctx).Warn("fail to save migration completion flag", zap.Error(err))
		return err
	}

	log.Ctx(ctx).Info("grant migration to entity ID completed successfully",
		zap.Int("new_id_keys", len(saves)), zap.Int("stale_removed", len(removeKeys)))
	return nil
}

// reconcileOrphanedIDGrants backfills missing ID-based grant keys for the
// rollback + re-upgrade scenario. While the cluster was running the old
// version, new grants were written as name-based only; on re-upgrade the
// migration flag is already set so MigrateGrantsToEntityID would otherwise
// skip the additive migration entirely and those grants would be invisible
// to the ID-based enforcement path.
//
// This function used to also delete ID-based keys that lacked a matching
// name-based counterpart as "orphans". That was incorrect: after rename,
// DeleteStaleNameBasedGrant removes the old name-based pointer while the
// ID-based key correctly lives on (grants are indexed by collectionID, not
// name), so an ID-based grant without a name-based counterpart is the
// legitimate post-rename shape. Orphan cleanup would silently delete those
// valid grants on the next restart. Residual dead ID-based keys from
// partially-completed drops are inert (they reference non-existent
// collectionIDs so no enforcement path can match them) and can accumulate
// safely; DropCollection's DeleteGrantByCollectionID is the authoritative
// cleanup path.
func (kc *Catalog) reconcileOrphanedIDGrants(
	ctx context.Context,
	tenant string,
	collectionNameToID func(dbName, collName string) (int64, error),
	dbNameToID func(dbName string) (int64, error),
) error {
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		log.Ctx(ctx).Warn("reconciliation: fail to load grants", zap.Error(err))
		return err
	}

	// First pass: collect existing ID-based keys so backfill can dedupe
	// against grants that already have their ID-based counterpart.
	actualIDSuffixes := make(map[string]struct{})
	for _, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 || grantInfos[1] != "Collection" {
			continue
		}
		dbName, _ := funcutil.SplitObjectName(grantInfos[2])
		if !funcutil.IsIDBasedDBName(dbName) {
			continue
		}
		actualIDSuffixes[fmt.Sprintf("%s/%s/%s", grantInfos[0], grantInfos[1], grantInfos[2])] = struct{}{}
	}

	// Second pass: for each name-based grant whose ID-based counterpart is
	// missing, create the ID-based pointer + copy the grantee-id sub-keys.
	saves := make(map[string]string)
	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 || grantInfos[1] != "Collection" {
			continue
		}
		dbName, objectName := funcutil.SplitObjectName(grantInfos[2])

		// Skip ID-based and *.* entries — only name-based grants drive backfill.
		if funcutil.IsIDBasedDBName(dbName) || dbName == util.AnyWord {
			continue
		}

		dbID, dbErr := dbNameToID(dbName)
		if dbErr != nil {
			continue // DB no longer exists — nothing to backfill
		}

		var expectedObj string
		if objectName == util.AnyWord {
			expectedObj = funcutil.CombineObjectName(funcutil.FormatDatabaseID(dbID), util.AnyWord)
		} else {
			collID, collErr := collectionNameToID(dbName, objectName)
			if collErr != nil {
				continue // Collection no longer exists — nothing to backfill
			}
			expectedObj = funcutil.CombineObjectName(funcutil.FormatDatabaseID(dbID), funcutil.FormatCollectionID(collID))
		}
		expectedSuffix := fmt.Sprintf("%s/%s/%s", grantInfos[0], grantInfos[1], expectedObj)
		if _, ok := actualIDSuffixes[expectedSuffix]; ok {
			continue // ID-based counterpart already exists
		}

		expectedLogical := fmt.Sprintf("%s/%s", GranteePrefix, expectedSuffix)
		newID := crypto.MD5(expectedLogical)
		saves[expectedLogical] = newID

		// Copy the name-based grantee-id sub-keys under the new ID-based grantee ID.
		oldGranteeID := values[i]
		oldIDPrefix := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, oldGranteeID)
		subKeys, subValues, subErr := kc.Txn.LoadWithPrefix(ctx, oldIDPrefix)
		if subErr != nil {
			log.Ctx(ctx).Warn("reconciliation: fail to load grantee-id sub-keys for backfill",
				zap.String("oldGranteeID", oldGranteeID), zap.Error(subErr))
			return subErr
		}
		for j, subKey := range subKeys {
			privilegeName := typeutil.After(subKey, oldIDPrefix)
			if privilegeName == "" {
				continue
			}
			saves[fmt.Sprintf("%s/%s/%s", GranteeIDPrefix, newID, privilegeName)] = subValues[j]
		}
	}

	if len(saves) == 0 {
		log.Ctx(ctx).Info("reconciliation: no ID-based grants need backfill")
		return nil
	}

	log.Ctx(ctx).Info("reconciliation: backfilling missing ID-based grant keys",
		zap.Int("count", len(saves)))
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if err = etcd.SaveByBatchWithLimit(saves, maxTxnNum, func(partialKvs map[string]string) error {
		return kc.Txn.MultiSave(ctx, partialKvs)
	}); err != nil {
		log.Ctx(ctx).Warn("reconciliation: fail to backfill ID-based grant keys", zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	var (
		k          = funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, role.Name)
		err        error
		removeKeys []string
	)

	removeKeys = append(removeKeys, k)

	// the values are the grantee id list
	_, values, err := kc.Txn.LoadWithPrefix(ctx, k)
	if err != nil {
		log.Ctx(ctx).Warn("fail to load grant privilege entities", zap.String("key", k), zap.Error(err))
		return err
	}
	for _, v := range values {
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, v)
		removeKeys = append(removeKeys, granteeIDKey)
	}

	if err = kc.Txn.MultiSaveAndRemoveWithPrefix(ctx, nil, removeKeys); err != nil {
		log.Ctx(ctx).Error("fail to remove with the prefix", zap.String("key", k), zap.Error(err))
	}
	return err
}

func (kc *Catalog) ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error) {
	var grants []*milvuspb.GrantEntity
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		log.Ctx(ctx).Error("fail to load all grant privilege entities", zap.String("key", granteeKey), zap.Error(err))
		return []*milvuspb.GrantEntity{}, err
	}

	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 {
			log.Ctx(ctx).Warn("invalid grantee key", zap.String("string", key), zap.String("sub_string", granteeKey))
			continue
		}
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, values[i])
		idKeys, _, err := kc.Txn.LoadWithPrefix(ctx, granteeIDKey)
		if err != nil {
			log.Ctx(ctx).Error("fail to load the grantee ids", zap.String("key", granteeIDKey), zap.Error(err))
			return []*milvuspb.GrantEntity{}, err
		}
		for _, idKey := range idKeys {
			granteeIDInfos := typeutil.AfterN(idKey, granteeIDKey, "/")
			if len(granteeIDInfos) != 1 {
				log.Ctx(ctx).Warn("invalid grantee id", zap.String("string", idKey), zap.String("sub_string", granteeIDKey))
				continue
			}
			dbName, objectName := funcutil.SplitObjectName(grantInfos[2])

			var privilegeName string
			if granteeIDInfos[0] == util.AnyWord {
				privilegeName = util.AnyWord
			} else {
				privilegeName = util.PrivilegeNameForAPI(granteeIDInfos[0])
			}
			grants = append(grants, &milvuspb.GrantEntity{
				Role:       &milvuspb.RoleEntity{Name: grantInfos[0]},
				Object:     &milvuspb.ObjectEntity{Name: grantInfos[1]},
				ObjectName: objectName,
				DbName:     dbName,
				Grantor: &milvuspb.GrantorEntity{
					Privilege: &milvuspb.PrivilegeEntity{Name: privilegeName},
				},
			})
		}
	}
	return grants, nil
}

func (kc *Catalog) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	var userRoles []string
	k := funcutil.HandleTenantForEtcdPrefix(RoleMappingPrefix, tenant)
	keys, _, err := kc.Txn.LoadWithPrefix(ctx, k)
	if err != nil {
		log.Ctx(ctx).Error("fail to load all user-role mappings", zap.String("key", k), zap.Error(err))
		return []string{}, err
	}

	for _, key := range keys {
		userRolesInfos := typeutil.AfterN(key, k, "/")
		if len(userRolesInfos) != 2 {
			log.Ctx(ctx).Warn("invalid user-role key", zap.String("string", key), zap.String("sub_string", k))
			continue
		}
		userRoles = append(userRoles, funcutil.EncodeUserRoleCache(userRolesInfos[0], userRolesInfos[1]))
	}
	return userRoles, nil
}

func (kc *Catalog) BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error) {
	users, err := kc.ListUser(ctx, tenant, nil, true)
	if err != nil {
		return nil, err
	}

	credentials, err := kc.ListCredentialsWithPasswd(ctx)
	if err != nil {
		return nil, err
	}

	userInfos := lo.FilterMap(users, func(entity *milvuspb.UserResult, _ int) (*milvuspb.UserInfo, bool) {
		userName := entity.GetUser().GetName()
		if userName == util.UserRoot {
			return nil, false
		}
		return &milvuspb.UserInfo{
			User:     userName,
			Password: credentials[userName],
			Roles:    entity.GetRoles(),
		}, true
	})

	roles, err := kc.ListRole(ctx, tenant, nil, false)
	if err != nil {
		return nil, err
	}

	roleEntity := lo.FilterMap(roles, func(entity *milvuspb.RoleResult, _ int) (*milvuspb.RoleEntity, bool) {
		roleName := entity.GetRole().GetName()
		if roleName == util.RoleAdmin || roleName == util.RolePublic {
			return nil, false
		}

		return entity.GetRole(), true
	})

	grantsEntity := make([]*milvuspb.GrantEntity, 0)
	for _, role := range roleEntity {
		grants, err := kc.ListGrant(ctx, tenant, &milvuspb.GrantEntity{
			Role:   role,
			DbName: util.AnyWord,
		})
		if err != nil {
			return nil, err
		}
		grantsEntity = append(grantsEntity, grants...)
	}

	privGroups, err := kc.ListPrivilegeGroups(ctx)
	if err != nil {
		return nil, err
	}

	return &milvuspb.RBACMeta{
		Users:           userInfos,
		Roles:           roleEntity,
		Grants:          grantsEntity,
		PrivilegeGroups: privGroups,
	}, nil
}

func (kc *Catalog) RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error {
	for _, role := range meta.GetRoles() {
		if err := kc.CreateRole(ctx, tenant, role); err != nil {
			return errors.Wrap(err, "failed to create role")
		}
	}

	for _, group := range meta.GetPrivilegeGroups() {
		if err := kc.SavePrivilegeGroup(ctx, group); err != nil {
			return errors.Wrap(err, "failed to save privilege group")
		}
	}

	// Note: grants are now handled by MetaTable.RestoreRBAC to resolve name → collectionID

	for _, user := range meta.GetUsers() {
		if err := kc.AlterCredential(ctx, &model.Credential{
			Username:          user.GetUser(),
			EncryptedPassword: user.GetPassword(),
		}); err != nil {
			return errors.Wrap(err, "failed to alter credential")
		}

		// restore user role mapping
		entity := &milvuspb.UserEntity{
			Name: user.GetUser(),
		}
		for _, role := range user.GetRoles() {
			if err := kc.AlterUserRole(ctx, tenant, entity, role, milvuspb.OperateUserRoleType_AddUserToRole); err != nil {
				return errors.Wrap(err, "failed to alter user role")
			}
		}
	}
	return nil
}

func (kc *Catalog) GetPrivilegeGroup(ctx context.Context, groupName string) (*milvuspb.PrivilegeGroupInfo, error) {
	k := BuildPrivilegeGroupkey(groupName)
	val, err := kc.Txn.Load(ctx, k)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, fmt.Errorf("privilege group [%s] does not exist", groupName)
		}
		log.Ctx(ctx).Error("failed to load privilege group", zap.String("group", groupName), zap.Error(err))
		return nil, err
	}
	privGroupInfo := &milvuspb.PrivilegeGroupInfo{}
	err = proto.Unmarshal([]byte(val), privGroupInfo)
	if err != nil {
		log.Ctx(ctx).Error("failed to unmarshal privilege group info", zap.Error(err))
		return nil, err
	}
	return privGroupInfo, nil
}

func (kc *Catalog) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	k := BuildPrivilegeGroupkey(groupName)
	err := kc.Txn.Remove(ctx, k)
	if err != nil {
		log.Ctx(ctx).Warn("fail to drop privilege group", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) SavePrivilegeGroup(ctx context.Context, data *milvuspb.PrivilegeGroupInfo) error {
	k := BuildPrivilegeGroupkey(data.GroupName)
	groupInfo := &milvuspb.PrivilegeGroupInfo{
		GroupName:  data.GroupName,
		Privileges: lo.Uniq(data.Privileges),
	}
	v, err := proto.Marshal(groupInfo)
	if err != nil {
		log.Ctx(ctx).Error("failed to marshal privilege group info", zap.Error(err))
		return err
	}
	if err = kc.Txn.Save(ctx, k, string(v)); err != nil {
		log.Ctx(ctx).Warn("fail to put privilege group", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	_, vals, err := kc.Txn.LoadWithPrefix(ctx, PrivilegeGroupPrefix+"/")
	if err != nil {
		log.Ctx(ctx).Error("failed to list privilege groups", zap.String("prefix", PrivilegeGroupPrefix), zap.Error(err))
		return nil, err
	}
	privGroups := make([]*milvuspb.PrivilegeGroupInfo, 0, len(vals))
	for _, val := range vals {
		privGroupInfo := &milvuspb.PrivilegeGroupInfo{}
		err = proto.Unmarshal([]byte(val), privGroupInfo)
		if err != nil {
			log.Ctx(ctx).Error("failed to unmarshal privilege group info", zap.Error(err))
			return nil, err
		}
		privGroups = append(privGroups, privGroupInfo)
	}
	return privGroups, nil
}

func (kc *Catalog) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	kvs := make(map[string]string)

	k := BuildFileResourceKey(resource.Id)
	v, err := proto.Marshal(resource)
	if err != nil {
		log.Ctx(ctx).Error("failed to marshal resource info", zap.Error(err))
		return err
	}
	kvs[k] = string(v)
	kvs[FileResourceVersionKey] = fmt.Sprint(version)

	if err = kc.Txn.MultiSave(ctx, kvs); err != nil {
		log.Ctx(ctx).Warn("fail to save resource info", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	k := BuildFileResourceKey(resourceID)
	if err := kc.Txn.MultiSaveAndRemove(ctx, map[string]string{FileResourceVersionKey: fmt.Sprint(version)}, []string{k}); err != nil {
		log.Ctx(ctx).Warn("fail to remove resource info", zap.String("key", k), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
	_, values, err := kc.Txn.LoadWithPrefix(ctx, FileResourceMetaPrefix+"/")
	if err != nil {
		return nil, 0, err
	}

	var version uint64 = 0
	exist, err := kc.Txn.Has(ctx, FileResourceVersionKey)
	if err != nil {
		return nil, 0, err
	}

	if exist {
		strVersion, err := kc.Txn.Load(ctx, FileResourceVersionKey)
		if err != nil {
			return nil, 0, err
		}
		v, err := strconv.ParseUint(strVersion, 10, 64)
		if err != nil {
			return nil, 0, err
		}
		version = v
	}

	infos := make([]*internalpb.FileResourceInfo, 0, len(values))
	for _, v := range values {
		info := &internalpb.FileResourceInfo{}
		err := proto.Unmarshal([]byte(v), info)
		if err != nil {
			return nil, 0, err
		}
		infos = append(infos, info)
	}

	return infos, version, nil
}

func BuildFileResourceKey(resourceID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d", FileResourceMetaPrefix, resourceID)
}

func (kc *Catalog) Close() {
	// do nothing
}

func isDefaultDB(dbID int64) bool {
	if dbID == util.DefaultDBID || dbID == util.NonDBID {
		return true
	}
	return false
}
