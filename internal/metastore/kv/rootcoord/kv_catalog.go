package rootcoord

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
		return merr.WrapErrServiceInternalMsg("collection state should be created, collection name: %s, collection id: %d, state: %s", coll.Name, coll.CollectionID, coll.State)
	}

	// Delegate to the composite Update, which owns the atomic-or-ordered
	// commit. When the child kvs plus the collection key fit one etcd txn,
	// everything lands atomically; otherwise Update falls back to flushing
	// the children first and the collection key (the CommitSave visibility
	// marker) last. That ordering preserves the crash-safety contract the
	// inline code used to hand-roll: if we crash before the collection key
	// lands, the collection is not loaded on restart (no collection key = not
	// in collID2Meta), so the DDL ack callback retries and completes the
	// write, overwriting any orphan child keys. metastore.CreateCollection
	// carries no precondition, so a retry after a fully-committed write is an
	// idempotent overwrite, not a failure.
	return kc.Update(ctx, ts, metastore.CreateCollection(coll))
}

// buildCollectionKV computes the collection key and marshaled collection
// value. Factored out so both Catalog.CreateCollection (which now delegates
// to the composite Update) and Catalog.Update's CollectionEntry/ActionAdd
// type-switch case (accumulated into a single txn.Builder) apply the exact
// same kv encoding.
func buildCollectionKV(coll *model.Collection) (string, string, error) {
	k := BuildCollectionKey(coll.DBID, coll.CollectionID)
	collInfo := model.MarshalCollectionModel(coll)
	v, err := proto.Marshal(collInfo)
	if err != nil {
		return "", "", merr.WrapErrSerializationFailed(err, "marshal collection info")
	}
	return k, string(v), nil
}

// buildCreateCollectionChildKvs computes the child metadata kvs (partitions,
// fields, struct array fields, functions) persisted when creating a
// collection. Factored out so both Catalog.CreateCollection and
// Catalog.Update's CollectionEntry/ActionAdd type-switch case apply the exact
// same kv encoding.
func buildCreateCollectionChildKvs(coll *model.Collection) (map[string]string, error) {
	kvs := map[string]string{}

	// save partition info to new path.
	for _, partition := range coll.Partitions {
		k := BuildPartitionKey(coll.CollectionID, partition.PartitionID)
		partitionInfo := model.MarshalPartitionModel(partition)
		v, err := proto.Marshal(partitionInfo)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		kvs[k] = string(v)
	}

	// save struct array fields to new path
	for _, structArrayField := range coll.StructArrayFields {
		k := BuildStructArrayFieldKey(coll.CollectionID, structArrayField.FieldID)
		structArrayFieldInfo := model.MarshalStructArrayFieldModel(structArrayField)
		v, err := proto.Marshal(structArrayFieldInfo)
		if err != nil {
			return nil, err
		}
		kvs[k] = string(v)
	}

	// save functions info to new path.
	for _, function := range coll.Functions {
		k := BuildFunctionKey(coll.CollectionID, function.ID)
		functionInfo := model.MarshalFunctionModel(function)
		v, err := proto.Marshal(functionInfo)
		if err != nil {
			return nil, err
		}
		kvs[k] = string(v)
	}

	return kvs, nil
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
		return merr.WrapErrServiceInternalMsg("partition already exist: %d", partition.PartitionID)
	}

	if partitionExistByName(collMeta, partition.PartitionName) {
		return merr.WrapErrServiceInternalMsg("partition already exist: %s", partition.PartitionName)
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
	credentialInfo.Username = ""       // Username is already save in the key, remove it from the value.
	credentialInfo.Sha256Password = "" // Sha256Password is cache-only, do not persist it.
	v, err := json.Marshal(credentialInfo)
	if err != nil {
		mlog.Error(ctx, "create credential marshal fail", mlog.String("key", k), mlog.Err(err))
		return err
	}

	err = kc.Txn.Save(ctx, k, string(v))
	if err != nil {
		mlog.Error(ctx, "create credential persist meta fail", mlog.String("key", k), mlog.Err(err))
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
			mlog.Debug(ctx, "not found the user", mlog.String("key", k))
		} else {
			mlog.Warn(ctx, "get credential meta fail", mlog.String("key", k), mlog.Err(err))
		}
		return nil, err
	}

	credentialInfo := internalpb.CredentialInfo{}
	err = json.Unmarshal([]byte(v), &credentialInfo)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "unmarshal credential info")
	}
	// we don't save the username in the credential info, so we need to set it manually from path.
	credentialInfo.Username = username
	return model.UnmarshalCredentialModel(&credentialInfo), nil
}

func (kc *Catalog) AlterAlias(ctx context.Context, alias *model.Alias, ts typeutil.Timestamp) error {
	return kc.CreateAlias(ctx, alias, ts)
}

func (kc *Catalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	// Delegate to the composite Update, which owns the atomic-or-ordered
	// commit. When the child metadata keys plus the collection key fit one
	// etcd txn, they are removed atomically; otherwise Update flushes the
	// child removals first and the collection key (the CommitRemove
	// visibility marker) last. If RootCoord crashes mid-flush, the collection
	// stays in Dropping state and the tombstone sweeper retries on next
	// startup.
	return kc.Update(ctx, ts, metastore.DropCollection(collectionInfo))
}

// buildDropCollectionKeys computes the collection key and the child
// metadata keys (aliases, partitions, fields, struct array fields,
// functions) removed when dropping a collection. Factored out so both
// Catalog.DropCollection (which now delegates to the composite Update) and
// Catalog.Update's CollectionEntry/ActionDelete type-switch case
// (accumulated into a single txn.Builder) apply the exact same key set.
func buildDropCollectionKeys(collectionInfo *model.Collection) (string, []string) {
	collectionKey := BuildCollectionKey(collectionInfo.DBID, collectionInfo.CollectionID)

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

	return collectionKey, delMetakeysSnap
}

func (kc *Catalog) alterModifyCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp, fieldModify bool) error {
	if oldColl.TenantID != newColl.TenantID || oldColl.CollectionID != newColl.CollectionID {
		return merr.WrapErrParameterInvalidMsg("altering tenant id or collection id is forbidden")
	}
	if oldColl.DBID != newColl.DBID {
		return merr.WrapErrParameterInvalidMsg("altering dbID should use `AlterCollectionDB` interface")
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
	removals := []string{}
	// no default aliases will be created.
	// save fields info to new path.
	if fieldModify {
		newFieldIDs := make(map[int64]struct{}, len(newColl.Fields))
		for _, field := range newColl.Fields {
			newFieldIDs[field.FieldID] = struct{}{}
			k := BuildFieldKey(newColl.CollectionID, field.FieldID)
			fieldInfo := model.MarshalFieldModel(field)
			v, err := proto.Marshal(fieldInfo)
			if err != nil {
				return err
			}
			saves[k] = string(v)
		}
		for _, field := range oldColl.Fields {
			if _, ok := newFieldIDs[field.FieldID]; !ok {
				removals = append(removals, BuildFieldKey(oldColl.CollectionID, field.FieldID))
			}
		}

		newStructArrayFieldIDs := make(map[int64]struct{}, len(newColl.StructArrayFields))
		for _, structArrayField := range newColl.StructArrayFields {
			newStructArrayFieldIDs[structArrayField.FieldID] = struct{}{}
			k := BuildStructArrayFieldKey(newColl.CollectionID, structArrayField.FieldID)
			structArrayFieldInfo := model.MarshalStructArrayFieldModel(structArrayField)
			v, err := proto.Marshal(structArrayFieldInfo)
			if err != nil {
				return err
			}
			saves[k] = string(v)
		}
		for _, structArrayField := range oldColl.StructArrayFields {
			if _, ok := newStructArrayFieldIDs[structArrayField.FieldID]; !ok {
				removals = append(removals, BuildStructArrayFieldKey(oldColl.CollectionID, structArrayField.FieldID))
			}
		}

		newFunctionIDs := make(map[int64]struct{}, len(newColl.Functions))
		for _, function := range newColl.Functions {
			newFunctionIDs[function.ID] = struct{}{}
			k := BuildFunctionKey(newColl.CollectionID, function.ID)
			functionInfo := model.MarshalFunctionModel(function)
			v, err := proto.Marshal(functionInfo)
			if err != nil {
				return err
			}
			saves[k] = string(v)
		}
		for _, function := range oldColl.Functions {
			if _, ok := newFunctionIDs[function.ID]; !ok {
				removals = append(removals, BuildFunctionKey(oldColl.CollectionID, function.ID))
			}
		}
	}

	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if len(removals) > 0 {
		if len(saves)+len(removals) <= maxTxnNum {
			return kc.Txn.MultiSaveAndRemove(ctx, saves, removals)
		}
		return batchMultiSaveAndRemove(ctx, kc.Txn, maxTxnNum, saves, removals)
	}
	return etcd.SaveByBatchWithLimit(saves, maxTxnNum, func(partialKvs map[string]string) error {
		return kc.Txn.MultiSave(ctx, partialKvs)
	})
}

func (kc *Catalog) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType metastore.AlterType, ts typeutil.Timestamp, fieldModify bool) error {
	switch alterType {
	case metastore.MODIFY:
		return kc.alterModifyCollection(ctx, oldColl, newColl, ts, fieldModify)
	default:
		return merr.WrapErrParameterInvalidMsg("altering collection doesn't support %s", alterType.String())
	}
}

func (kc *Catalog) AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts typeutil.Timestamp) error {
	if oldColl.TenantID != newColl.TenantID || oldColl.CollectionID != newColl.CollectionID {
		return merr.WrapErrParameterInvalidMsg("altering tenant id or collection id is forbidden")
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
		return merr.WrapErrParameterInvalidMsg("altering collection id or partition id is forbidden")
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
	return merr.WrapErrParameterInvalidMsg("altering partition doesn't support %s", alterType.String())
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
		mlog.Warn(ctx, "fail to list user", mlog.String("key", k), mlog.Err(err))
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
		mlog.Warn(ctx, "fail to drop credential", mlog.String("key", k), mlog.Err(err))
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
		mlog.Warn(ctx, "get collection meta fail", mlog.String("collectionName", collectionName), mlog.Err(err))
		return nil, err
	}

	for _, val := range vals {
		if IsTombstone(val) {
			continue
		}
		colMeta := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(val), &colMeta)
		if err != nil {
			mlog.Warn(ctx, "get collection meta unmarshal fail", mlog.String("collectionName", collectionName), mlog.Err(err))
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
		mlog.Error(ctx, "get collections meta fail",
			mlog.String("prefix", prefix),
			mlog.Uint64("timestamp", ts),
			mlog.Err(err))
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
				mlog.Warn(ctx, "unmarshal collection info failed", mlog.Err(err))
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
	mlog.Info(ctx, "unmarshal all collection details cost", mlog.Int64("db", dbID), mlog.Duration("cost", time.Since(start)))
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
	credentials, err := kc.listCredentials(ctx)
	if err != nil {
		return nil, err
	}
	return lo.Map(credentials, func(credential *model.Credential, _ int) string {
		return credential.Username
	}), nil
}

func (kc *Catalog) listCredentials(ctx context.Context) ([]*model.Credential, error) {
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, CredentialPrefix+"/")
	if err != nil {
		mlog.Error(ctx, "list all credentials fail", mlog.String("prefix", CredentialPrefix), mlog.Err(err))
		return nil, err
	}

	credentials := make([]*model.Credential, 0, len(keys))
	prefix := CredentialPrefix + "/"
	for i := range keys {
		prefixPos := strings.Index(keys[i], prefix)
		if prefixPos < 0 {
			mlog.Warn(ctx, "invalid credential key", mlog.String("path", keys[i]), mlog.String("prefix", prefix))
			continue
		}
		username := keys[i][prefixPos+len(prefix):]
		if len(username) == 0 || strings.Contains(username, "/") {
			mlog.Warn(ctx, "invalid credential key", mlog.String("path", keys[i]), mlog.String("prefix", prefix))
			continue
		}
		credentialInfo := &internalpb.CredentialInfo{}
		if err := json.Unmarshal([]byte(values[i]), credentialInfo); err != nil {
			mlog.Error(ctx, "credential unmarshal fail", mlog.String("key", keys[i]), mlog.Err(err))
			return nil, err
		}
		credentialInfo.Username = username
		credentials = append(credentials, model.UnmarshalCredentialModel(credentialInfo))
	}

	return credentials, nil
}

func (kc *Catalog) remove(ctx context.Context, k string) error {
	var err error
	if _, err = kc.Txn.Load(ctx, k); err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		return err
	}
	if err != nil && errors.Is(err, merr.ErrIoKeyNotFound) {
		mlog.Debug(ctx, "the key isn't existed", mlog.String("key", k))
		return common.NewIgnorableErrorf("the key[%s] isn't existed", k)
	}
	return kc.Txn.Remove(ctx, k)
}

func (kc *Catalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	k := RolePrefix + "/" + entity.Name
	value, err := model.MarshalRoleModel(&model.Role{
		Name:        entity.GetName(),
		Description: entity.GetDescription(),
	})
	if err != nil {
		return err
	}
	return kc.Txn.Save(ctx, k, value)
}

func (kc *Catalog) AlterRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	k := RolePrefix + "/" + entity.GetName()
	value, err := kc.Txn.Load(ctx, k)
	if err != nil {
		mlog.Warn(ctx, "fail to load a role", mlog.String("key", k), mlog.Err(err))
		return err
	}
	role, err := model.UnmarshalRoleModel(entity.GetName(), value)
	if err != nil {
		mlog.Warn(ctx, "undecodable role value, fallback to empty description",
			mlog.String("role", entity.GetName()),
			mlog.Err(err))
		role = &model.Role{Name: entity.GetName()}
	}
	role.Description = entity.GetDescription()
	newValue, err := model.MarshalRoleModel(role)
	if err != nil {
		return err
	}
	return kc.Txn.Save(ctx, k, newValue)
}

func (kc *Catalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	k := RolePrefix + "/" + roleName
	roleResults, err := kc.ListRole(ctx, tenant, &milvuspb.RoleEntity{Name: roleName}, true)
	if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
		mlog.Warn(ctx, "fail to list role", mlog.String("key", k), mlog.Err(err))
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
		mlog.Warn(ctx, "fail to drop role", mlog.String("key", k), mlog.Err(err))
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
	return merr.WrapErrParameterInvalidMsg("invalid operate user role type, operate type: %d", operateType)
}

func (kc *Catalog) ListRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	var results []*milvuspb.RoleResult

	roleToUsers := make(map[string][]string)
	if includeUserInfo {
		roleMappingKey := funcutil.HandleTenantForEtcdPrefix(RoleMappingPrefix, tenant)
		keys, _, err := kc.Txn.LoadWithPrefix(ctx, roleMappingKey)
		if err != nil {
			mlog.Error(ctx, "fail to load role mappings", mlog.String("key", roleMappingKey), mlog.Err(err))
			return results, err
		}

		for _, key := range keys {
			roleMappingInfos := typeutil.AfterN(key, roleMappingKey, "/")
			if len(roleMappingInfos) != 2 || funcutil.IsEmptyString(roleMappingInfos[0]) || funcutil.IsEmptyString(roleMappingInfos[1]) {
				mlog.Warn(ctx, "invalid role mapping key", mlog.String("string", key), mlog.String("sub_string", roleMappingKey))
				continue
			}
			username := roleMappingInfos[0]
			roleName := roleMappingInfos[1]
			roleToUsers[roleName] = append(roleToUsers[roleName], username)
		}
	}

	appendRoleResult := func(roleName string, value string) {
		role, err := model.UnmarshalRoleModel(roleName, value)
		if err != nil {
			mlog.Warn(ctx, "undecodable role value, fallback to empty description",
				mlog.String("role", roleName),
				mlog.Err(err))
			role = &model.Role{Name: roleName}
		}
		var users []*milvuspb.UserEntity
		for _, username := range roleToUsers[roleName] {
			users = append(users, &milvuspb.UserEntity{Name: username})
		}
		results = append(results, &milvuspb.RoleResult{
			Role:  &milvuspb.RoleEntity{Name: role.Name, Description: role.Description},
			Users: users,
		})
	}

	if entity == nil {
		roleKey := funcutil.HandleTenantForEtcdPrefix(RolePrefix, tenant)
		keys, values, err := kc.Txn.LoadWithPrefix(ctx, roleKey)
		if err != nil {
			mlog.Error(ctx, "fail to load roles", mlog.String("key", roleKey), mlog.Err(err))
			return results, err
		}
		for i, key := range keys {
			infoArr := typeutil.AfterN(key, roleKey, "/")
			if len(infoArr) != 1 || funcutil.IsEmptyString(infoArr[0]) {
				mlog.Warn(ctx, "invalid role key", mlog.String("string", key), mlog.String("sub_string", roleKey))
				continue
			}
			value := ""
			if i < len(values) {
				value = values[i]
			}
			appendRoleResult(infoArr[0], value)
		}
	} else {
		if funcutil.IsEmptyString(entity.Name) {
			return results, merr.WrapErrParameterInvalidMsg("role name in the role entity is empty")
		}
		roleKey := RolePrefix + "/" + entity.Name
		value, err := kc.Txn.Load(ctx, roleKey)
		if err != nil {
			mlog.Warn(ctx, "fail to load a role", mlog.String("key", roleKey), mlog.Err(err))
			return results, err
		}
		appendRoleResult(entity.Name, value)
	}

	return results, nil
}

func (kc *Catalog) getRolesByUsername(ctx context.Context, tenant string, username string) ([]string, error) {
	var roles []string
	k := funcutil.HandleTenantForEtcdPrefix(RoleMappingPrefix, tenant, username)
	keys, _, err := kc.Txn.LoadWithPrefix(ctx, k)
	if err != nil {
		mlog.Error(ctx, "fail to load role mappings by the username", mlog.String("key", k), mlog.Err(err))
		return roles, err
	}
	for _, key := range keys {
		roleMappingInfos := typeutil.AfterN(key, k, "/")
		if len(roleMappingInfos) != 1 || funcutil.IsEmptyString(roleMappingInfos[0]) {
			mlog.Warn(ctx, "invalid role mapping key", mlog.String("string", key), mlog.String("sub_string", k))
			continue
		}
		roles = append(roles, roleMappingInfos[0])
	}
	return roles, nil
}

// getUserResult gets the user result from a loaded credential.
func (kc *Catalog) getUserResult(ctx context.Context, tenant string, credential *model.Credential, includeRoleInfo bool) (*milvuspb.UserResult, error) {
	result := &milvuspb.UserResult{
		User:        &milvuspb.UserEntity{Name: credential.Username},
		Description: credential.Description,
	}

	if !includeRoleInfo {
		return result, nil
	}
	roleNames, err := kc.getRolesByUsername(ctx, tenant, credential.Username)
	if err != nil {
		mlog.Warn(ctx, "fail to get roles by the username", mlog.Err(err))
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
		credentials []*model.Credential
		err         error
	)

	if entity == nil {
		credentials, err = kc.listCredentials(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		if funcutil.IsEmptyString(entity.Name) {
			return nil, merr.WrapErrParameterInvalidMsg("username in the user entity is empty")
		}
		credential, err := kc.GetCredential(ctx, entity.Name)
		if err != nil {
			return nil, err
		}
		credentials = []*model.Credential{credential}
	}

	results := make([]*milvuspb.UserResult, 0, len(credentials))
	for _, credential := range credentials {
		result, err := kc.getUserResult(ctx, tenant, credential, includeRoleInfo)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func buildGranteeIDKey(idStr string, privilegeName string) string {
	return fmt.Sprintf("%s/%s/%s", GranteeIDPrefix, idStr, privilegeName)
}

func granteeIDCandidates(granteeKey string, idStr string) []string {
	candidates := make([]string, 0, 2)
	seen := make(map[string]struct{}, 2)
	appendCandidate := func(candidate string) {
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		candidates = append(candidates, candidate)
	}

	newID := crypto.GranteeID(granteeKey)
	appendCandidate(idStr)
	if idStr != newID {
		appendCandidate(newID)
	}
	return candidates
}

func newSharedGranteeIDError(idStr string, granteeKey string, otherGranteeKey string) error {
	return merr.WrapErrIoFailedReason(fmt.Sprintf("shared legacy grantee id %s is referenced by both %s and %s", idStr, granteeKey, otherGranteeKey))
}

func isLegacyGranteeID(idStr string) bool {
	return len(idStr) == 16
}

func logicalGranteeKeyFromEtcdKey(ctx context.Context, granteePrefix string, key string) (string, bool) {
	grantInfos := typeutil.AfterN(key, granteePrefix, "/")
	if len(grantInfos) != 3 {
		mlog.Warn(ctx, "invalid grantee key while checking grantee id sharing",
			mlog.String("key", key), mlog.String("prefix", granteePrefix))
		return "", false
	}
	return fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2]), true
}

func findOtherGranteeWithIDFromKeys(ctx context.Context, granteePrefix string, granteeKey string, idStr string, keys []string, values []string) (string, error) {
	for i, key := range keys {
		if i >= len(values) {
			mlog.Warn(ctx, "grantee key has no matching id value while checking grantee id sharing",
				mlog.String("key", key), mlog.String("prefix", granteePrefix))
			continue
		}
		if values[i] != idStr {
			continue
		}
		logicalKey, ok := logicalGranteeKeyFromEtcdKey(ctx, granteePrefix, key)
		if !ok {
			return "", newSharedGranteeIDError(idStr, granteeKey, key)
		}
		if logicalKey != granteeKey {
			return logicalKey, nil
		}
	}
	return "", nil
}

func shouldRemoveGranteeIDSubtree(ctx context.Context, granteePrefix string, idStr string, keys []string, values []string, removingGrantees map[string]struct{}) bool {
	if !isLegacyGranteeID(idStr) {
		return true
	}
	for i, key := range keys {
		if i >= len(values) || values[i] != idStr {
			continue
		}
		logicalKey, ok := logicalGranteeKeyFromEtcdKey(ctx, granteePrefix, key)
		if !ok {
			return false
		}
		if _, removing := removingGrantees[logicalKey]; !removing {
			return false
		}
	}
	return true
}

func (kc *Catalog) loadGranteeIDPrefix(ctx context.Context, tenant string, granteeKey string, idStr string) ([]string, []string, string, error) {
	return kc.loadGranteeIDPrefixWithLoadedGrantees(ctx, tenant, granteeKey, idStr, nil, nil)
}

func (kc *Catalog) loadGranteeIDPrefixWithLoadedGrantees(ctx context.Context, tenant string, granteeKey string, idStr string, granteeKeys []string, granteeValues []string) ([]string, []string, string, error) {
	var firstPrefix string
	newID := crypto.GranteeID(granteeKey)
	if isLegacyGranteeID(idStr) && idStr != newID {
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, idStr)
		var (
			otherGranteeKey string
			err             error
		)
		if granteeKeys != nil {
			granteePrefix := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
			otherGranteeKey, err = findOtherGranteeWithIDFromKeys(ctx, granteePrefix, granteeKey, idStr, granteeKeys, granteeValues)
		} else {
			otherGranteeKey, err = kc.findOtherGranteeWithID(ctx, tenant, granteeKey, idStr)
		}
		if err != nil {
			return nil, nil, granteeIDKey, err
		}
		if otherGranteeKey != "" {
			return nil, nil, granteeIDKey, newSharedGranteeIDError(idStr, granteeKey, otherGranteeKey)
		}
	}

	for _, candidate := range granteeIDCandidates(granteeKey, idStr) {
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, candidate)
		if firstPrefix == "" {
			firstPrefix = granteeIDKey
		}
		keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeIDKey)
		if err != nil {
			if errors.Is(err, merr.ErrIoKeyNotFound) {
				continue
			}
			return nil, nil, granteeIDKey, err
		}
		if len(keys) > 0 {
			return keys, values, granteeIDKey, nil
		}
	}
	return nil, nil, firstPrefix, nil
}

func (kc *Catalog) findOtherGranteeWithID(ctx context.Context, tenant string, granteeKey string, idStr string) (string, error) {
	granteePrefix := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteePrefix)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return "", nil
		}
		return "", err
	}
	return findOtherGranteeWithIDFromKeys(ctx, granteePrefix, granteeKey, idStr, keys, values)
}

func (kc *Catalog) migrateGranteeID(ctx context.Context, tenant string, granteeKey string, idStr string) (string, error) {
	newID := crypto.GranteeID(granteeKey)
	if idStr == newID {
		return idStr, nil
	}
	if isLegacyGranteeID(idStr) {
		otherGranteeKey, err := kc.findOtherGranteeWithID(ctx, tenant, granteeKey, idStr)
		if err != nil {
			return "", err
		}
		if otherGranteeKey != "" {
			return "", newSharedGranteeIDError(idStr, granteeKey, otherGranteeKey)
		}
	}

	saves := map[string]string{granteeKey: newID}
	var removals []string
	granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, idStr)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeIDKey)
	if err != nil {
		if !errors.Is(err, merr.ErrIoKeyNotFound) {
			return "", err
		}
	}
	for i, key := range keys {
		privilegeName := typeutil.After(key, granteeIDKey)
		if privilegeName == "" {
			mlog.Warn(ctx, "failed to extract privilege name from grantee id key",
				mlog.String("idKey", key), mlog.String("prefix", granteeIDKey))
			continue
		}
		saves[buildGranteeIDKey(newID, privilegeName)] = values[i]
		removals = append(removals, buildGranteeIDKey(idStr, privilegeName))
	}
	if err := kc.Txn.MultiSaveAndRemove(ctx, saves, removals); err != nil {
		return "", err
	}
	return newID, nil
}

func (kc *Catalog) AlterGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	var (
		privilegeName = entity.Grantor.Privilege.Name
		granteeKey    = fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, funcutil.CombineObjectName(entity.DbName, entity.ObjectName))
		idStr         string
		v             string
		err           error
	)

	// Compatible with logic without db
	if entity.DbName == util.DefaultDBName {
		legacyKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, entity.ObjectName)
		v, err = kc.Txn.Load(ctx, legacyKey)
		if err == nil {
			idStr = v
			granteeKey = legacyKey
		}
	}
	if idStr == "" {
		if v, err = kc.Txn.Load(ctx, granteeKey); err == nil {
			idStr = v
		} else {
			mlog.Warn(ctx, "fail to load grant privilege entity", mlog.String("key", granteeKey), mlog.Any("type", operateType), mlog.Err(err))
			if funcutil.IsRevoke(operateType) {
				if errors.Is(err, merr.ErrIoKeyNotFound) {
					return common.NewIgnorableErrorf("the grant[%s] isn't existed", granteeKey)
				}
				return err
			}
			if !errors.Is(err, merr.ErrIoKeyNotFound) {
				return err
			}

			idStr = crypto.GranteeID(granteeKey)
			err = kc.Txn.Save(ctx, granteeKey, idStr)
			if err != nil {
				mlog.Error(ctx, "fail to allocate id when altering the grant", mlog.Err(err))
				return err
			}
		}
	}
	if idStr != crypto.GranteeID(granteeKey) {
		idStr, err = kc.migrateGranteeID(ctx, tenant, granteeKey, idStr)
		if err != nil {
			mlog.Error(ctx, "fail to migrate grantee id when altering the grant", mlog.String("key", granteeKey), mlog.Err(err))
			return err
		}
	}
	k := buildGranteeIDKey(idStr, privilegeName)
	_, err = kc.Txn.Load(ctx, k)
	if err != nil {
		mlog.Warn(ctx, "fail to load the grantee id", mlog.String("key", k), mlog.Err(err))
		if !errors.Is(err, merr.ErrIoKeyNotFound) {
			mlog.Warn(context.TODO(), "fail to load the grantee id", mlog.String("key", k), mlog.Err(err))
			return err
		}
		mlog.Debug(ctx, "not found the grantee id", mlog.String("key", k))
		if funcutil.IsRevoke(operateType) {
			return common.NewIgnorableErrorf("the grantee-id[%s] isn't existed", k)
		}
		if funcutil.IsGrant(operateType) {
			if err = kc.Txn.Save(ctx, k, entity.Grantor.User.Name); err != nil {
				mlog.Error(ctx, "fail to save the grantee id", mlog.String("key", k), mlog.Err(err))
			}
			return err
		}
		return nil
	}
	if funcutil.IsRevoke(operateType) {
		if err = kc.Txn.Remove(ctx, k); err != nil {
			mlog.Error(ctx, "fail to remove the grantee id", mlog.String("key", k), mlog.Err(err))
			return err
		}
		return err
	}
	return common.NewIgnorableErrorf("the privilege[%s] has been granted", privilegeName)
}

func (kc *Catalog) ListGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	var entities []*milvuspb.GrantEntity

	var granteeKey string
	appendGrantEntity := func(granteeKey string, v string, object string, objectName string) error {
		dbName := ""
		dbName, objectName = funcutil.SplitObjectName(objectName)
		if dbName != entity.DbName && dbName != util.AnyWord && entity.DbName != util.AnyWord {
			return nil
		}
		keys, values, granteeIDKey, err := kc.loadGranteeIDPrefix(ctx, tenant, granteeKey, v)
		if err != nil {
			mlog.Error(ctx, "fail to load the grantee ids", mlog.String("key", granteeIDKey), mlog.Err(err))
			return err
		}
		for i, key := range keys {
			granteeIDInfos := typeutil.AfterN(key, granteeIDKey, "/")
			if len(granteeIDInfos) != 1 || funcutil.IsEmptyString(granteeIDInfos[0]) {
				mlog.Warn(ctx, "invalid grantee id", mlog.String("string", key), mlog.String("sub_string", granteeIDKey))
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
				err = appendGrantEntity(granteeKey, v, entity.Object.Name, entity.ObjectName)
				if err == nil {
					return entities, nil
				}
				return entities, err
			}
		}

		if entity.DbName != util.AnyWord {
			granteeKey = fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, funcutil.CombineObjectName(util.AnyWord, entity.ObjectName))
			v, err := kc.Txn.Load(ctx, granteeKey)
			if err == nil {
				if err = appendGrantEntity(granteeKey, v, entity.Object.Name, funcutil.CombineObjectName(util.AnyWord, entity.ObjectName)); err != nil {
					return entities, err
				}
			}
		}

		granteeKey = fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, entity.Role.Name, entity.Object.Name, funcutil.CombineObjectName(entity.DbName, entity.ObjectName))
		v, err := kc.Txn.Load(ctx, granteeKey)
		if err != nil {
			mlog.Error(ctx, "fail to load the grant privilege entity", mlog.String("key", granteeKey), mlog.Err(err))
			return entities, err
		}
		err = appendGrantEntity(granteeKey, v, entity.Object.Name, funcutil.CombineObjectName(entity.DbName, entity.ObjectName))
		if err != nil {
			return entities, err
		}
	} else {
		granteeKey = funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, entity.Role.Name)
		keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
		if err != nil {
			mlog.Error(ctx, "fail to load grant privilege entities", mlog.String("key", granteeKey), mlog.Err(err))
			return entities, err
		}
		for i, key := range keys {
			grantInfos := typeutil.AfterN(key, granteeKey, "/")
			if len(grantInfos) != 2 || funcutil.IsEmptyString(grantInfos[0]) || funcutil.IsEmptyString(grantInfos[1]) {
				mlog.Warn(ctx, "invalid grantee key", mlog.String("string", key), mlog.String("sub_string", granteeKey))
				continue
			}
			keyWithoutTrailingSlash := strings.TrimSuffix(granteeKey, "/")
			err = appendGrantEntity(fmt.Sprintf("%s/%s/%s", keyWithoutTrailingSlash, grantInfos[0], grantInfos[1]), values[i], grantInfos[0], grantInfos[1])
			if err != nil {
				return entities, err
			}
		}
	}

	return entities, nil
}

func (kc *Catalog) DeleteGrantByCollectionName(ctx context.Context, tenant string, dbName string, collectionName string) error {
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		mlog.Warn(ctx, "fail to load grant privilege entities for collection cleanup",
			mlog.String("key", granteeKey), mlog.Err(err))
		return err
	}

	var exactRemoveKeys []string
	var granteeIDs []string
	removingGrantees := make(map[string]struct{})
	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 {
			continue
		}
		// grantInfos: [role, objectType, dbName.objectName]
		if grantInfos[1] != "Collection" {
			continue
		}
		grantDB, grantObj := funcutil.SplitObjectName(grantInfos[2])
		if grantObj == collectionName && grantDB == dbName {
			// Reconstruct logical key (without etcd rootPath) for deletion.
			// LoadWithPrefix returns full etcd keys (with rootPath prefix),
			// but MultiSaveAndRemove prepends rootPath again, so we must
			// use the logical key to avoid double-prefix.
			logicalKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2])
			exactRemoveKeys = append(exactRemoveKeys, logicalKey)
			removingGrantees[logicalKey] = struct{}{}
			granteeIDs = append(granteeIDs, values[i])
		}
	}

	if len(exactRemoveKeys) == 0 && len(granteeIDs) == 0 {
		return nil
	}

	var prefixRemoveKeys []string
	for _, idStr := range granteeIDs {
		if !shouldRemoveGranteeIDSubtree(ctx, granteeKey, idStr, keys, values, removingGrantees) {
			continue
		}
		// Use prefix deletion for the granteeID key (has sub-keys)
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, idStr)
		prefixRemoveKeys = append(prefixRemoveKeys, granteeIDKey)
	}

	// Use prefix deletion for granteeID keys (which have sub-keys underneath).
	if len(prefixRemoveKeys) > 0 {
		if err = kc.Txn.MultiSaveAndRemoveWithPrefix(ctx, nil, prefixRemoveKeys); err != nil {
			mlog.Warn(ctx, "fail to remove granteeID entries for collection",
				mlog.String("dbName", dbName), mlog.String("collectionName", collectionName), mlog.Err(err))
			return err
		}
	}

	// Use exact deletion for grantee keys (leaf keys with no sub-keys).
	// Avoid MultiSaveAndRemoveWithPrefix here to prevent accidentally matching
	// keys like col1_backup when removing col1.
	if len(exactRemoveKeys) > 0 {
		if err = kc.Txn.MultiSaveAndRemove(ctx, nil, exactRemoveKeys); err != nil {
			mlog.Warn(ctx, "fail to remove grantee entries for collection",
				mlog.String("dbName", dbName), mlog.String("collectionName", collectionName), mlog.Err(err))
			return err
		}
	}

	return nil
}

func (kc *Catalog) MigrateGrantCollectionName(ctx context.Context, tenant string, oldDBName string, oldName string, newDBName string, newName string) error {
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		mlog.Warn(ctx, "fail to load grant privilege entities for collection migration",
			mlog.String("key", granteeKey), mlog.Err(err))
		return err
	}

	saves := make(map[string]string)
	var removeKeys []string
	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 {
			continue
		}
		if grantInfos[1] != "Collection" {
			continue
		}
		grantDB, grantObj := funcutil.SplitObjectName(grantInfos[2])
		if grantObj == oldName && grantDB == oldDBName {
			oldIdStr := values[i]
			// Reconstruct logical key (without etcd rootPath) for deletion.
			// LoadWithPrefix returns full etcd keys (with rootPath prefix),
			// but MultiSaveAndRemove prepends rootPath again, so we must
			// use the logical key to avoid double-prefix.
			oldKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2])
			if isLegacyGranteeID(oldIdStr) {
				otherGranteeKey, err := findOtherGranteeWithIDFromKeys(ctx, granteeKey, oldKey, oldIdStr, keys, values)
				if err != nil {
					removeKeys = append(removeKeys, oldKey)
					continue
				}
				if otherGranteeKey != "" {
					removeKeys = append(removeKeys, oldKey)
					continue
				}
			}

			// Load GranteeIDPrefix entries FIRST, before queuing the parent key
			// for migration. If this load fails, we skip both parent and child
			// to avoid half-migration (parent migrated, children lost).
			oldGranteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, oldIdStr)
			idKeys, idValues, loadErr := kc.Txn.LoadWithPrefix(ctx, oldGranteeIDKey)
			if loadErr != nil {
				mlog.Warn(ctx, "fail to load grantee id entries for migration, skipping this grant entirely",
					mlog.String("key", oldGranteeIDKey), mlog.Err(loadErr))
				continue
			}

			// Build new key with new collection name and recompute idStr
			// to avoid sharing permission space with a future collection
			// that reuses the old name.
			newObjName := funcutil.CombineObjectName(newDBName, newName)
			newKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], newObjName)
			newIdStr := crypto.GranteeID(newKey)
			saves[newKey] = newIdStr
			removeKeys = append(removeKeys, oldKey)

			// Migrate GranteeIDPrefix entries from oldIdStr to newIdStr
			for j, idKey := range idKeys {
				// Use AfterN to extract privilege name correctly regardless of
				// etcd rootPath prefix in the returned key.
				privilegeName := typeutil.After(idKey, oldGranteeIDKey)
				if privilegeName == "" {
					mlog.Warn(ctx, "failed to extract privilege name from grantee id key",
						mlog.String("idKey", idKey), mlog.String("prefix", oldGranteeIDKey))
					continue
				}
				newIDKey := fmt.Sprintf("%s/%s/%s", GranteeIDPrefix, newIdStr, privilegeName)
				saves[newIDKey] = idValues[j]
				// Reconstruct logical key for deletion
				oldIDKey := fmt.Sprintf("%s/%s/%s", GranteeIDPrefix, oldIdStr, privilegeName)
				removeKeys = append(removeKeys, oldIDKey)
			}
		}
	}

	if len(removeKeys) == 0 {
		return nil
	}

	// Use MultiSaveAndRemove (exact deletion) instead of prefix-based deletion
	// to avoid accidentally matching keys like col1_backup when removing col1
	if err = kc.Txn.MultiSaveAndRemove(ctx, saves, removeKeys); err != nil {
		mlog.Warn(ctx, "fail to migrate grants for renamed collection",
			mlog.String("oldDBName", oldDBName), mlog.String("oldName", oldName),
			mlog.String("newDBName", newDBName), mlog.String("newName", newName), mlog.Err(err))
	}
	return err
}

func (kc *Catalog) DeleteGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error {
	var (
		k          = funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant, role.Name)
		err        error
		removeKeys []string
	)

	removeKeys = append(removeKeys, k)

	// the values are the grantee id list
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, k)
	if err != nil {
		mlog.Warn(ctx, "fail to load grant privilege entities", mlog.String("key", k), mlog.Err(err))
		return err
	}
	removingGrantees := make(map[string]struct{})
	keyWithoutTrailingSlash := strings.TrimSuffix(k, "/")
	for _, key := range keys {
		grantInfos := typeutil.AfterN(key, k, "/")
		if len(grantInfos) != 2 {
			mlog.Warn(ctx, "invalid grantee key while deleting role",
				mlog.String("key", key), mlog.String("prefix", k))
			continue
		}
		removingGrantees[fmt.Sprintf("%s/%s/%s", keyWithoutTrailingSlash, grantInfos[0], grantInfos[1])] = struct{}{}
	}
	var allKeys []string
	var allValues []string
	for _, v := range values {
		if isLegacyGranteeID(v) && allKeys == nil {
			granteePrefix := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
			allKeys, allValues, err = kc.Txn.LoadWithPrefix(ctx, granteePrefix)
			if err != nil {
				mlog.Warn(ctx, "fail to load grant privilege entities for shared id check", mlog.String("key", granteePrefix), mlog.Err(err))
				return err
			}
		}
		if !shouldRemoveGranteeIDSubtree(ctx, funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant), v, allKeys, allValues, removingGrantees) {
			continue
		}
		granteeIDKey := funcutil.HandleTenantForEtcdPrefix(GranteeIDPrefix, tenant, v)
		removeKeys = append(removeKeys, granteeIDKey)
	}

	if err = kc.Txn.MultiSaveAndRemoveWithPrefix(ctx, nil, removeKeys); err != nil {
		mlog.Error(ctx, "fail to remove with the prefix", mlog.String("key", k), mlog.Err(err))
	}
	return err
}

func (kc *Catalog) ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error) {
	var grants []*milvuspb.GrantEntity
	granteeKey := funcutil.HandleTenantForEtcdPrefix(GranteePrefix, tenant)
	keys, values, err := kc.Txn.LoadWithPrefix(ctx, granteeKey)
	if err != nil {
		mlog.Error(ctx, "fail to load all grant privilege entities", mlog.String("key", granteeKey), mlog.Err(err))
		return []*milvuspb.GrantEntity{}, err
	}

	for i, key := range keys {
		grantInfos := typeutil.AfterN(key, granteeKey, "/")
		if len(grantInfos) != 3 ||
			funcutil.IsEmptyString(grantInfos[0]) ||
			funcutil.IsEmptyString(grantInfos[1]) ||
			funcutil.IsEmptyString(grantInfos[2]) {
			mlog.Warn(ctx, "invalid grantee key", mlog.String("string", key), mlog.String("sub_string", granteeKey))
			continue
		}
		logicalGranteeKey := fmt.Sprintf("%s/%s/%s/%s", GranteePrefix, grantInfos[0], grantInfos[1], grantInfos[2])
		idKeys, _, granteeIDKey, err := kc.loadGranteeIDPrefixWithLoadedGrantees(ctx, tenant, logicalGranteeKey, values[i], keys, values)
		if err != nil {
			mlog.Error(ctx, "fail to load the grantee ids", mlog.String("key", granteeIDKey), mlog.Err(err))
			return []*milvuspb.GrantEntity{}, err
		}
		for _, idKey := range idKeys {
			granteeIDInfos := typeutil.AfterN(idKey, granteeIDKey, "/")
			if len(granteeIDInfos) != 1 || funcutil.IsEmptyString(granteeIDInfos[0]) {
				mlog.Warn(ctx, "invalid grantee id", mlog.String("string", idKey), mlog.String("sub_string", granteeIDKey))
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
		mlog.Error(ctx, "fail to load all user-role mappings", mlog.String("key", k), mlog.Err(err))
		return []string{}, err
	}

	for _, key := range keys {
		userRolesInfos := typeutil.AfterN(key, k, "/")
		if len(userRolesInfos) != 2 || funcutil.IsEmptyString(userRolesInfos[0]) || funcutil.IsEmptyString(userRolesInfos[1]) {
			mlog.Warn(ctx, "invalid user-role key", mlog.String("string", key), mlog.String("sub_string", k))
			continue
		}
		userRoles = append(userRoles, funcutil.EncodeUserRoleCache(userRolesInfos[0], userRolesInfos[1]))
	}
	return userRoles, nil
}

func (kc *Catalog) BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error) {
	credentials, err := kc.listCredentials(ctx)
	if err != nil {
		return nil, err
	}

	users := make([]*milvuspb.UserResult, 0, len(credentials))
	credentialPasswords := make(map[string]string, len(credentials))
	for _, credential := range credentials {
		credentialPasswords[credential.Username] = credential.EncryptedPassword
		user, err := kc.getUserResult(ctx, tenant, credential, true)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}

	userInfos := lo.FilterMap(users, func(entity *milvuspb.UserResult, _ int) (*milvuspb.UserInfo, bool) {
		userName := entity.GetUser().GetName()
		if userName == util.UserRoot {
			return nil, false
		}
		return &milvuspb.UserInfo{
			User:     userName,
			Password: credentialPasswords[userName],
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

	for _, grant := range meta.GetGrants() {
		privName := grant.GetGrantor().GetPrivilege().GetName()
		switch {
		case util.IsAnyWord(privName):
		case util.IsPrivilegeNameDefined(privName):
			grant.Grantor.Privilege.Name = util.PrivilegeNameForMetastore(privName)
		default:
			grant.Grantor.Privilege.Name = util.PrivilegeGroupNameForMetastore(privName)
		}
		if err := kc.AlterGrant(ctx, tenant, grant, milvuspb.OperatePrivilegeType_Grant); err != nil {
			return errors.Wrap(err, "failed to alter grant")
		}
	}

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
			return nil, merr.WrapErrParameterInvalidMsg("privilege group [%s] does not exist", groupName)
		}
		mlog.Error(ctx, "failed to load privilege group", mlog.String("group", groupName), mlog.Err(err))
		return nil, err
	}
	privGroupInfo := &milvuspb.PrivilegeGroupInfo{}
	err = proto.Unmarshal([]byte(val), privGroupInfo)
	if err != nil {
		mlog.Error(ctx, "failed to unmarshal privilege group info", mlog.Err(err))
		return nil, err
	}
	return privGroupInfo, nil
}

func (kc *Catalog) DropPrivilegeGroup(ctx context.Context, groupName string) error {
	k := BuildPrivilegeGroupkey(groupName)
	err := kc.Txn.Remove(ctx, k)
	if err != nil {
		mlog.Warn(ctx, "fail to drop privilege group", mlog.String("key", k), mlog.Err(err))
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
		mlog.Error(ctx, "failed to marshal privilege group info", mlog.Err(err))
		return err
	}
	if err = kc.Txn.Save(ctx, k, string(v)); err != nil {
		mlog.Warn(ctx, "fail to put privilege group", mlog.String("key", k), mlog.Err(err))
		return err
	}
	return nil
}

func (kc *Catalog) ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error) {
	_, vals, err := kc.Txn.LoadWithPrefix(ctx, PrivilegeGroupPrefix+"/")
	if err != nil {
		mlog.Error(ctx, "failed to list privilege groups", mlog.String("prefix", PrivilegeGroupPrefix), mlog.Err(err))
		return nil, err
	}
	privGroups := make([]*milvuspb.PrivilegeGroupInfo, 0, len(vals))
	for _, val := range vals {
		privGroupInfo := &milvuspb.PrivilegeGroupInfo{}
		err = proto.Unmarshal([]byte(val), privGroupInfo)
		if err != nil {
			mlog.Error(ctx, "failed to unmarshal privilege group info", mlog.Err(err))
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
		mlog.Error(ctx, "failed to marshal resource info", mlog.Err(err))
		return err
	}
	kvs[k] = string(v)
	kvs[FileResourceVersionKey] = fmt.Sprint(version)

	if err = kc.Txn.MultiSave(ctx, kvs); err != nil {
		mlog.Warn(ctx, "fail to save resource info", mlog.String("key", k), mlog.Err(err))
		return err
	}
	return nil
}

func (kc *Catalog) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	k := BuildFileResourceKey(resourceID)
	if err := kc.Txn.MultiSaveAndRemove(ctx, map[string]string{FileResourceVersionKey: fmt.Sprint(version)}, []string{k}); err != nil {
		mlog.Warn(ctx, "fail to remove resource info", mlog.String("key", k), mlog.Err(err))
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
