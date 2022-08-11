package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/contextutil"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
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

func (tc *Catalog) CreateCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert collection
		var startPositionsStr string
		if collection.StartPositions != nil {
			startPositionsBytes, err := json.Marshal(collection.StartPositions)
			if err != nil {
				log.Error("marshal collection start positions error", zap.Int64("collID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
				return err
			}
			startPositionsStr = string(startPositionsBytes)
		}

		err := tc.metaDomain.CollectionDb(txCtx).Insert(&dbmodel.Collection{
			TenantID:         tenantID,
			CollectionID:     collection.CollectionID,
			CollectionName:   collection.Name,
			Description:      collection.Description,
			AutoID:           collection.AutoID,
			ShardsNum:        collection.ShardsNum,
			StartPosition:    startPositionsStr,
			ConsistencyLevel: int32(collection.ConsistencyLevel),
			Ts:               ts,
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

func (tc *Catalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	tenantID := contextutil.TenantID(ctx)

	// get latest timestamp less than or equals to param ts
	cidTsPair, err := tc.metaDomain.CollectionDb(ctx).GetCollectionIDTs(tenantID, collectionID, ts)
	if err != nil {
		return nil, err
	}
	if cidTsPair.IsDeleted {
		log.Error("not found collection", zap.Int64("collID", collectionID), zap.Uint64("ts", ts))
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

	// get indexes by collection_id
	indexes, err := tc.metaDomain.IndexDb(ctx).Get(tenantID, collectionID)
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
	mCollection.FieldIDToIndexID = dbmodel.ConvertIndexDBToModel(indexes)

	return mCollection, nil
}

func (tc *Catalog) GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	tenantID := contextutil.TenantID(ctx)

	// Since collection name will not change for different ts
	collectionID, err := tc.metaDomain.CollectionDb(ctx).GetCollectionIDByName(tenantID, collectionName, ts)
	if err != nil {
		return nil, err
	}

	return tc.GetCollectionByID(ctx, collectionID, ts)
}

// ListCollections For time travel (ts > 0), find only one record respectively for each collection no matter `is_deleted` is true or false
// i.e. there are 3 collections in total,
// [collection1, t1, is_deleted=true]
// [collection2, t2, is_deleted=false]
// [collection3, t3, is_deleted=false]
// t1, t2, t3 are the largest timestamp that less than or equal to @param ts
// the final result will only return collection2 and collection3 since collection1 is deleted
func (tc *Catalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	tenantID := contextutil.TenantID(ctx)

	// 1. find each collection_id with latest ts <= @param ts
	cidTsPairs, err := tc.metaDomain.CollectionDb(ctx).ListCollectionIDTs(tenantID, ts)
	if err != nil {
		return nil, err
	}
	if len(cidTsPairs) == 0 {
		return map[string]*model.Collection{}, nil
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

	r := map[string]*model.Collection{}
	for _, c := range collections {
		r[c.Name] = c
	}

	return r, nil
}

func (tc *Catalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
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
			log.Error("insert tombstone record for collections failed", zap.String("tenant", tenantID), zap.Int64("collID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
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
				log.Error("insert tombstone record for collection_aliases failed", zap.String("tenant", tenantID), zap.Int64("collID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
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
			log.Error("insert tombstone record for collection_channels failed", zap.String("tenant", tenantID), zap.Int64("collID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
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
			log.Error("insert tombstone record for field_schemas failed", zap.String("tenant", tenantID), zap.Int64("collID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
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
			log.Error("insert tombstone record for partitions failed", zap.String("tenant", tenantID), zap.Int64("collID", collection.CollectionID), zap.Uint64("ts", ts), zap.Error(err))
			return err
		}

		// 6. mark deleted for indexes
		err = tc.metaDomain.IndexDb(txCtx).MarkDeletedByCollectionID(tenantID, collection.CollectionID)
		if err != nil {
			log.Error("mark deleted for indexes failed", zap.String("tenant", tenantID), zap.Int64("collID", collection.CollectionID), zap.Error(err))
			return err
		}

		// 7. mark deleted for segment_indexes
		err = tc.metaDomain.SegmentIndexDb(txCtx).MarkDeletedByCollectionID(tenantID, collection.CollectionID)
		if err != nil {
			log.Error("mark deleted for segment_indexes failed", zap.String("tenant", tenantID), zap.Int64("collID", collection.CollectionID), zap.Error(err))
			return err
		}

		return err
	})
}

func (tc *Catalog) CreatePartition(ctx context.Context, partition *model.Partition, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)

	p := &dbmodel.Partition{
		TenantID:                  tenantID,
		PartitionID:               partition.PartitionID,
		PartitionName:             partition.PartitionName,
		PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
		CollectionID:              partition.CollectionID,
		Ts:                        ts,
	}
	err := tc.metaDomain.PartitionDb(ctx).Insert([]*dbmodel.Partition{p})
	if err != nil {
		log.Error("insert partitions failed", zap.String("tenant", tenantID), zap.Int64("collID", partition.CollectionID), zap.Int64("partitionID", partition.PartitionID), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) DropPartition(ctx context.Context, collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
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
		log.Error("insert tombstone record for partition failed", zap.String("tenant", tenantID), zap.Int64("collID", collectionID), zap.Int64("partitionID", partitionID), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) CreateIndex(ctx context.Context, col *model.Collection, index *model.Index) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert index
		indexParamsBytes, err := json.Marshal(index.IndexParams)
		if err != nil {
			log.Error("marshal IndexParams of index failed", zap.String("tenant", tenantID), zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName), zap.Error(err))
		}
		indexParamsStr := string(indexParamsBytes)

		idx := &dbmodel.Index{
			TenantID:     tenantID,
			CollectionID: index.CollectionID,
			FieldID:      index.FieldID,
			IndexID:      index.IndexID,
			IndexName:    index.IndexName,
			IndexParams:  indexParamsStr,
		}

		err = tc.metaDomain.IndexDb(txCtx).Insert([]*dbmodel.Index{idx})
		if err != nil {
			log.Error("insert indexes failed", zap.String("tenant", tenantID), zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName), zap.Error(err))
			return err
		}

		// insert segment_indexes
		if len(index.SegmentIndexes) > 0 {
			segIndexes := make([]*dbmodel.SegmentIndex, 0, len(index.SegmentIndexes))

			for _, segIndex := range index.SegmentIndexes {
				indexFilePaths, err := json.Marshal(segIndex.IndexFilePaths)
				if err != nil {
					log.Error("marshal IndexFilePaths failed", zap.String("tenant", tenantID), zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID), zap.Int64("segmentID", segIndex.SegmentID), zap.Error(err))
					return err
				}
				indexFilePathsStr := string(indexFilePaths)
				si := &dbmodel.SegmentIndex{
					TenantID:       tenantID,
					CollectionID:   index.CollectionID,
					PartitionID:    segIndex.PartitionID,
					SegmentID:      segIndex.SegmentID,
					FieldID:        index.FieldID,
					IndexID:        index.IndexID,
					IndexBuildID:   segIndex.BuildID,
					EnableIndex:    segIndex.EnableIndex,
					IndexFilePaths: indexFilePathsStr,
					IndexSize:      segIndex.IndexSize,
				}
				segIndexes = append(segIndexes, si)
			}

			err := tc.metaDomain.SegmentIndexDb(txCtx).Insert(segIndexes)
			if err != nil {
				log.Error("insert segment_indexes failed", zap.String("tenant", tenantID), zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName), zap.Error(err))
				return err
			}
		}

		return nil
	})
}

func (tc *Catalog) AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index, alterType metastore.AlterType) error {
	switch alterType {
	case metastore.ADD:
		return tc.alterAddIndex(ctx, oldIndex, newIndex)
	case metastore.DELETE:
		return tc.alterDeleteIndex(ctx, oldIndex, newIndex)
	default:
		return errors.New("Unknown alter type:" + fmt.Sprintf("%d", alterType))
	}
}

func (tc *Catalog) alterAddIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		adds := make([]*dbmodel.SegmentIndex, 0, len(newIndex.SegmentIndexes))

		for segID, newSegIndex := range newIndex.SegmentIndexes {
			oldSegIndex, ok := oldIndex.SegmentIndexes[segID]
			if !ok || !reflect.DeepEqual(oldSegIndex, newSegIndex) {
				segment := newSegIndex.Segment
				segIdxInfo := &dbmodel.SegmentIndex{
					TenantID:     tenantID,
					CollectionID: newIndex.CollectionID,
					PartitionID:  segment.PartitionID,
					SegmentID:    segment.SegmentID,
					FieldID:      newIndex.FieldID,
					IndexID:      newIndex.IndexID,
					IndexBuildID: newSegIndex.BuildID,
					EnableIndex:  newSegIndex.EnableIndex,
					CreateTime:   newSegIndex.CreateTime,
					//IndexFilePaths: indexFilePathsStr,
					//IndexSize:      newSegIndex.IndexSize,
				}

				adds = append(adds, segIdxInfo)
			}
		}

		if len(adds) == 0 {
			return nil
		}

		// upsert segment_indexes
		err := tc.metaDomain.SegmentIndexDb(txCtx).Upsert(adds)
		if err != nil {
			log.Error("upsert segment_indexes failed", zap.String("tenant", tenantID), zap.Int64("collectionID", newIndex.CollectionID), zap.Int64("indexID", newIndex.IndexID), zap.Error(err))
			return err
		}

		// update index info
		if oldIndex.CreateTime != newIndex.CreateTime || oldIndex.IsDeleted != newIndex.IsDeleted {
			indexParamsBytes, err := json.Marshal(newIndex.IndexParams)
			if err != nil {
				log.Error("marshal IndexParams of index failed", zap.String("tenant", tenantID), zap.Int64("collectionID", newIndex.CollectionID), zap.Int64("indexID", newIndex.IndexID), zap.Error(err))
				return err
			}
			indexParamsStr := string(indexParamsBytes)

			index := &dbmodel.Index{
				TenantID:     tenantID,
				CollectionID: newIndex.CollectionID,
				IndexName:    newIndex.IndexName,
				IndexID:      newIndex.IndexID,
				IndexParams:  indexParamsStr,
				IsDeleted:    newIndex.IsDeleted,
				CreateTime:   newIndex.CreateTime,
			}
			err = tc.metaDomain.IndexDb(txCtx).Update(index)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (tc *Catalog) alterDeleteIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index) error {
	tenantID := contextutil.TenantID(ctx)
	delSegIndexes := make([]*dbmodel.SegmentIndex, 0, len(newIndex.SegmentIndexes))

	for _, segIdx := range newIndex.SegmentIndexes {
		segIndex := &dbmodel.SegmentIndex{
			SegmentID: segIdx.SegmentID,
			IndexID:   newIndex.IndexID,
		}
		delSegIndexes = append(delSegIndexes, segIndex)
	}

	if len(delSegIndexes) == 0 {
		return nil
	}

	err := tc.metaDomain.SegmentIndexDb(ctx).MarkDeleted(tenantID, delSegIndexes)
	if err != nil {
		log.Error("mark SegmentIndex deleted failed", zap.Int64("collID", newIndex.CollectionID), zap.Int64("indexID", newIndex.IndexID), zap.Error(err))
		return err
	}

	return err
}

func (tc *Catalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// mark deleted for index
		err := tc.metaDomain.IndexDb(txCtx).MarkDeletedByIndexID(tenantID, dropIdxID)
		if err != nil {
			return err
		}

		// mark deleted for segment_indexes
		err = tc.metaDomain.SegmentIndexDb(txCtx).MarkDeletedByIndexID(tenantID, dropIdxID)
		if err != nil {
			return err
		}

		return nil
	})
}

func (tc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	tenantID := contextutil.TenantID(ctx)

	rs, err := tc.metaDomain.IndexDb(ctx).List(tenantID)
	if err != nil {
		return nil, err
	}

	result, err := dbmodel.UnmarshalIndexModel(rs)
	if err != nil {
		return nil, err
	}

	return result, nil
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
		log.Error("insert collection_aliases failed", zap.Int64("collID", alias.CollectionID), zap.String("alias", alias.Name), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) DropAlias(ctx context.Context, alias string, ts typeutil.Timestamp) error {
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
		log.Error("insert tombstone record for collection_aliases failed", zap.Int64("collID", collectionID), zap.String("collAlias", alias), zap.Uint64("ts", ts), zap.Error(err))
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
func (tc *Catalog) ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Alias, error) {
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

	usernames, err := tc.metaDomain.UserDb(ctx).ListUsername(tenantID)
	if err != nil {
		return nil, err
	}

	return usernames, nil
}

func (tc *Catalog) CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error {
	//TODO implement me
	return nil
}

func (tc *Catalog) DropRole(ctx context.Context, tenant string, roleName string) error {
	//TODO implement me
	return nil
}

func (tc *Catalog) OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error {
	//TODO implement me
	return nil
}

func (tc *Catalog) SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error) {
	//TODO implement me
	return nil, nil
}

func (tc *Catalog) SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error) {
	//TODO implement me
	return nil, nil
}

func (tc *Catalog) OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error {
	//TODO implement me
	return nil
}

func (tc *Catalog) SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error) {
	//TODO implement me
	return nil, nil
}

func (tc *Catalog) ListPolicy(ctx context.Context, tenant string) ([]string, error) {
	//TODO implement me
	return nil, nil
}

func (tc *Catalog) ListUserRole(ctx context.Context, tenant string) ([]string, error) {
	//TODO implement me
	return nil, nil
}

func (tc *Catalog) Close() {

}
