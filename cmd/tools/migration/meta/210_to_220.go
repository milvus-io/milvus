package meta

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/cmd/tools/migration/allocator"
	"github.com/milvus-io/milvus/cmd/tools/migration/legacy/legacypb"
	"github.com/milvus-io/milvus/cmd/tools/migration/versions"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	pb "github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func alias210ToAlias220(record *pb.CollectionInfo, ts Timestamp) *model.Alias {
	if record == nil {
		return nil
	}
	return &model.Alias{
		Name:         record.GetSchema().GetName(),
		CollectionID: record.GetID(),
		CreatedTime:  ts,
		State:        pb.AliasState_AliasCreated,
	}
}

func (meta *TtAliasesMeta210) to220() (TtAliasesMeta220, error) {
	ttAliases := make(TtAliasesMeta220)
	for alias := range *meta {
		for ts := range (*meta)[alias] {
			aliasModel := alias210ToAlias220((*meta)[alias][ts], ts)
			ttAliases.AddAlias(alias, aliasModel, ts)
		}
	}
	return ttAliases, nil
}

func (meta *AliasesMeta210) to220() (AliasesMeta220, error) {
	aliases := make(AliasesMeta220)
	for alias := range *meta {
		aliasModel := alias210ToAlias220((*meta)[alias], 0)
		aliases.AddAlias(alias, aliasModel)
	}
	return aliases, nil
}

func getLatestFieldIndexes(colls map[Timestamp]*pb.CollectionInfo) *FieldIndexesWithSchema {
	type pair struct {
		ts   Timestamp
		coll *pb.CollectionInfo
	}
	l := len(colls)
	pairs := make([]pair, l)
	for ts, coll := range colls {
		pairs = append(pairs, pair{ts: ts, coll: coll})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].ts < pairs[j].ts
	})
	if l > 0 && pairs[l-1].coll != nil {
		return &FieldIndexesWithSchema{indexes: pairs[l-1].coll.GetFieldIndexes(), schema: pairs[l-1].coll.GetSchema()}
	}
	return nil
}

func collection210ToCollection220(coll *pb.CollectionInfo) *model.Collection {
	return model.UnmarshalCollectionModel(coll)
}

func (meta *TtCollectionsMeta210) to220() (TtCollectionsMeta220, FieldIndexes210, error) {
	ttCollections := make(TtCollectionsMeta220)
	fieldIndexes := make(FieldIndexes210)
	for collectionID := range *meta {
		colls := (*meta)[collectionID]
		indexes := getLatestFieldIndexes(colls)
		if indexes != nil {
			fieldIndexes.AddRecord(collectionID, indexes.indexes, indexes.schema)
		}
		for ts := range colls {
			coll := colls[ts]
			ttCollections.AddCollection(collectionID, collection210ToCollection220(coll), ts)
		}
	}
	return ttCollections, fieldIndexes, nil
}

func (meta *CollectionsMeta210) to220() (CollectionsMeta220, FieldIndexes210, error) {
	collections := make(CollectionsMeta220)
	fieldIndexes := make(FieldIndexes210)
	for collectionID := range *meta {
		coll := (*meta)[collectionID]
		fieldIndexes.AddRecord(collectionID, coll.GetFieldIndexes(), coll.GetSchema())
		collections.AddCollection(collectionID, collection210ToCollection220(coll))
	}
	return collections, fieldIndexes, nil
}

func (meta *CollectionLoadInfo210) to220() (CollectionLoadInfo220, PartitionLoadInfo220, error) {
	collectionLoadInfos := make(CollectionLoadInfo220)
	partitionLoadInfos := make(PartitionLoadInfo220)
	for collectionID, loadInfo := range *meta {
		if loadInfo.LoadPercentage < 100 {
			continue
		}

		switch loadInfo.LoadType {
		case querypb.LoadType_LoadCollection:
			collectionLoadInfos[collectionID] = loadInfo
		case querypb.LoadType_LoadPartition:
			partitions, ok := partitionLoadInfos[collectionID]
			if !ok {
				partitions = make(map[int64]*model.PartitionLoadInfo)
				partitionLoadInfos[collectionID] = partitions
			}
			for _, partitionID := range loadInfo.PartitionIDs {
				partitions[partitionID] = &model.PartitionLoadInfo{
					CollectionID:   collectionID,
					PartitionID:    partitionID,
					LoadType:       querypb.LoadType_LoadPartition,
					LoadPercentage: 100,
					Status:         querypb.LoadStatus_Loaded,
					ReplicaNumber:  loadInfo.ReplicaNumber,
					FieldIndexID:   make(map[UniqueID]UniqueID),
				}
			}
		}
	}

	return collectionLoadInfos, partitionLoadInfos, nil
}

func combineToCollectionIndexesMeta220(fieldIndexes FieldIndexes210, collectionIndexes CollectionIndexesMeta210) (CollectionIndexesMeta220, error) {
	indexes := make(CollectionIndexesMeta220)
	for collectionID := range fieldIndexes {
		record := fieldIndexes[collectionID]
		if record.schema == nil {
			fmt.Println("combineToCollectionIndexesMeta220, nil schema: ", collectionID, ", record: ", record)
			continue
		}
		helper, err := typeutil.CreateSchemaHelper(record.schema)
		if err != nil {
			return nil, err
		}
		for _, index := range record.indexes {
			field, err := helper.GetFieldFromID(index.GetFiledID())
			if err != nil {
				return nil, err
			}
			indexInfo, err := collectionIndexes.GetIndex(collectionID, index.GetIndexID())
			if err != nil {
				return nil, err
			}
			newIndexParamsMap := make(map[string]string)
			for _, kv := range indexInfo.IndexParams {
				if kv.Key == common.IndexParamsKey {
					params, err := funcutil.JSONToMap(kv.Value)
					if err != nil {
						return nil, err
					}
					for k, v := range params {
						newIndexParamsMap[k] = v
					}
				} else {
					newIndexParamsMap[kv.Key] = kv.Value
				}
			}
			newIndexParams := make([]*commonpb.KeyValuePair, 0)
			for k, v := range newIndexParamsMap {
				newIndexParams = append(newIndexParams, &commonpb.KeyValuePair{Key: k, Value: v})
			}
			newIndexName := indexInfo.GetIndexName()
			if newIndexName == "_default_idx" {
				newIndexName = "_default_idx_" + strconv.FormatInt(index.GetFiledID(), 10)
			}
			record := &model.Index{
				TenantID:        "",
				CollectionID:    collectionID,
				FieldID:         index.GetFiledID(),
				IndexID:         index.GetIndexID(),
				IndexName:       newIndexName,
				IsDeleted:       indexInfo.GetDeleted(),
				CreateTime:      indexInfo.GetCreateTime(),
				TypeParams:      field.GetTypeParams(),
				IndexParams:     newIndexParams,
				UserIndexParams: indexInfo.GetIndexParams(),
			}
			indexes.AddRecord(collectionID, index.GetIndexID(), record)
		}
	}
	return indexes, nil
}

func getOrFillBuildMeta(record *pb.SegmentIndexInfo, indexBuildMeta IndexBuildMeta210, alloc allocator.Allocator) (*legacypb.IndexMeta, error) {
	if record.GetBuildID() == 0 && !record.GetEnableIndex() {
		buildID, err := alloc.AllocID()
		if err != nil {
			return nil, err
		}
		buildMeta := &legacypb.IndexMeta{
			IndexBuildID:   buildID,
			State:          commonpb.IndexState_Finished,
			FailReason:     "",
			Req:            nil,
			IndexFilePaths: nil,
			MarkDeleted:    false,
			NodeID:         0,
			IndexVersion:   1, // TODO: maybe a constraint is better.
			Recycled:       false,
			SerializeSize:  0,
		}
		indexBuildMeta[buildID] = buildMeta
		return buildMeta, nil
	}
	buildMeta, ok := indexBuildMeta[record.GetBuildID()]
	if !ok {
		return nil, fmt.Errorf("index build meta not found, segment id: %d, index id: %d, index build id: %d",
			record.GetSegmentID(), record.GetIndexID(), record.GetBuildID())
	}
	return buildMeta, nil
}

func combineToSegmentIndexesMeta220(segmentIndexes SegmentIndexesMeta210, indexBuildMeta IndexBuildMeta210) (SegmentIndexesMeta220, error) {
	alloc := allocator.NewAllocatorFromList(indexBuildMeta.GetAllBuildIDs(), false, true)

	segmentIndexModels := make(SegmentIndexesMeta220)
	for segID := range segmentIndexes {
		for indexID := range segmentIndexes[segID] {
			record := segmentIndexes[segID][indexID]
			buildMeta, err := getOrFillBuildMeta(record, indexBuildMeta, alloc)
			if err != nil {
				return nil, err
			}

			fileKeys := make([]string, len(buildMeta.GetIndexFilePaths()))
			for i, filePath := range buildMeta.GetIndexFilePaths() {
				parts := strings.Split(filePath, "/")
				if len(parts) == 0 {
					return nil, fmt.Errorf("invaild index file path: %s", filePath)
				}

				fileKeys[i] = parts[len(parts)-1]
			}

			segmentIndexModel := &model.SegmentIndex{
				SegmentID:     segID,
				CollectionID:  record.GetCollectionID(),
				PartitionID:   record.GetPartitionID(),
				NumRows:       buildMeta.GetReq().GetNumRows(),
				IndexID:       indexID,
				BuildID:       record.GetBuildID(),
				NodeID:        buildMeta.GetNodeID(),
				IndexVersion:  buildMeta.GetIndexVersion(),
				IndexState:    buildMeta.GetState(),
				FailReason:    buildMeta.GetFailReason(),
				IsDeleted:     buildMeta.GetMarkDeleted(),
				CreateTime:    record.GetCreateTime(),
				IndexFileKeys: fileKeys,
				IndexSize:     buildMeta.GetSerializeSize(),
				WriteHandoff:  buildMeta.GetState() == commonpb.IndexState_Finished,
			}
			segmentIndexModels.AddRecord(segID, indexID, segmentIndexModel)
		}
	}
	return segmentIndexModels, nil
}

func combineToLoadInfo220(collectionLoadInfo CollectionLoadInfo220, partitionLoadInto PartitionLoadInfo220, fieldIndexes FieldIndexes210) {
	toBeReleased := make([]UniqueID, 0)

	for collectionID, loadInfo := range collectionLoadInfo {
		indexes, ok := fieldIndexes[collectionID]
		if !ok || len(indexes.indexes) == 0 {
			toBeReleased = append(toBeReleased, collectionID)
			continue
		}

		for _, index := range indexes.indexes {
			loadInfo.FieldIndexID[index.GetFiledID()] = index.GetIndexID()
		}
	}

	for collectionID, partitions := range partitionLoadInto {
		indexes, ok := fieldIndexes[collectionID]
		if !ok || len(indexes.indexes) == 0 {
			toBeReleased = append(toBeReleased, collectionID)
			continue
		}

		for _, loadInfo := range partitions {
			for _, index := range indexes.indexes {
				loadInfo.FieldIndexID[index.GetFiledID()] = index.GetIndexID()
			}
		}
	}

	for _, collectionID := range toBeReleased {
		log.Warn("release the collection without index", zap.Int64("collectionID", collectionID))
		delete(collectionLoadInfo, collectionID)
	}
}

func From210To220(metas *Meta) (*Meta, error) {
	if !metas.Version.EQ(versions.Version210) {
		return nil, fmt.Errorf("version mismatch: %s", metas.Version.String())
	}
	ttAliases, err := metas.Meta210.TtAliases.to220()
	if err != nil {
		return nil, err
	}
	aliases, err := metas.Meta210.Aliases.to220()
	if err != nil {
		return nil, err
	}
	ttCollections, fieldIndexes, err := metas.Meta210.TtCollections.to220()
	if err != nil {
		return nil, err
	}
	collections, fieldIndexes2, err := metas.Meta210.Collections.to220()
	if err != nil {
		return nil, err
	}
	collectionLoadInfos, partitionLoadInfos, err := metas.Meta210.CollectionLoadInfos.to220()
	if err != nil {
		return nil, err
	}

	fieldIndexes.Merge(fieldIndexes2)
	collectionIndexes, err := combineToCollectionIndexesMeta220(fieldIndexes, metas.Meta210.CollectionIndexes)
	if err != nil {
		return nil, err
	}
	segmentIndexes, err := combineToSegmentIndexesMeta220(metas.Meta210.SegmentIndexes, metas.Meta210.IndexBuildMeta)
	if err != nil {
		return nil, err
	}
	combineToLoadInfo220(collectionLoadInfos, partitionLoadInfos, fieldIndexes)

	metas220 := &Meta{
		SourceVersion: metas.Version,
		Version:       versions.Version220,
		Meta220: &All220{
			TtCollections:       ttCollections,
			Collections:         collections,
			TtAliases:           ttAliases,
			Aliases:             aliases,
			TtPartitions:        make(TtPartitionsMeta220),
			Partitions:          make(PartitionsMeta220),
			TtFields:            make(TtFieldsMeta220),
			Fields:              make(FieldsMeta220),
			CollectionIndexes:   collectionIndexes,
			SegmentIndexes:      segmentIndexes,
			CollectionLoadInfos: collectionLoadInfos,
			PartitionLoadInfos:  partitionLoadInfos,
		},
	}
	return metas220, nil
}
