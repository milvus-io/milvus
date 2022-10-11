package meta

import (
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/cmd/tools/migration/versions"
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
			record := &model.Index{
				TenantID:     "", // TODO: how to set this if we support mysql later?
				CollectionID: collectionID,
				FieldID:      index.GetFiledID(),
				IndexID:      index.GetIndexID(),
				IndexName:    indexInfo.GetIndexName(),
				IsDeleted:    indexInfo.GetDeleted(),
				CreateTime:   indexInfo.GetCreateTime(),
				TypeParams:   field.GetTypeParams(),
				IndexParams:  indexInfo.GetIndexParams(),
			}
			indexes.AddRecord(collectionID, index.GetIndexID(), record)
		}
	}
	return indexes, nil
}

func combineToSegmentIndexesMeta220(segmentIndexes SegmentIndexesMeta210, indexBuildMeta IndexBuildMeta210) (SegmentIndexesMeta220, error) {
	segmentIndexModels := make(SegmentIndexesMeta220)
	for segID := range segmentIndexes {
		for indexID := range segmentIndexes[segID] {
			record := segmentIndexes[segID][indexID]
			buildMeta, ok := indexBuildMeta[record.GetBuildID()]
			if !ok {
				return nil, fmt.Errorf("index build meta not found, segment id: %d, index id: %d, index build id: %d", segID, indexID, record.GetBuildID())
			}
			segmentIndexModel := &model.SegmentIndex{
				SegmentID:      segID,
				CollectionID:   record.GetCollectionID(),
				PartitionID:    record.GetPartitionID(),
				NumRows:        0, // TODO: how to set this?
				IndexID:        indexID,
				BuildID:        record.GetBuildID(),
				NodeID:         buildMeta.GetNodeID(),
				IndexVersion:   buildMeta.GetIndexVersion(),
				IndexState:     buildMeta.GetState(),
				FailReason:     buildMeta.GetFailReason(),
				IsDeleted:      buildMeta.GetMarkDeleted(),
				CreateTime:     record.GetCreateTime(),
				IndexFilePaths: buildMeta.GetIndexFilePaths(),
				IndexSize:      buildMeta.GetSerializeSize(),
			}
			segmentIndexModels.AddRecord(segID, indexID, segmentIndexModel)
		}
	}
	return segmentIndexModels, nil
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
	fieldIndexes.Merge(fieldIndexes2)
	collectionIndexes, err := combineToCollectionIndexesMeta220(fieldIndexes, metas.Meta210.CollectionIndexes)
	if err != nil {
		return nil, err
	}
	segmentIndexes, err := combineToSegmentIndexesMeta220(metas.Meta210.SegmentIndexes, metas.Meta210.IndexBuildMeta)
	if err != nil {
		return nil, err
	}
	metas220 := &Meta{
		SourceVersion: metas.Version,
		Version:       versions.Version220,
		Meta220: &All220{
			TtCollections:     ttCollections,
			Collections:       collections,
			TtAliases:         ttAliases,
			Aliases:           aliases,
			TtPartitions:      make(TtPartitionsMeta220),
			Partitions:        make(PartitionsMeta220),
			TtFields:          make(TtFieldsMeta220),
			Fields:            make(FieldsMeta220),
			CollectionIndexes: collectionIndexes,
			SegmentIndexes:    segmentIndexes,
		},
	}
	return metas220, nil
}
