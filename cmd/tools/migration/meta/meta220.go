package meta

import (
	"github.com/blang/semver/v4"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/cmd/tools/migration/versions"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util"
)

type TtCollectionsMeta220 map[UniqueID]map[Timestamp]*model.Collection // coll_id -> ts -> coll
type CollectionsMeta220 map[UniqueID]*model.Collection                 // coll_id -> coll

type TtAliasesMeta220 map[string]map[Timestamp]*model.Alias // alias name -> ts -> coll
type AliasesMeta220 map[string]*model.Alias                 // alias name -> coll

type TtPartitionsMeta220 map[UniqueID]map[Timestamp][]*model.Partition // coll_id -> ts -> partitions
type PartitionsMeta220 map[UniqueID][]*model.Partition                 // coll_id -> ts -> partitions

type TtFieldsMeta220 map[UniqueID]map[Timestamp][]*model.Field // coll_id -> ts -> fields
type FieldsMeta220 map[UniqueID][]*model.Field                 // coll_id -> ts -> fields

type CollectionIndexesMeta220 map[UniqueID]map[UniqueID]*model.Index     // coll_id -> index_id -> index
type SegmentIndexesMeta220 map[UniqueID]map[UniqueID]*model.SegmentIndex // seg_id -> index_id -> segment index

type CollectionLoadInfo220 map[UniqueID]*model.CollectionLoadInfo            // collectionID -> CollectionLoadInfo
type PartitionLoadInfo220 map[UniqueID]map[UniqueID]*model.PartitionLoadInfo // collectionID, partitionID -> PartitionLoadInfo

func (meta *TtCollectionsMeta220) GenerateSaves(sourceVersion semver.Version) (map[string]string, error) {
	saves := make(map[string]string)

	opts := make([]model.Option, 0)
	if sourceVersion.LT(versions.Version220) {
		opts = append(opts, model.WithFields())
		opts = append(opts, model.WithPartitions())
	}

	for collectionID := range *meta {
		for ts := range (*meta)[collectionID] {
			ckey := rootcoord.BuildCollectionKey(util.NonDBID, collectionID)
			key := rootcoord.ComposeSnapshotKey(rootcoord.SnapshotPrefix, ckey, rootcoord.SnapshotsSep, ts)
			collection := (*meta)[collectionID][ts]
			var value string
			if collection == nil {
				// save a tombstone.
				value = string(rootcoord.ConstructTombstone())
			} else {
				collectionPb := model.MarshalCollectionModelWithOption(collection, opts...)
				marshaledCollectionPb, err := proto.Marshal(collectionPb)
				if err != nil {
					return nil, err
				}
				value = string(marshaledCollectionPb)
			}
			saves[key] = value
		}
	}

	return saves, nil
}

func (meta *TtCollectionsMeta220) AddCollection(collectionID UniqueID, coll *model.Collection, ts Timestamp) {
	_, collExist := (*meta)[collectionID]
	if collExist {
		(*meta)[collectionID][ts] = coll
	} else {
		(*meta)[collectionID] = map[Timestamp]*model.Collection{
			ts: coll,
		}
	}
}

func (meta *CollectionsMeta220) AddCollection(collectionID UniqueID, coll *model.Collection) {
	(*meta)[collectionID] = coll
}

func (meta *CollectionsMeta220) GenerateSaves(sourceVersion semver.Version) (map[string]string, error) {
	saves := make(map[string]string)

	opts := make([]model.Option, 0)
	if sourceVersion.LT(versions.Version220) {
		opts = append(opts, model.WithFields())
		opts = append(opts, model.WithPartitions())
	}

	for collectionID := range *meta {
		ckey := rootcoord.BuildCollectionKey(util.NonDBID, collectionID)
		collection := (*meta)[collectionID]
		var value string
		if collection == nil {
			// save a tombstone.
			value = string(rootcoord.ConstructTombstone())
		} else {
			collectionPb := model.MarshalCollectionModelWithOption(collection, opts...)
			marshaledCollectionPb, err := proto.Marshal(collectionPb)
			if err != nil {
				return nil, err
			}
			value = string(marshaledCollectionPb)
		}
		saves[ckey] = value
	}

	return saves, nil
}

func (meta *TtAliasesMeta220) AddAlias(alias string, aliasInfo *model.Alias, ts Timestamp) {
	_, aliasExist := (*meta)[alias]
	if aliasExist {
		(*meta)[alias][ts] = aliasInfo
	} else {
		(*meta)[alias] = map[Timestamp]*model.Alias{
			ts: aliasInfo,
		}
	}
}

func (meta *TtAliasesMeta220) GenerateSaves() (map[string]string, error) {
	saves := make(map[string]string)

	for alias := range *meta {
		for ts := range (*meta)[alias] {
			ckey := rootcoord.BuildAliasKey(alias)
			key := rootcoord.ComposeSnapshotKey(rootcoord.SnapshotPrefix, ckey, rootcoord.SnapshotsSep, ts)
			aliasInfo := (*meta)[alias][ts]
			var value string
			if aliasInfo == nil {
				// save a tombstone.
				value = string(rootcoord.ConstructTombstone())
			} else {
				aliasPb := model.MarshalAliasModel(aliasInfo)
				marshaledAliasPb, err := proto.Marshal(aliasPb)
				if err != nil {
					return nil, err
				}
				value = string(marshaledAliasPb)
			}
			saves[key] = value
		}
	}

	return saves, nil
}

func (meta *AliasesMeta220) AddAlias(alias string, aliasInfo *model.Alias) {
	(*meta)[alias] = aliasInfo
}

func (meta *AliasesMeta220) GenerateSaves() (map[string]string, error) {
	saves := make(map[string]string)

	for alias := range *meta {
		ckey := rootcoord.BuildAliasKey(alias)
		aliasInfo := (*meta)[alias]
		var value string
		if aliasInfo == nil {
			// save a tombstone.
			value = string(rootcoord.ConstructTombstone())
		} else {
			aliasPb := model.MarshalAliasModel(aliasInfo)
			marshaledAliasPb, err := proto.Marshal(aliasPb)
			if err != nil {
				return nil, err
			}
			value = string(marshaledAliasPb)
		}
		saves[ckey] = value
	}

	return saves, nil
}

func (meta *CollectionIndexesMeta220) AddRecord(collID UniqueID, indexID int64, record *model.Index) {
	if _, collExist := (*meta)[collID]; !collExist {
		(*meta)[collID] = map[UniqueID]*model.Index{
			indexID: record,
		}
	} else {
		(*meta)[collID][indexID] = record
	}
}

func (meta *CollectionIndexesMeta220) GenerateSaves() (map[string]string, error) {
	saves := make(map[string]string)

	for collectionID := range *meta {
		for indexID := range (*meta)[collectionID] {
			ckey := datacoord.BuildIndexKey(collectionID, indexID)
			index := (*meta)[collectionID][indexID]
			var value string
			indexPb := model.MarshalIndexModel(index)
			marshaledIndexPb, err := proto.Marshal(indexPb)
			if err != nil {
				return nil, err
			}
			value = string(marshaledIndexPb)
			saves[ckey] = value
		}
	}

	return saves, nil
}

func (meta *SegmentIndexesMeta220) GenerateSaves() (map[string]string, error) {
	saves := make(map[string]string)

	for segmentID := range *meta {
		for indexID := range (*meta)[segmentID] {
			index := (*meta)[segmentID][indexID]
			ckey := datacoord.BuildSegmentIndexKey(index.CollectionID, index.PartitionID, index.SegmentID, index.BuildID)
			var value string
			indexPb := model.MarshalSegmentIndexModel(index)
			marshaledIndexPb, err := proto.Marshal(indexPb)
			if err != nil {
				return nil, err
			}
			value = string(marshaledIndexPb)
			saves[ckey] = value
		}
	}

	return saves, nil
}

func (meta *SegmentIndexesMeta220) AddRecord(segID UniqueID, indexID UniqueID, record *model.SegmentIndex) {
	if _, segExist := (*meta)[segID]; !segExist {
		(*meta)[segID] = map[UniqueID]*model.SegmentIndex{
			indexID: record,
		}
	} else {
		(*meta)[segID][indexID] = record
	}
}

func (meta *CollectionLoadInfo220) GenerateSaves() (map[string]string, error) {
	saves := make(map[string]string)
	for _, loadInfo := range *meta {
		k := querycoord.EncodeCollectionLoadInfoKey(loadInfo.CollectionID)
		v, err := proto.Marshal(&querypb.CollectionLoadInfo{
			CollectionID:       loadInfo.CollectionID,
			ReleasedPartitions: loadInfo.ReleasedPartitionIDs,
			ReplicaNumber:      loadInfo.ReplicaNumber,
			Status:             loadInfo.Status,
			FieldIndexID:       loadInfo.FieldIndexID,
		})
		if err != nil {
			return nil, err
		}
		saves[k] = string(v)
	}
	return saves, nil
}

func (meta *PartitionLoadInfo220) GenerateSaves() (map[string]string, error) {
	saves := make(map[string]string)
	for _, partitions := range *meta {
		for _, loadInfo := range partitions {
			k := querycoord.EncodePartitionLoadInfoKey(loadInfo.CollectionID, loadInfo.PartitionID)
			v, err := proto.Marshal(&querypb.PartitionLoadInfo{
				CollectionID:  loadInfo.CollectionID,
				PartitionID:   loadInfo.PartitionID,
				ReplicaNumber: loadInfo.ReplicaNumber,
				Status:        loadInfo.Status,
				FieldIndexID:  loadInfo.FieldIndexID,
			})
			if err != nil {
				return nil, err
			}
			saves[k] = string(v)
		}
	}
	return saves, nil
}

type All220 struct {
	TtCollections TtCollectionsMeta220
	Collections   CollectionsMeta220

	TtAliases TtAliasesMeta220
	Aliases   AliasesMeta220

	TtPartitions TtPartitionsMeta220
	Partitions   PartitionsMeta220

	TtFields TtFieldsMeta220
	Fields   FieldsMeta220

	CollectionIndexes CollectionIndexesMeta220
	SegmentIndexes    SegmentIndexesMeta220

	// QueryCoord Meta
	CollectionLoadInfos CollectionLoadInfo220
	PartitionLoadInfos  PartitionLoadInfo220
}
