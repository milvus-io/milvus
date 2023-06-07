package meta

import (
	"fmt"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/cmd/tools/migration/legacy"
	"github.com/milvus-io/milvus/cmd/tools/migration/legacy/legacypb"
	"github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/util"
)

type FieldIndexesWithSchema struct {
	indexes []*pb.FieldIndexInfo
	schema  *schemapb.CollectionSchema
}

type FieldIndexes210 map[UniqueID]*FieldIndexesWithSchema // coll_id -> field indexes.

type TtCollectionsMeta210 map[UniqueID]map[Timestamp]*pb.CollectionInfo // coll_id -> ts -> coll
type CollectionsMeta210 map[UniqueID]*pb.CollectionInfo                 // coll_id -> coll

type TtAliasesMeta210 map[string]map[Timestamp]*pb.CollectionInfo // alias name -> ts -> coll
type AliasesMeta210 map[string]*pb.CollectionInfo                 // alias name -> coll

type CollectionIndexesMeta210 map[UniqueID]map[UniqueID]*pb.IndexInfo     // coll_id -> index_id -> index
type SegmentIndexesMeta210 map[UniqueID]map[UniqueID]*pb.SegmentIndexInfo // seg_id -> index_id -> segment index

type IndexBuildMeta210 map[UniqueID]*legacypb.IndexMeta // index_build_id -> index

type LastDDLRecords map[string]string // We don't care this since it didn't work.

type CollectionLoadInfo210 map[UniqueID]*model.CollectionLoadInfo // collectionID -> CollectionLoadInfo

type All210 struct {
	TtAliases TtAliasesMeta210
	Aliases   AliasesMeta210

	TtCollections TtCollectionsMeta210
	Collections   CollectionsMeta210

	CollectionIndexes CollectionIndexesMeta210
	SegmentIndexes    SegmentIndexesMeta210
	IndexBuildMeta    IndexBuildMeta210

	LastDDLRecords LastDDLRecords

	CollectionLoadInfos CollectionLoadInfo210
}

func (meta *All210) GenerateSaves() map[string]string {
	ttAliases := meta.TtAliases.GenerateSaves()
	aliases := meta.Aliases.GenerateSaves()
	ttCollections := meta.TtCollections.GenerateSaves()
	collections := meta.Collections.GenerateSaves()
	collectionIndexes := meta.CollectionIndexes.GenerateSaves()
	segmentIndexes := meta.SegmentIndexes.GenerateSaves()
	indexBuilds := meta.IndexBuildMeta.GenerateSaves()
	lastDdlRecords := meta.LastDDLRecords.GenerateSaves()

	return merge(false,
		ttAliases, aliases,
		ttCollections, collections,
		collectionIndexes,
		segmentIndexes,
		indexBuilds,
		lastDdlRecords)
}

func merge(clone bool, kvs ...map[string]string) map[string]string {
	if len(kvs) <= 0 {
		return map[string]string{}
	}
	var ret map[string]string
	var iterator int
	if clone {
		ret = make(map[string]string)
		iterator = 0
	} else {
		ret = kvs[0]
		iterator = 1
	}
	for i := iterator; i < len(kvs); i++ {
		for k, v := range kvs[i] {
			ret[k] = v
		}
	}
	return ret
}

func (meta *TtAliasesMeta210) AddAlias(alias string, info *pb.CollectionInfo, ts Timestamp) {
	if _, aliasExist := (*meta)[alias]; !aliasExist {
		(*meta)[alias] = map[Timestamp]*pb.CollectionInfo{
			ts: info,
		}
	} else {
		(*meta)[alias][ts] = info
	}
}

func (meta *TtAliasesMeta210) GenerateSaves() map[string]string {
	kvs := make(map[string]string)
	var v []byte
	var err error
	for alias := range *meta {
		for ts := range (*meta)[alias] {
			k := rootcoord.ComposeSnapshotKey(rootcoord.SnapshotPrefix, rootcoord.BuildAliasKey210(alias), rootcoord.SnapshotsSep, ts)
			record := (*meta)[alias][ts]
			if record == nil {
				v = rootcoord.ConstructTombstone()
			} else {
				v, err = proto.Marshal(record)
				if err != nil {
					panic(err)
				}
			}
			kvs[k] = string(v)
		}
	}
	return kvs
}

func (meta *AliasesMeta210) AddAlias(alias string, info *pb.CollectionInfo) {
	(*meta)[alias] = info
}

func (meta *AliasesMeta210) GenerateSaves() map[string]string {
	kvs := make(map[string]string)
	var v []byte
	var err error
	for alias := range *meta {
		record := (*meta)[alias]
		k := rootcoord.BuildAliasKey210(alias)
		if record == nil {
			v = rootcoord.ConstructTombstone()
		} else {
			v, err = proto.Marshal(record)
			if err != nil {
				panic(err)
			}
		}
		kvs[k] = string(v)
	}
	return kvs
}

func (meta *TtCollectionsMeta210) AddCollection(collID UniqueID, coll *pb.CollectionInfo, ts Timestamp) {
	if _, collExist := (*meta)[collID]; !collExist {
		(*meta)[collID] = map[Timestamp]*pb.CollectionInfo{
			ts: coll,
		}
	} else {
		(*meta)[collID][ts] = coll
	}
}

func (meta *TtCollectionsMeta210) GenerateSaves() map[string]string {
	kvs := make(map[string]string)
	var v []byte
	var err error
	for collection := range *meta {
		for ts := range (*meta)[collection] {
			k := rootcoord.ComposeSnapshotKey(rootcoord.SnapshotPrefix, rootcoord.BuildCollectionKey(util.NonDBID, collection), rootcoord.SnapshotsSep, ts)
			record := (*meta)[collection][ts]
			if record == nil {
				v = rootcoord.ConstructTombstone()
			} else {
				v, err = proto.Marshal(record)
				if err != nil {
					panic(err)
				}
			}
			kvs[k] = string(v)
		}
	}
	return kvs
}

func (meta *CollectionsMeta210) AddCollection(collID UniqueID, coll *pb.CollectionInfo) {
	(*meta)[collID] = coll
}

func (meta *CollectionsMeta210) GenerateSaves() map[string]string {
	kvs := make(map[string]string)
	var v []byte
	var err error
	for collection := range *meta {
		record := (*meta)[collection]
		k := rootcoord.BuildCollectionKey(util.NonDBID, collection)
		if record == nil {
			v = rootcoord.ConstructTombstone()
		} else {
			v, err = proto.Marshal(record)
			if err != nil {
				panic(err)
			}
		}
		kvs[k] = string(v)
	}
	return kvs
}

func (meta *CollectionIndexesMeta210) AddIndex(collectionID UniqueID, indexID UniqueID, index *pb.IndexInfo) {
	if _, collExist := (*meta)[collectionID]; !collExist {
		(*meta)[collectionID] = map[UniqueID]*pb.IndexInfo{
			indexID: index,
		}
	} else {
		(*meta)[collectionID][indexID] = index
	}
}

func (meta *CollectionIndexesMeta210) GetIndex(collectionID UniqueID, indexID UniqueID) (*pb.IndexInfo, error) {
	if _, collExist := (*meta)[collectionID]; !collExist {
		return nil, fmt.Errorf("collection not exist: %d", collectionID)
	}
	if _, indexExist := (*meta)[collectionID][indexID]; !indexExist {
		return nil, fmt.Errorf("index not exist, collection: %d, index: %d", collectionID, indexID)
	}
	return (*meta)[collectionID][indexID], nil
}

func (meta *CollectionIndexesMeta210) GenerateSaves() map[string]string {
	kvs := make(map[string]string)
	var v []byte
	var err error
	for collectionID := range *meta {
		for indexID := range (*meta)[collectionID] {
			k := legacy.BuildCollectionIndexKey210(collectionID, indexID)
			record := (*meta)[collectionID][indexID]
			if record == nil {
				v = rootcoord.ConstructTombstone()
			} else {
				v, err = proto.Marshal(record)
				if err != nil {
					panic(err)
				}
			}
			kvs[k] = string(v)
		}
	}
	return kvs
}

func (meta *SegmentIndexesMeta210) AddIndex(segmentID UniqueID, indexID UniqueID, index *pb.SegmentIndexInfo) {
	if _, segExist := (*meta)[segmentID]; !segExist {
		(*meta)[segmentID] = map[UniqueID]*pb.SegmentIndexInfo{
			indexID: index,
		}
	} else {
		(*meta)[segmentID][indexID] = index
	}
}

func (meta *SegmentIndexesMeta210) GenerateSaves() map[string]string {
	kvs := make(map[string]string)
	var v []byte
	var err error
	for segmentID := range *meta {
		for indexID := range (*meta)[segmentID] {
			k := legacy.BuildSegmentIndexKey210(segmentID, indexID)
			record := (*meta)[segmentID][indexID]
			if record == nil {
				v = rootcoord.ConstructTombstone()
			} else {
				v, err = proto.Marshal(record)
				if err != nil {
					panic(err)
				}
			}
			kvs[k] = string(v)
		}
	}
	return kvs
}

func (meta *IndexBuildMeta210) AddRecord(indexBuildID UniqueID, record *legacypb.IndexMeta) {
	(*meta)[indexBuildID] = record
}

func (meta *IndexBuildMeta210) GenerateSaves() map[string]string {
	kvs := make(map[string]string)
	var v []byte
	var err error
	for buildID := range *meta {
		record := (*meta)[buildID]
		k := legacy.BuildIndexBuildKey210(buildID)
		v, err = proto.Marshal(record)
		if err != nil {
			panic(err)
		}

		kvs[k] = string(v)
	}
	return kvs
}

func (meta *IndexBuildMeta210) GetAllBuildIDs() []UniqueID {
	ret := make([]UniqueID, 0, len(*meta))
	for buildID := range *meta {
		ret = append(ret, buildID)
	}
	return ret
}

func (meta *FieldIndexes210) AddRecord(collectionID UniqueID, fieldIndexes []*pb.FieldIndexInfo, schema *schemapb.CollectionSchema) {
	(*meta)[collectionID] = &FieldIndexesWithSchema{indexes: fieldIndexes, schema: schema}
}

func (meta *FieldIndexes210) Merge(other FieldIndexes210) {
	for collectionID := range other {
		meta.AddRecord(collectionID, (other)[collectionID].indexes, (other)[collectionID].schema)
	}
}

func (meta *LastDDLRecords) AddRecord(k, v string) {
	(*meta)[k] = v
}

func (meta *LastDDLRecords) GenerateSaves() map[string]string {
	return *meta
}
