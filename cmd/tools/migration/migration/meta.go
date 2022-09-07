package migration

import (
	"fmt"

	"github.com/blang/semver/v4"

	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

// for these time-travel related meta, nil record represents tombstone for simplicity.
type ttCollectionsMeta map[UniqueID]map[Timestamp]*model.Collection
type ttPartitionsMeta map[UniqueID]map[Timestamp]*model.Partition

// collectionID -> ts -> fields
type ttFieldsMeta map[UniqueID]map[Timestamp][]*model.Field
type ttAliasesMeta map[string]map[Timestamp]*model.Alias

// serve for acceleration of snapshot kv,
// you cannot ignore them for now, since snapshotkv.load will always read latest directly.
type collectionsMeta map[UniqueID]*model.Collection
type partitionsMeta map[UniqueID]*model.Partition
type fieldsMeta map[UniqueID][]*model.Field
type aliasesMeta map[string]*model.Alias

// collID -> indexID -> index
type collectionIndexesMeta map[UniqueID]map[UniqueID]*model.Index

// segmentIndexes records which indexes are on the segment
// segID -> indexID -> segmentIndex
type segmentIndexesMeta map[UniqueID]map[UniqueID]*model.SegmentIndex

func (meta *ttAliasesMeta) addAlias(alias string, aliasInfo *model.Alias, ts Timestamp) {
	_, aliasExist := (*meta)[alias]
	if aliasExist {
		(*meta)[alias][ts] = aliasInfo
	} else {
		(*meta)[alias] = map[Timestamp]*model.Alias{
			ts: aliasInfo,
		}
	}
}

func (meta *ttAliasesMeta) merge(other ttAliasesMeta) {
	for alias := range other {
		for ts := range (other)[alias] {
			aliasInfo := (other)[alias][ts]
			meta.addAlias(alias, aliasInfo, ts)
		}
	}
}

func (meta *ttAliasesMeta) show() {
	fmt.Println("===================================== tt aliases ======================================")
	for alias := range *meta {
		for ts := range (*meta)[alias] {
			aliasInfo := (*meta)[alias][ts]
			if aliasInfo == nil {
				fmt.Printf("alias: %s, ts: %d, it's a tombstone\n", alias, ts)
			} else {
				fmt.Printf("alias: %s, ts: %d, name: %s, collection: %d, created at: %v, state: %s\n", alias, ts, aliasInfo.Name, aliasInfo.CollectionID, aliasInfo.CreatedTime, aliasInfo.State)
			}
		}
	}
	fmt.Println("===================================== tt aliases ======================================")
}

func (meta *aliasesMeta) addAlias(alias string, aliasInfo *model.Alias) {
	(*meta)[alias] = aliasInfo
}

func (meta *aliasesMeta) merge(other aliasesMeta) {
	for alias := range other {
		aliasInfo := (other)[alias]
		meta.addAlias(alias, aliasInfo)
	}
}

func (meta *aliasesMeta) show() {
	fmt.Println("===================================== aliases ======================================")
	for alias := range *meta {
		aliasInfo := (*meta)[alias]
		if aliasInfo == nil {
			fmt.Printf("alias: %s, it's a tombstone\n", alias)
		} else {
			fmt.Printf("alias: %s, name: %s, collection: %d, created at: %v, state: %s\n", alias, aliasInfo.Name, aliasInfo.CollectionID, aliasInfo.CreatedTime, aliasInfo.State)
		}
	}
	fmt.Println("===================================== aliases ======================================")
}

func (meta *ttCollectionsMeta) addCollection(collectionID UniqueID, collectionInfo *model.Collection, ts Timestamp) {
	_, collExist := (*meta)[collectionID]
	if collExist {
		(*meta)[collectionID][ts] = collectionInfo
	} else {
		(*meta)[collectionID] = map[Timestamp]*model.Collection{
			ts: collectionInfo,
		}
	}
}

func (meta *ttCollectionsMeta) merge(other ttCollectionsMeta) {
	for collectionID := range other {
		for ts := range (other)[collectionID] {
			collectionInfo := (other)[collectionID][ts]
			meta.addCollection(collectionID, collectionInfo, ts)
		}
	}
}

func (meta *ttCollectionsMeta) show() {
	fmt.Println("===================================== tt collections ======================================")
	for collectionID := range *meta {
		for ts := range (*meta)[collectionID] {
			collectionInfo := (*meta)[collectionID][ts]
			if collectionInfo == nil {
				fmt.Printf("collection: %d, ts: %d, it's a tombstone\n", collectionID, ts)
			} else {
				fmt.Printf("collectionID: %d, ts: %d, collection: %v\n", collectionID, ts, collectionInfo)
			}
		}
	}
	fmt.Println("===================================== tt collections ======================================")
}

func (meta *collectionsMeta) addCollection(collectionID UniqueID, collectionInfo *model.Collection) {
	(*meta)[collectionID] = collectionInfo
}

func (meta *collectionsMeta) merge(other collectionsMeta) {
	for collectionID := range other {
		collectionInfo := (other)[collectionID]
		meta.addCollection(collectionID, collectionInfo)
	}
}

func (meta *collectionsMeta) show() {
	fmt.Println("===================================== collections ======================================")
	for collectionID := range *meta {
		collectionInfo := (*meta)[collectionID]
		if collectionInfo == nil {
			fmt.Printf("collection: %d, it's a tombstone\n", collectionID)
		} else {
			fmt.Printf("collectionID: %d, collection: %v\n", collectionID, collectionInfo)
		}
	}
	fmt.Println("===================================== collections ======================================")
}

func (meta *collectionIndexesMeta) addRecord(collectionID, indexID UniqueID, index *model.Index) {
	_, collExist := (*meta)[collectionID]
	if !collExist {
		(*meta)[collectionID] = map[UniqueID]*model.Index{
			indexID: index,
		}
	} else {
		(*meta)[collectionID][indexID] = index
	}
}

func (meta *collectionIndexesMeta) merge(other collectionIndexesMeta) {
	for collectionID := range other {
		for indexID := range other[collectionID] {
			meta.addRecord(collectionID, indexID, other[collectionID][indexID])
		}
	}
}

func printIndexModel(index *model.Index) {
	if index == nil {
		return
	}
	fmt.Printf("TenantID: %s, CollectionID: %d, FieldID: %d, IndexID: %d, IndexName: %s, IsDeleted: %v, CreateTime: %v, TypeParams: %v, IndexParams: %v\n",
		index.TenantID, index.CollectionID, index.FieldID, index.IndexID, index.IndexName, index.IsDeleted, index.CreateTime, index.TypeParams, index.IndexParams)
}

func (meta *collectionIndexesMeta) show() {
	fmt.Println("===================================== collection indexes ======================================")
	for collectionID := range *meta {
		for indexID := range (*meta)[collectionID] {
			record := (*meta)[collectionID][indexID]
			printIndexModel(record)
		}
	}
	fmt.Println("===================================== collection indexes ======================================")
}

func (meta *segmentIndexesMeta) addRecord(pbSegmentIndex *pb.SegmentIndexInfo) {
	_, collExist := (*meta)[pbSegmentIndex.GetCollectionID()]
	if !collExist {
		(*meta)[pbSegmentIndex.GetCollectionID()] = map[UniqueID]*model.SegmentIndex{
			pbSegmentIndex.GetIndexID(): {
				SegmentID:      pbSegmentIndex.GetSegmentID(),
				CollectionID:   pbSegmentIndex.GetCollectionID(),
				PartitionID:    pbSegmentIndex.GetPartitionID(),
				NumRows:        pbSegmentIndex.SegmentID,
				IndexID:        pbSegmentIndex.GetIndexID(),
				BuildID:        pbSegmentIndex.GetBuildID(),
				NodeID:         0,     //todo: how?
				IndexVersion:   0,     //todo: how?
				IndexState:     0,     //todo: how?
				FailReason:     "",    //todo: how?
				IsDeleted:      false, //todo: how?
				CreateTime:     pbSegmentIndex.CreateTime,
				IndexFilePaths: nil, //todo: how?
				IndexSize:      0,   //todo: how?
			},
		}
	} else {
		(*meta)[pbSegmentIndex.GetCollectionID()][pbSegmentIndex.GetIndexID()] = &model.SegmentIndex{
			SegmentID:      pbSegmentIndex.GetSegmentID(),
			CollectionID:   pbSegmentIndex.GetCollectionID(),
			PartitionID:    pbSegmentIndex.GetPartitionID(),
			NumRows:        pbSegmentIndex.SegmentID,
			IndexID:        pbSegmentIndex.GetIndexID(),
			BuildID:        pbSegmentIndex.GetBuildID(),
			NodeID:         0,     //todo: how?
			IndexVersion:   0,     //todo: how?
			IndexState:     0,     //todo: how?
			FailReason:     "",    //todo: how?
			IsDeleted:      false, //todo: how?
			CreateTime:     pbSegmentIndex.CreateTime,
			IndexFilePaths: nil, //todo: how?
			IndexSize:      0,   //todo: how?
		}
	}
}

func printSegmentIndex(m *model.SegmentIndex) {
	if m == nil {
		return
	}
	fmt.Printf("SegmentID: %v, CollectionID: %v, PartitionID: %v, NumRows: %v, IndexID: %v, BuildID: %v, NodeID: %v, IndexVersion: %v, IndexState: %v, FailReason: %v, IsDeleted: %v, CreateTime: %v, IndexFilePaths: %v, IndexSize: %v\n",
		m.SegmentID, m.CollectionID, m.PartitionID, m.NumRows, m.IndexID, m.BuildID, m.NodeID, m.IndexVersion, m.IndexState, m.FailReason, m.IsDeleted, m.CreateTime, m.IndexFilePaths, m.IndexSize)
}

func (meta *segmentIndexesMeta) show() {
	fmt.Println("===================================== segment indexes ======================================")
	for collectionID := range *meta {
		for indexID := range (*meta)[collectionID] {
			record := (*meta)[collectionID][indexID]
			printSegmentIndex(record)
		}
	}
	fmt.Println("===================================== segment indexes ======================================")
}

func (meta *ttPartitionsMeta) show() {
	fmt.Println("===================================== tt partitions ======================================")
	for partitionID := range *meta {
		for ts := range (*meta)[partitionID] {
			partition := (*meta)[partitionID][ts]
			if partition == nil {
				fmt.Printf("partition: %d, ts: %d, it's a tombstone\n", partitionID, ts)
			} else {
				fmt.Printf("partition: %d, ts: %d, record: %v\n", partitionID, ts, partition)
			}
		}
	}
	fmt.Println("===================================== tt partitions ======================================")
}

func (meta *partitionsMeta) show() {
	fmt.Println("===================================== partitions ======================================")
	for partitionID := range *meta {
		partition := (*meta)[partitionID]
		if partition == nil {
			fmt.Printf("partition: %d, it's a tombstone\n", partitionID)
		} else {
			fmt.Printf("partition: %d, record: %v\n", partitionID, partition)
		}
	}
	fmt.Println("===================================== partitions ======================================")
}

func (meta *ttFieldsMeta) show() {
	fmt.Println("===================================== tt fields ======================================")
	for collectionID := range *meta {
		for ts := range (*meta)[collectionID] {
			fields := (*meta)[collectionID][ts]
			if fields == nil {
				fmt.Printf("collectionID: %d, ts: %d, it's a tombstone\n", collectionID, ts)
			} else {
				fmt.Printf("collectionID: %d, ts: %d, record: %v\n", collectionID, ts, fields)
			}
		}
	}
	fmt.Println("===================================== tt fields ======================================")
}

func (meta *fieldsMeta) show() {
	fmt.Println("===================================== fields ======================================")
	for collectionID := range *meta {
		fields := (*meta)[collectionID]
		if fields == nil {
			fmt.Printf("collectionID: %d, it's a tombstone\n", collectionID)
		} else {
			fmt.Printf("collectionID: %d, record: %v\n", collectionID, fields)
		}
	}
	fmt.Println("===================================== fields ======================================")
}

type Meta struct {
	ttCollections ttCollectionsMeta
	ttPartitions  ttPartitionsMeta
	ttFields      ttFieldsMeta
	ttAliases     ttAliasesMeta

	collections collectionsMeta
	partitions  partitionsMeta
	fields      fieldsMeta
	aliases     aliasesMeta

	collectionIndexes collectionIndexesMeta
	segmentIndexes    segmentIndexesMeta

	version semver.Version
}

func (meta *Meta) Show() {
	meta.ttAliases.show()
	meta.aliases.show()

	meta.ttCollections.show()
	meta.collections.show()

	meta.ttPartitions.show()
	meta.partitions.show()

	meta.ttFields.show()
	meta.fields.show()

	meta.collectionIndexes.show()

	meta.segmentIndexes.show()
}
