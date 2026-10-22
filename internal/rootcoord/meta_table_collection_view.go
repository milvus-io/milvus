// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// collectionSchemaPB builds the schema RootCoord reports for coll, with
// DbName set to dbName: the model's DBName may be out of sync with the
// database resolved from DBID.
func collectionSchemaPB(coll *model.Collection, dbName string) *schemapb.CollectionSchema {
	schema := coll.ToCollectionSchemaPB()
	schema.DbName = dbName
	return schema
}

// collectionDBNameLocked resolves the name of coll's database the way
// describe does, falling back to the model's DBName when the database is
// gone (a Dropping collection outliving DropDatabase). The caller must hold
// ddLock.
func (mt *MetaTable) collectionDBNameLocked(coll *model.Collection) string {
	if db, err := mt.getDatabaseByIDInternal(mt.ctx, normalizeCollectionDBID(coll.DBID), 0); err == nil {
		return db.Name
	}
	if coll.DBName != "" {
		return coll.DBName
	}
	return util.DefaultDBName
}

// buildCollectionViewLocked converts coll into the CollectionInfo published
// to DataCoord and QueryCoord. The caller must hold ddLock.
func (mt *MetaTable) buildCollectionViewLocked(coll *model.Collection) *metacache.CollectionInfo {
	dbName := mt.collectionDBNameLocked(coll)
	// Only Created and Dropping are visible: a legacy Creating collection is
	// a tombstone pending removal, like a Dropping one.
	state := pb.CollectionState_CollectionCreated
	if !coll.Available() {
		state = pb.CollectionState_CollectionDropping
	}
	var partitions, dropping []int64
	idsByName := make(map[string]int64, len(coll.Partitions))
	for _, partition := range coll.Partitions {
		if partition.Available() {
			partitions = append(partitions, partition.PartitionID)
			idsByName[partition.PartitionName] = partition.PartitionID
		} else {
			dropping = append(dropping, partition.PartitionID)
			if _, ok := idsByName[partition.PartitionName]; !ok {
				idsByName[partition.PartitionName] = partition.PartitionID
			}
		}
	}
	// Describe reports one shard per vchannel for legacy collections.
	shardsNum := coll.ShardsNum
	if shardsNum == 0 {
		shardsNum = int32(len(coll.VirtualChannelNames))
	}
	return &metacache.CollectionInfo{
		ID:                 coll.CollectionID,
		Schema:             collectionSchemaPB(coll, dbName),
		State:              state,
		Partitions:         partitions,
		DroppingPartitions: dropping,
		PartitionIDsByName: idsByName,
		VChannelNames:      coll.VirtualChannelNames,
		Properties:         funcutil.KeyValuePair2Map(coll.Properties),
		PropertyPairs:      coll.Properties,
		DatabaseName:       dbName,
		DatabaseID:         coll.DBID,
		StartPositions:     coll.StartPositions,
		CreatedAt:          coll.CreateTime,
		UpdateTimestamp:    coll.UpdateTimestamp,
		ShardsNum:          shardsNum,
		ConsistencyLevel:   coll.ConsistencyLevel,
	}
}

// CollectionSource returns the view of MetaTable that DataCoord and
// QueryCoord read collections from. Its reads never take ddLock.
func (mt *MetaTable) CollectionSource() metacache.CollectionSource {
	return (*collectionSource)(mt)
}

// collectionSource serves MetaTable.collViews, which writers publish under
// ddLock in setCollectionLocked / deleteCollectionLocked.
type collectionSource MetaTable

func (s *collectionSource) GetCollection(collectionID int64) (*metacache.CollectionInfo, bool) {
	return s.collViews.Get(collectionID)
}

func (s *collectionSource) GetCollections() []*metacache.CollectionInfo {
	return s.collViews.Values()
}

func (s *collectionSource) GetCollectionIDs() []int64 {
	return s.collViews.Keys()
}

// GetCollectionByName reads names and aliases, which take their own lock
// rather than ddLock. A rename updates them before it publishes the new
// view, so a view in another database, or under another name, is not
// returned. Databases are compared by ID: the name maps are keyed by the
// model's DBName, which may differ from the name resolved from DBID.
func (s *collectionSource) GetCollectionByName(dbName, name string) (*metacache.CollectionInfo, bool) {
	collectionID, isAlias := s.aliases.get(dbName, name)
	if !isAlias {
		var ok bool
		if collectionID, ok = s.names.get(dbName, name); !ok {
			return nil, false
		}
	}
	view, ok := s.collViews.Get(collectionID)
	db, dbOK := s.dbViews.Get(dbName)
	// Names stay mapped to a Dropping collection until RemoveCollection;
	// describe by name reports it as not found, and so does this.
	if !ok || !dbOK || !view.IsCreated() || normalizeCollectionDBID(view.DatabaseID) != db.ID ||
		(!isAlias && view.Schema.GetName() != name) {
		return nil, false
	}
	return view, true
}

func (s *collectionSource) GetDatabase(dbName string) (*metacache.DatabaseInfo, bool) {
	return s.dbViews.Get(dbName)
}

func (s *collectionSource) OnCollectionAdded(fn func(collectionID int64)) {
	s.collHooks.OnCollectionAdded(fn)
}

func (s *collectionSource) OnCollectionRemoved(fn func(collectionID int64)) {
	s.collHooks.OnCollectionRemoved(fn)
}

func (s *collectionSource) OnCollectionDropping(fn func(collectionID int64)) {
	s.collHooks.OnCollectionDropping(fn)
}

func (s *collectionSource) OnCollectionAltered(fn func(old, updated *metacache.CollectionInfo)) {
	s.collHooks.OnCollectionAltered(fn)
}
