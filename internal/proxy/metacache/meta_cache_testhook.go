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

//go:build test

package metacache

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type StateSnapshotForTest struct {
	Collections        map[typeutil.UniqueID]*CollectionInfo
	NameIdx            map[string]map[string]typeutil.UniqueID
	AliasInfo          map[string]map[string]string
	DBInfo             map[string]*DatabaseInfo
	PartitionCacheKeys []string
}

func (m *MetaCache) SnapshotForTest() StateSnapshotForTest {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := StateSnapshotForTest{
		Collections:        make(map[typeutil.UniqueID]*CollectionInfo, len(m.collections)),
		NameIdx:            copyNestedIDMap(m.nameIdx),
		AliasInfo:          copyNestedStringMap(m.aliasInfo),
		DBInfo:             make(map[string]*DatabaseInfo, len(m.dbInfo)),
		PartitionCacheKeys: make([]string, 0, len(m.partitionCache)),
	}
	for id, info := range m.collections {
		snapshot.Collections[id] = info
	}
	for db, info := range m.dbInfo {
		snapshot.DBInfo[db] = info
	}
	for key := range m.partitionCache {
		snapshot.PartitionCacheKeys = append(snapshot.PartitionCacheKeys, key)
	}
	slices.Sort(snapshot.PartitionCacheKeys)
	return snapshot
}

func copyNestedIDMap(in map[string]map[string]typeutil.UniqueID) map[string]map[string]typeutil.UniqueID {
	out := make(map[string]map[string]typeutil.UniqueID, len(in))
	for db, values := range in {
		out[db] = make(map[string]typeutil.UniqueID, len(values))
		for name, id := range values {
			out[db][name] = id
		}
	}
	return out
}

func copyNestedStringMap(in map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(in))
	for db, values := range in {
		out[db] = make(map[string]string, len(values))
		for name, target := range values {
			out[db][name] = target
		}
	}
	return out
}

func (m *MetaCache) SetAliasLockedForTest(database, alias, realName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setAliasLocked(database, alias, realName)
}

func (m *MetaCache) GetCollectionForTest(database, collectionName string, collectionID typeutil.UniqueID) (*CollectionInfo, bool) {
	return m.getCollection(database, collectionName, collectionID)
}

func (m *MetaCache) LiveLockedForTest(collectionID typeutil.UniqueID) (*CollectionInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.liveLocked(collectionID)
}

func (m *MetaCache) SafeGetDBInfoForTest(database string) *DatabaseInfo {
	return m.safeGetDBInfo(database)
}

func (m *MetaCache) SeedDBInfoForTest(database string, info *DatabaseInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dbInfo == nil {
		m.dbInfo = map[string]*DatabaseInfo{}
	}
	m.dbInfo[database] = info
}

func (m *MetaCache) SeedCollectionForTest(db, name string, id typeutil.UniqueID, aliases ...string) *CollectionInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.collections == nil {
		m.collections = map[typeutil.UniqueID]*CollectionInfo{}
	}
	if m.nameIdx == nil {
		m.nameIdx = map[string]map[string]typeutil.UniqueID{}
	}
	if m.aliasInfo == nil {
		m.aliasInfo = map[string]map[string]string{}
	}
	info := &CollectionInfo{
		CollID:  id,
		DBName:  db,
		Schema:  mustNewSchemaInfoForTest(name),
		Aliases: slices.Clone(aliases),
	}
	m.collections[id] = info
	db = normalizeDBName(db)
	if _, ok := m.nameIdx[db]; !ok {
		m.nameIdx[db] = map[string]typeutil.UniqueID{}
	}
	m.nameIdx[db][name] = id
	for _, alias := range aliases {
		m.setAliasLocked(db, alias, name)
	}
	return info
}

func (m *MetaCache) SetCollectionAliasesForTest(collectionID typeutil.UniqueID, aliases ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info, ok := m.collections[collectionID]; ok {
		info.Aliases = slices.Clone(aliases)
	}
}

func mustNewSchemaInfoForTest(name string) *SchemaInfo {
	info, err := NewSchemaInfo(newCollectionSchemaForTest(name))
	if err != nil {
		panic(err)
	}
	return info
}

func newCollectionSchemaForTest(name string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Name: name}
}

func (m *MetaCache) SeedPartitionCacheForTest(collectionID typeutil.UniqueID, infos []*PartitionInfo, hasPartitionKey bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.partitionCache == nil {
		m.partitionCache = map[string]*PartitionInfos{}
	}
	m.partitionCache[buildPartitionCacheKey(collectionID)] = parsePartitionsInfo(infos, hasPartitionKey)
}

func (m *MetaCache) DeleteCollectionForTest(collectionID typeutil.UniqueID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collections, collectionID)
}

func (m *MetaCache) DeleteCollectionAndNameHintForTest(database, name string, collectionID typeutil.UniqueID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collections, collectionID)
	if ids, ok := m.nameIdx[normalizeDBName(database)]; ok {
		delete(ids, name)
	}
}

func (m *MetaCache) DeleteNameHintForTest(database, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ids, ok := m.nameIdx[normalizeDBName(database)]; ok {
		delete(ids, name)
	}
}

func (m *MetaCache) AliasTargetForTest(database, alias string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if db, ok := m.aliasInfo[normalizeDBName(database)]; ok {
		target, ok := db[alias]
		return target, ok
	}
	return "", false
}

func (m *MetaCache) HasNameHintForTest(database, name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if db, ok := m.nameIdx[normalizeDBName(database)]; ok {
		_, ok := db[name]
		return ok
	}
	return false
}

func (m *MetaCache) NameHintForTest(database, name string) (typeutil.UniqueID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if db, ok := m.nameIdx[normalizeDBName(database)]; ok {
		id, ok := db[name]
		return id, ok
	}
	return 0, false
}

func (m *MetaCache) CollectionCountForTest() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.collections)
}

func (m *MetaCache) HasPartitionCacheForTest(collectionID typeutil.UniqueID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.partitionCache[buildPartitionCacheKey(collectionID)]
	return ok
}

func (m *MetaCache) HasDBInfoForTest(database string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.dbInfo[database]
	return ok
}

func (m *MetaCache) SetRemoveDatabaseMidWindowHookForTest(h func()) func() {
	m.testHookRemoveDatabaseMidWindow = h
	return func() { m.testHookRemoveDatabaseMidWindow = nil }
}

func (m *MetaCache) SetBeforeSingleflightReturnHookForTest(h func()) func() {
	m.testHookBeforeSingleflightReturn = h
	return func() { m.testHookBeforeSingleflightReturn = nil }
}

func (m *MetaCache) SetInvalidateCollectionMetaMidMutationHookForTest(h func()) func() {
	m.testHookInvalidateCollectionMetaMidMutation = h
	return func() { m.testHookInvalidateCollectionMetaMidMutation = nil }
}
