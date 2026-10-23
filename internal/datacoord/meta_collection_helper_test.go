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

package datacoord

import (
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metacache/metacachetest"
	"github.com/milvus-io/milvus/internal/metastore"
)

// AddCollection seeds a collection into the store's in-memory
// CollectionSource. Production DataCoord reads collections from RootCoord
// and never writes them.
func (m *meta) AddCollection(collection *collectionInfo) {
	m.metaStore.PutCollection(collection)
}

// DropCollection removes a collection seeded by AddCollection.
func (m *meta) DropCollection(collectionID int64) {
	m.metaStore.RemoveCollection(collectionID)
}

// newMetaWithCollections returns a meta whose only content is colls.
func newMetaWithCollections(colls ...*collectionInfo) *meta {
	m := &meta{metaStore: metacache.NewMetaStore(nil)}
	for _, coll := range colls {
		m.AddCollection(coll)
	}
	return m
}

// seedCollections puts colls into s's collection view, creating the meta
// and store if needed.
func seedCollections(s *Server, colls ...*collectionInfo) *Server {
	if s.meta == nil {
		s.meta = &meta{}
	}
	if s.meta.metaStore == nil {
		s.meta.metaStore = metacache.NewMetaStore(nil)
	}
	for _, coll := range colls {
		s.meta.AddCollection(coll)
	}
	return s
}

// newMetaStoreWithCollections returns a MetaStore over catalog whose
// collection view holds ids, as newMeta would find it after RootCoord loads.
func newMetaStoreWithCollections(catalog metastore.DataCoordCatalog, ids ...int64) metacache.MetaStore {
	store := metacache.NewMetaStore(catalog)
	for _, id := range ids {
		store.PutCollection(&collectionInfo{ID: id})
	}
	return store
}

// newMetaWithDatabases returns a meta whose view holds dbs.
func newMetaWithDatabases(dbs ...*metacache.DatabaseInfo) *meta {
	m := newMetaWithCollections()
	for _, db := range dbs {
		metacachetest.Collections(m.metaStore).PutDatabase(db)
	}
	return m
}
