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

// Package metacachetest holds test helpers for metacache.
package metacachetest

import "github.com/milvus-io/milvus/internal/metacache"

// Collections returns the store-owned collection source behind view, for
// tests to fill. It panics if view is not a MetaStore or if its source was
// replaced by SetCollectionSource.
func Collections(view metacache.MetaView) *metacache.Collections {
	return view.(metacache.MetaStore).CollectionSource().(*metacache.Collections)
}

// NewMetaStore returns a MetaStore without a catalog whose collection view
// holds colls.
func NewMetaStore(colls ...*metacache.CollectionInfo) metacache.MetaStore {
	store := metacache.NewMetaStore(nil)
	for _, coll := range colls {
		store.PutCollection(coll)
	}
	return store
}
