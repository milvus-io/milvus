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

package metacache

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CollectionSource owns collection metadata; MetaView's collection reads
// forward to it. In MixCoord it is RootCoord's MetaTable. Reads must not
// block on DDL, and returned CollectionInfo values are immutable snapshots.
type CollectionSource interface {
	GetCollection(collectionID int64) (*CollectionInfo, bool)
	GetCollections() []*CollectionInfo
	GetCollectionIDs() []int64
	// GetCollectionByName resolves name in database dbName, aliases first,
	// as RootCoord's describe does. Like describe by name, it finds only
	// Created collections: a Dropping one keeps its name until RootCoord
	// removes it, but the name is free for a new collection. MetaStore maps
	// an empty dbName to the default database before calling it.
	GetCollectionByName(dbName, name string) (*CollectionInfo, bool)
	GetDatabase(dbName string) (*DatabaseInfo, bool)

	// OnCollectionAdded / OnCollectionRemoved register fn, called after a
	// collection has entered or left the source. fn runs outside the
	// source's locks, once per event, and must not block.
	OnCollectionAdded(fn func(collectionID int64))
	OnCollectionRemoved(fn func(collectionID int64))
	// OnCollectionDropping registers fn, called likewise once a Created
	// collection has turned Dropping.
	OnCollectionDropping(fn func(collectionID int64))
	// OnCollectionAltered registers fn, called likewise once an alter has
	// replaced a Created collection's view, with the views before and
	// after.
	OnCollectionAltered(fn func(old, updated *CollectionInfo))
}

// CollectionHooks keeps the callbacks registered through a
// CollectionSource's OnCollectionAdded / OnCollectionRemoved /
// OnCollectionDropping / OnCollectionAltered.
type CollectionHooks struct {
	mu       sync.RWMutex
	added    []func(collectionID int64)
	removed  []func(collectionID int64)
	dropping []func(collectionID int64)
	altered  []func(old, updated *CollectionInfo)
}

func (h *CollectionHooks) OnCollectionAdded(fn func(collectionID int64)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.added = append(h.added, fn)
}

func (h *CollectionHooks) OnCollectionRemoved(fn func(collectionID int64)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removed = append(h.removed, fn)
}

func (h *CollectionHooks) OnCollectionDropping(fn func(collectionID int64)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dropping = append(h.dropping, fn)
}

func (h *CollectionHooks) OnCollectionAltered(fn func(old, updated *CollectionInfo)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.altered = append(h.altered, fn)
}

func (h *CollectionHooks) notify(fns *[]func(collectionID int64), collectionID int64) {
	h.mu.RLock()
	registered := *fns
	h.mu.RUnlock()
	for _, fn := range registered {
		fn(collectionID)
	}
}

// NotifyAdded runs the added callbacks. The caller must not hold the
// source's locks.
func (h *CollectionHooks) NotifyAdded(collectionID int64) {
	h.notify(&h.added, collectionID)
}

// NotifyRemoved runs the removed callbacks. The caller must not hold the
// source's locks.
func (h *CollectionHooks) NotifyRemoved(collectionID int64) {
	h.notify(&h.removed, collectionID)
}

// NotifyDropping runs the dropping callbacks. The caller must not hold the
// source's locks.
func (h *CollectionHooks) NotifyDropping(collectionID int64) {
	h.notify(&h.dropping, collectionID)
}

// NotifyAltered runs the altered callbacks. The caller must not hold the
// source's locks.
func (h *CollectionHooks) NotifyAltered(old, updated *CollectionInfo) {
	h.mu.RLock()
	registered := h.altered
	h.mu.RUnlock()
	for _, fn := range registered {
		fn(old, updated)
	}
}

// Collections is a CollectionSource the MetaStore owns and writes
// itself, through PutCollection / RemoveCollection. A MetaStore uses it
// until SetCollectionSource replaces it, which MixCoord does with RootCoord's
// MetaTable; standalone DataCoord and tests fill it directly.
type Collections struct {
	CollectionHooks
	writeMu     sync.Mutex // serializes Put / Remove so each event fires once
	collections typeutil.ConcurrentMap[int64, *CollectionInfo]
	databases   typeutil.ConcurrentMap[string, *DatabaseInfo]
}

func NewCollections() *Collections {
	return &Collections{}
}

// Put replaces the entry for info.ID.
func (s *Collections) Put(info *CollectionInfo) {
	s.writeMu.Lock()
	old, existed := s.collections.Get(info.ID)
	s.collections.Insert(info.ID, info)
	s.writeMu.Unlock()
	switch {
	case !existed:
		s.NotifyAdded(info.ID)
	case old.IsCreated() && !info.IsCreated():
		s.NotifyDropping(info.ID)
	case old.IsCreated():
		s.NotifyAltered(old, info)
	}
}

func (s *Collections) Remove(collectionID int64) {
	s.writeMu.Lock()
	_, removed := s.collections.GetAndRemove(collectionID)
	s.writeMu.Unlock()
	if removed {
		s.NotifyRemoved(collectionID)
	}
}

func (s *Collections) GetCollection(collectionID int64) (*CollectionInfo, bool) {
	return s.collections.Get(collectionID)
}

func (s *Collections) GetCollections() []*CollectionInfo {
	return s.collections.Values()
}

func (s *Collections) GetCollectionIDs() []int64 {
	return s.collections.Keys()
}

// GetCollectionByName scans the entries by DatabaseName and schema name; it
// knows no aliases.
func (s *Collections) GetCollectionByName(dbName, name string) (*CollectionInfo, bool) {
	var found *CollectionInfo
	s.collections.Range(func(_ int64, info *CollectionInfo) bool {
		if info.IsCreated() && info.DatabaseName == dbName && info.Schema.GetName() == name {
			found = info
			return false
		}
		return true
	})
	return found, found != nil
}

// PutDatabase replaces the entry for info.Name.
func (s *Collections) PutDatabase(info *DatabaseInfo) {
	s.databases.Insert(info.Name, info)
}

func (s *Collections) GetDatabase(dbName string) (*DatabaseInfo, bool) {
	return s.databases.Get(dbName)
}
