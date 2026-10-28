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

package gc

import (
	"cmp"
	"context"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// IndexPresence is what datacoord knows about an index ID of a collection
// that exists.
type IndexPresence int

const (
	// IndexLive is an index in the datacoord meta that is not deleted.
	IndexLive IndexPresence = iota
	// IndexDeleted is an index in the datacoord meta that is deleted.
	IndexDeleted
	// IndexMissing is an index ID that is not in the datacoord meta: the meta
	// GC removed a deleted index, or the CreateIndex callback has not written
	// the index yet.
	IndexMissing
)

// IndexDeps are the datacoord facts that the GC by index ID reads. The
// datacoord shell injects them.
type IndexDeps struct {
	// IndexPresence returns what datacoord knows about the index indexID of
	// a collection that exists.
	IndexPresence func(collectionID, indexID int64) IndexPresence
	// HasCollection reports whether the collection exists. An error skips
	// the round.
	HasCollection func(ctx context.Context, collectionID int64) (bool, error)
	// CollectionPaused reports whether the GC of the collection is paused,
	// for example by a backup.
	CollectionPaused func(collectionID int64) bool
}

// IndexDropper removes the manifest pointers of an index. The manifest
// manager implements it.
type IndexDropper interface {
	DropIndex(ctx context.Context, collectionID, indexID int64, vchannels []string) error
}

// EnableIndexRecovery makes each round also remove the pointers and the
// objects of the indexes that datacoord dropped, see runIndexes. Call it
// before Start.
func (g *GC) EnableIndexRecovery(deps IndexDeps, dropper IndexDropper) {
	g.indexDeps = &deps
	g.dropper = dropper
}

// indexRef names one index of one collection.
type indexRef struct {
	collectionID int64
	indexID      int64
}

// indexState is what one round finds for an index.
type indexState struct {
	vchannels  []string  // the vchannels with a manifest pointer
	newest     time.Time // the newest modification time of its objects
	hasObjects bool
}

// runIndexes removes the pointers and the objects of each index that
// datacoord dropped. It finds the indexes from the manifest pointers in
// pointerKeys and from one recursive walk of all objects of the primary key
// index. Only a recursive listing gives the modification time of an object on
// every object storage, so the walk does not list directories.
//
// The age of an index is the time since the newest modification of its
// objects; an index without objects has an infinite age. The index is
// removed when its age is above the tolerance of its case:
//
//   - its collection does not exist: DropTolerance,
//   - datacoord meta has it deleted: DropTolerance,
//   - datacoord meta does not have it: MissingTolerance, which keeps an
//     index whose CreateIndex callback has not written the meta yet,
//   - datacoord meta has it live: never.
//
// A paused collection is skipped. A HasCollection error skips the round
// before anything is removed. The pointers go first, then the objects, so a
// crash between the two leaves objects without a pointer, which the next
// round removes. Each step is idempotent.
func (g *GC) runIndexes(ctx context.Context, pointerKeys []string) error {
	indexes := map[indexRef]*indexState{}
	stateOf := func(ref indexRef) *indexState {
		st := indexes[ref]
		if st == nil {
			st = &indexState{}
			indexes[ref] = st
		}
		return st
	}
	for _, key := range pointerKeys {
		collectionID, indexID, vchannel, ok := manifest.ParsePointerKey(key)
		if !ok {
			continue
		}
		st := stateOf(indexRef{collectionID, indexID})
		st.vchannels = append(st.vchannels, vchannel)
	}
	root := path.Join(g.rootPath, common.PKIndexPath) + "/"
	err := g.store.Walk(ctx, root, func(key string, mod time.Time) bool {
		ref, ok := parseIndexOfObject(g.rootPath, root, key)
		if !ok {
			return true
		}
		st := stateOf(ref)
		if !st.hasObjects || mod.After(st.newest) {
			st.newest = mod
		}
		st.hasObjects = true
		return true
	})
	if err != nil {
		return markUnavailable(err, "walk the objects of the primary key index")
	}

	refs := make([]indexRef, 0, len(indexes))
	for ref := range indexes {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b indexRef) int {
		return cmp.Or(cmp.Compare(a.collectionID, b.collectionID), cmp.Compare(a.indexID, b.indexID))
	})

	// Decide first, so that a HasCollection error removes nothing.
	exists := map[int64]bool{}
	var drop []indexRef
	now := g.now()
	for _, ref := range refs {
		if g.indexDeps.CollectionPaused(ref.collectionID) {
			continue
		}
		found, ok := exists[ref.collectionID]
		if !ok {
			found, err = g.indexDeps.HasCollection(ctx, ref.collectionID)
			if err != nil {
				return markUnavailable(err, "check collection %d", ref.collectionID)
			}
			exists[ref.collectionID] = found
		}
		tolerance := g.cfg.DropTolerance
		if found {
			switch g.indexDeps.IndexPresence(ref.collectionID, ref.indexID) {
			case IndexLive:
				continue
			case IndexMissing:
				tolerance = g.cfg.MissingTolerance
			}
		}
		st := indexes[ref]
		if st.hasObjects && now.Sub(st.newest) <= tolerance {
			continue
		}
		drop = append(drop, ref)
	}

	for _, ref := range drop {
		if ctx.Err() != nil {
			return nil
		}
		if err := g.dropIndex(ctx, ref, indexes[ref].vchannels); err != nil {
			mlog.Warn(ctx, "pkindex gc of dropped index failed",
				mlog.FieldCollectionID(ref.collectionID), mlog.Int64("indexID", ref.indexID), mlog.Err(err))
		}
	}
	return nil
}

// dropIndex removes the pointers of an index on vchannels, then every object
// of the index.
func (g *GC) dropIndex(ctx context.Context, ref indexRef, vchannels []string) error {
	if len(vchannels) > 0 {
		if err := g.dropper.DropIndex(ctx, ref.collectionID, ref.indexID, vchannels); err != nil {
			return err
		}
	}
	dir := pkindex.IndexDir(g.rootPath, ref.collectionID, ref.indexID) + "/"
	var keys []string
	if err := g.store.Walk(ctx, dir, func(key string, _ time.Time) bool {
		keys = append(keys, key)
		return true
	}); err != nil {
		return markUnavailable(err, "walk the objects of index %d", ref.indexID)
	}
	if len(keys) > 0 {
		if err := g.store.MultiRemove(ctx, keys); err != nil {
			return markUnavailable(err, "remove %d objects of index %d", len(keys), ref.indexID)
		}
	}
	mlog.Info(ctx, "pkindex gc removed a dropped index",
		mlog.FieldCollectionID(ref.collectionID), mlog.Int64("indexID", ref.indexID),
		mlog.Int("pointers", len(vchannels)), mlog.Int("objects", len(keys)))
	return nil
}

// parseIndexOfObject returns the index of an object key under root, the
// primary key index directory under rootPath. The key must be below the
// directory of an index.
func parseIndexOfObject(rootPath, root, key string) (indexRef, bool) {
	rest, ok := strings.CutPrefix(key, root)
	if !ok {
		return indexRef{}, false
	}
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 3 {
		return indexRef{}, false
	}
	collectionID, indexID, ok := pkindex.ParseIndexDir(rootPath, root+parts[0]+"/"+parts[1])
	return indexRef{collectionID, indexID}, ok
}
