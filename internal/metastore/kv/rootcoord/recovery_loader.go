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
	"bytes"
	"context"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// recoveryScanProgressInterval is the number of keys between two progress logs
// of one recovery prefix scan.
const recoveryScanProgressInterval = 100000

// ListCollectionsForRecovery returns the collections of every database in
// dbIDs, keyed by database ID, as ListCollections returns them for each
// database. util.NonDBID selects the collections stored before databases were
// introduced; reading them migrates them to the default database.
//
// The legacy collection prefix, the database collection prefix and the
// partition, field, struct array field and function prefixes are each scanned
// once, and child metadata is joined to its collection by collection ID. This
// replaces one read per database plus four reads per collection.
func (kc *Catalog) ListCollectionsForRecovery(
	ctx context.Context,
	dbIDs []int64,
	ts typeutil.Timestamp,
) (map[int64][]*model.Collection, error) {
	walker, ok := kc.Txn.(kv.MetaKv)
	if !ok {
		return nil, merr.Wrapf(merr.ErrServiceUnimplemented, "metadata store does not support paginated collection recovery scans")
	}

	start := time.Now()
	collectionsByDB := make(map[int64][]*model.Collection, len(dbIDs))
	for _, dbID := range dbIDs {
		collectionsByDB[dbID] = make([]*model.Collection, 0)
	}
	// Collections whose partitions and fields are stored under the child
	// prefixes, by collection ID. A legacy collection migrated during this
	// recovery is returned for both util.NonDBID and the default database; the
	// second collection with the same ID is kept in duplicates.
	targets := make(map[int64]*model.Collection)
	duplicates := make(map[int64][]*model.Collection)
	numCollections := 0
	addCollection := func(dbID int64, value []byte) error {
		collection, childMetaStoredSeparately, err := kc.decodeCollection(ctx, value, ts)
		if err != nil {
			return err
		}
		if childMetaStoredSeparately {
			collection.Partitions = make([]*model.Partition, 0)
			collection.Fields = make([]*model.Field, 0)
			collection.StructArrayFields = make([]*model.StructArrayField, 0)
			collection.Functions = make([]*model.Function, 0)
			if _, ok := targets[collection.CollectionID]; ok {
				duplicates[collection.CollectionID] = append(duplicates[collection.CollectionID], collection)
			} else {
				targets[collection.CollectionID] = collection
			}
		}
		collectionsByDB[dbID] = append(collectionsByDB[dbID], collection)
		numCollections++
		return nil
	}

	// The legacy prefix is scanned first: reading it migrates its collections
	// to the default database, and the database prefix scan returns them after
	// that, as ListCollections of the default database does.
	if _, ok := collectionsByDB[util.NonDBID]; ok {
		err := walkRecoveryPrefix(ctx, walker, CollectionMetaPrefix+"/", func(_, value []byte) error {
			return addCollection(util.NonDBID, value)
		})
		if err != nil {
			return nil, err
		}
	}
	err := walkRecoveryPrefixByID(ctx, walker, CollectionInfoMetaPrefix+"/", func(dbID int64, value []byte) error {
		if _, ok := collectionsByDB[dbID]; !ok || dbID == util.NonDBID {
			return nil
		}
		return addCollection(dbID, value)
	})
	if err != nil {
		return nil, err
	}
	if len(targets) > 0 {
		if err := attachRecoveryChildMetadata(ctx, walker, targets, duplicates); err != nil {
			return nil, err
		}
	}

	mlog.Info(ctx, "rootcoord collection metadata recovered with prefix scans",
		mlog.Int("numDatabases", len(collectionsByDB)),
		mlog.Int("numCollections", numCollections),
		mlog.Int("numCollectionsWithChildKeys", len(targets)),
		mlog.Duration("duration", time.Since(start)))
	return collectionsByDB, nil
}

// attachRecoveryChildMetadata scans the partition, field, struct array field
// and function prefixes concurrently and appends every entry to the collection
// in targets with its collection ID. Each scan fills a different field of the
// collections.
func attachRecoveryChildMetadata(
	ctx context.Context,
	walker kv.MetaKv,
	targets map[int64]*model.Collection,
	duplicates map[int64][]*model.Collection,
) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return scanRecoveryChildren(gCtx, walker, PartitionMetaPrefix+"/", targets, duplicates,
			unmarshalPartition, (*model.Partition).Clone,
			func(c *model.Collection) *[]*model.Partition { return &c.Partitions })
	})
	g.Go(func() error {
		return scanRecoveryChildren(gCtx, walker, FieldMetaPrefix+"/", targets, duplicates,
			unmarshalField, (*model.Field).Clone,
			func(c *model.Collection) *[]*model.Field { return &c.Fields })
	})
	g.Go(func() error {
		return scanRecoveryChildren(gCtx, walker, StructArrayFieldMetaPrefix+"/", targets, duplicates,
			unmarshalStructArrayField, (*model.StructArrayField).Clone,
			func(c *model.Collection) *[]*model.StructArrayField { return &c.StructArrayFields })
	})
	g.Go(func() error {
		return scanRecoveryChildren(gCtx, walker, FunctionMetaPrefix+"/", targets, duplicates,
			unmarshalFunction, (*model.Function).Clone,
			func(c *model.Collection) *[]*model.Function { return &c.Functions })
	})
	return g.Wait()
}

// scanRecoveryChildren scans prefix, whose keys have the form
// <prefix><collectionID>/<childID>, and appends every entry of a collection in
// targets to the slot of that collection. A collection in duplicates gets a
// clone, so collections never share child models. Entries of other collections
// are skipped without being decoded.
func scanRecoveryChildren[T any](
	ctx context.Context,
	walker kv.MetaKv,
	prefix string,
	targets map[int64]*model.Collection,
	duplicates map[int64][]*model.Collection,
	decode func(value []byte) (T, error),
	clone func(T) T,
	slot func(*model.Collection) *[]T,
) error {
	return walkRecoveryPrefixByID(ctx, walker, prefix, func(collectionID int64, value []byte) error {
		target, ok := targets[collectionID]
		if !ok {
			return nil
		}
		child, err := decode(value)
		if err != nil {
			return err
		}
		*slot(target) = append(*slot(target), child)
		for _, duplicate := range duplicates[collectionID] {
			*slot(duplicate) = append(*slot(duplicate), clone(child))
		}
		return nil
	})
}

// walkRecoveryPrefixByID scans prefix, whose keys have the form
// <prefix><id>/<rest>, and calls fn with the id of every entry. Other keys are
// skipped, as a read of <prefix><id>/ would not return them either.
func walkRecoveryPrefixByID(
	ctx context.Context,
	walker kv.MetaKv,
	prefix string,
	fn func(id int64, value []byte) error,
) error {
	// WalkWithPrefix passes full keys, in the form GetPath returns.
	fullPrefix := []byte(walker.GetPath(prefix))
	return walkRecoveryPrefix(ctx, walker, prefix, func(key, value []byte) error {
		id, ok := parseRecoveryKeyID(fullPrefix, key)
		if !ok {
			mlog.RatedWarn(ctx, 1.0, "skip metadata key without an identifier during rootcoord recovery",
				mlog.String("key", string(key)))
			return nil
		}
		return fn(id, value)
	})
}

// walkRecoveryPrefix calls fn for every entry under prefix that is not a
// legacy tombstone, reading metastore.paginationSize keys per request.
func walkRecoveryPrefix(
	ctx context.Context,
	walker kv.MetaKv,
	prefix string,
	fn func(key, value []byte) error,
) error {
	pageSize := paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt()
	start := time.Now()
	scanned := 0
	err := walker.WalkWithPrefix(ctx, prefix, pageSize, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		scanned++
		if scanned%recoveryScanProgressInterval == 0 {
			mlog.Info(ctx, "rootcoord recovery prefix scan progress",
				mlog.String("prefix", prefix),
				mlog.Int("numScanned", scanned),
				mlog.Duration("duration", time.Since(start)))
		}
		if bytes.Equal(value, SuffixSnapshotTombstone) {
			return nil
		}
		return fn(key, value)
	})
	if err != nil {
		return merr.Wrapf(err, "scan rootcoord metadata prefix %s", prefix)
	}
	mlog.Info(ctx, "rootcoord recovery prefix scan done",
		mlog.String("prefix", prefix),
		mlog.Int("numScanned", scanned),
		mlog.Int("pageSize", pageSize),
		mlog.Duration("duration", time.Since(start)))
	return nil
}

// parseRecoveryKeyID returns the id of a key of the form <fullPrefix><id>/<rest>.
// The id must be written as fmt %d writes it, which is how the per-id prefixes
// read by ListCollections are built.
func parseRecoveryKeyID(fullPrefix []byte, key []byte) (int64, bool) {
	rest, ok := bytes.CutPrefix(key, fullPrefix)
	if !ok {
		return 0, false
	}
	end := bytes.IndexByte(rest, '/')
	if end < 0 {
		return 0, false
	}
	idText := rest[:end]
	id, err := strconv.ParseInt(string(idText), 10, 64)
	if err != nil {
		return 0, false
	}
	var canonical [20]byte
	if !bytes.Equal(strconv.AppendInt(canonical[:0], id, 10), idText) {
		return 0, false
	}
	return id, true
}
