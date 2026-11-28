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
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file implements Watch of metastore.GenericCatalog over the catalog's
// MetaKv; Get and List live in model.go, Update in update.go. The MetaKv stands in for the
// catalog server until it exists: its root path is the cluster namespace, so
// every key here is relative.
//
// Snapshot pages and watches need backend support that kv.MetaKv does not
// declare. The etcd MetaKv provides them through the optional interfaces
// below. On any other MetaKv (TiKV, wrappers, test fakes) List walks without
// a snapshot, pages by skipping to the cursor, and returns revision 0, and
// Watch reports ErrServiceUnimplemented.

// pageScanner is an optional MetaKv capability: one page of the keys under a
// prefix, after a cursor key, at a revision (0: the latest). Keys passed to
// fn are relative to the root path. It returns the revision read and whether
// more keys follow. A compacted revision fails with an error that errors.Is
// matches to kv.ErrCompacted.
type pageScanner interface {
	WalkPrefixPage(ctx context.Context, prefix, after string, limit int, revision int64,
		fn func(key, value []byte) error) (int64, bool, error)
}

// revisionLoader is an optional MetaKv capability: a load that also returns
// the store revision it read at. Without it Get returns revision 0.
type revisionLoader interface {
	LoadWithRevision(ctx context.Context, key string) (string, int64, error)
}

// maxListAttempts bounds how often an unpinned List of every match restarts
// a snapshot the backend compacted before its last page.
const maxListAttempts = 3

// prefixWatcher is an optional MetaKv capability: a prefix watch from a
// revision, ending when ctx is done. Event keys are absolute.
type prefixWatcher interface {
	WatchWithPrefixFromRevision(ctx context.Context, prefix string, revision int64) clientv3.WatchChan
}

var _ metastore.DataCoordCatalog = (*Catalog)(nil)

// rootPrefix returns the prefix MetaKv puts before every key under root, ""
// for no root. Like util.GetPath and the etcd KV's trimRootPath, it only
// tolerates a trailing "/", so a configured root such as "by-dev/meta/"
// still matches.
func rootPrefix(root string) string {
	if root == "" {
		return ""
	}
	return strings.TrimSuffix(root, "/") + "/"
}

// Watch calls fn with decoded changes to the records of type t under the
// prefix coordinate. The etcd client re-establishes a broken watch stream from the
// last delivered revision by itself, so a transient failure causes neither a
// gap nor a duplicate here.
func (kc *Catalog) Watch(ctx context.Context, t *metastore.ModelType, coordinate []string, opts metastore.WatchOptions, fn func(metastore.EventBatch) error) error {
	if err := checkType(t); err != nil {
		return err
	}
	keyPrefix, err := prefixKey(t, coordinate, "watch")
	if err != nil {
		return err
	}
	watcher, ok := kc.MetaKv.(prefixWatcher)
	if !ok {
		return merr.Wrap(merr.ErrServiceUnimplemented, "meta kv backend does not support watch")
	}
	start := int64(0)
	if opts.AfterRevision > 0 {
		start = int64(opts.AfterRevision) + 1
	}
	// Canceling watchCtx on return ends the etcd watch.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	records := watchedRecords{root: kc.metaRootpath, prefix: rootPrefix(kc.metaRootpath), t: t}
	return consumeWatch(watchCtx, watcher.WatchWithPrefixFromRevision(watchCtx, keyPrefix, start), records, fn)
}

// watchedRecords turns watch event keys and values into records of the
// watched model type.
type watchedRecords struct {
	root   string // the catalog's meta root path, for error messages
	prefix string // rootPrefix(root), stripped from every event key
	t      *metastore.ModelType
}

// consumeWatch decodes the responses of an etcd watch channel and passes
// them to fn, one batch per revision, until ctx is done (nil), fn fails, or
// the watch fails.
func consumeWatch(ctx context.Context, wch clientv3.WatchChan, records watchedRecords,
	fn func(metastore.EventBatch) error,
) error {
	for {
		var resp clientv3.WatchResponse
		var ok bool
		select {
		case <-ctx.Done():
			return nil
		case resp, ok = <-wch:
		}
		switch {
		case ctx.Err() != nil:
			return nil
		case !ok:
			return merr.WrapErrServiceUnavailableMsg("catalog watch channel closed")
		case resp.CompactRevision != 0:
			return errors.Mark(merr.WrapErrIoFailedReason(fmt.Sprintf(
				"catalog watch: revision compacted, oldest retained revision is %d", resp.CompactRevision)),
				kv.ErrCompacted)
		case resp.Err() != nil:
			return merr.Wrap(resp.Err(), "catalog watch")
		}
		// Fail closed: a record the watcher cannot decode must not be skipped,
		// or the watcher's view silently diverges from the store.
		batches, err := groupByRevision(resp.Events, records)
		if err != nil {
			return err
		}
		for _, batch := range batches {
			if err := fn(batch); err != nil {
				return err
			}
		}
	}
}

// groupByRevision decodes one etcd watch response into one batch per
// revision. All events of one etcd txn share a revision, so an atomic Update
// becomes one batch.
func groupByRevision(events []*clientv3.Event, records watchedRecords) ([]metastore.EventBatch, error) {
	var batches []metastore.EventBatch
	for _, ev := range events {
		rev := metastore.Revision(ev.Kv.ModRevision)
		if len(batches) == 0 || batches[len(batches)-1].Revision != rev {
			batches = append(batches, metastore.EventBatch{Revision: rev})
		}
		key := strings.TrimPrefix(string(ev.Kv.Key), records.prefix)
		coord, err := recordCoordinateOfKey(records.root, records.t, key)
		if err != nil {
			return nil, err
		}
		event := metastore.Event{Type: metastore.EventDelete, Coordinate: coord}
		if ev.Type != mvccpb.DELETE {
			v, err := decodeRecord(records.root, records.t, key, ev.Kv.Value)
			if err != nil {
				return nil, err
			}
			event.Type, event.Value = metastore.EventPut, v
		}
		last := &batches[len(batches)-1]
		last.Events = append(last.Events, event)
	}
	return batches, nil
}
