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
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	pkindexgc "github.com/milvus-io/milvus/internal/datacoord/pkindex/gc"
	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// metaKVStore is the primary key index meta store on the datacoord meta kv.
// Its keys are relative to the root path of the meta kv.
type metaKVStore struct{ kv kv.MetaKv }

var _ manifest.MetaStore = metaKVStore{}

func (p metaKVStore) Load(ctx context.Context, key string) (string, bool, error) {
	v, err := p.kv.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// MultiLoad reads keys in meta kv calls of at most MaxTxnOps keys each. A key
// is missing when the meta kv returns an empty value for it; a stored pointer
// or pchannel meta is never empty.
func (p metaKVStore) MultiLoad(ctx context.Context, keys []string) ([]string, []bool, error) {
	limit := p.kv.MaxTxnOps()
	if limit <= 0 {
		return nil, nil, merr.WrapErrServiceInternalMsg("meta kv txn limit must be positive, got %d", limit)
	}
	values := make([]string, 0, len(keys))
	for batch := range slices.Chunk(keys, limit) {
		got, err := p.kv.MultiLoad(ctx, batch)
		if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
			return nil, nil, err
		}
		if len(got) != len(batch) {
			return nil, nil, merr.WrapErrServiceInternal(fmt.Sprintf("meta kv returned %d values for %d keys", len(got), len(batch)))
		}
		values = append(values, got...)
	}
	found := make([]bool, len(keys))
	for i, v := range values {
		found[i] = v != ""
	}
	return values, found, nil
}

// Save writes saves in one meta kv transaction under preds.
func (p metaKVStore) Save(ctx context.Context, saves map[string]string, preds ...predicates.Predicate) error {
	return p.kv.MultiSaveAndRemove(ctx, saves, nil, preds...)
}

// List returns the keys under prefix without the root path of the meta kv,
// which LoadWithPrefix includes.
func (p metaKVStore) List(ctx context.Context, prefix string) ([]string, []string, error) {
	keys, values, err := p.kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	root := strings.TrimSuffix(p.kv.GetPath(prefix), prefix)
	for i := range keys {
		keys[i] = strings.TrimPrefix(keys[i], root)
	}
	return keys, values, nil
}

// chunkManagerStore is the manifest object store on the datacoord chunk
// manager. Its keys are complete chunk manager keys.
type chunkManagerStore struct{ cm storage.ChunkManager }

var _ manifest.ObjectStore = chunkManagerStore{}

func (s chunkManagerStore) Write(ctx context.Context, key string, data []byte) error {
	return s.cm.Write(ctx, key, data)
}

// Read returns an error marked with pkindex.ErrCorrupted when the key does not exist.
func (s chunkManagerStore) Read(ctx context.Context, key string) ([]byte, error) {
	b, err := s.cm.Read(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, errors.Mark(errors.Wrapf(err, "read object %s", key), pkindex.ErrCorrupted)
	}
	return b, err
}

func (s chunkManagerStore) Walk(ctx context.Context, prefix string, fn func(string, time.Time) bool) error {
	return s.cm.WalkWithPrefix(ctx, prefix, true, func(info *storage.ChunkObjectInfo) bool {
		return fn(info.FilePath, info.ModifyTime)
	})
}

func (s chunkManagerStore) MultiRemove(ctx context.Context, keys []string) error {
	return s.cm.MultiRemove(ctx, keys)
}

// initPKIndex builds the primary key index manifest manager and GC on the meta
// kv and cm, and gives the manager to the service. An invalid GC configuration
// leaves s.pkindexGC nil and does not stop the manager.
func (s *Server) initPKIndex(cm storage.ChunkManager) {
	meta := metaKVStore{kv: s.kv}
	store := chunkManagerStore{cm: cm}
	rootPath := cm.RootPath()
	cfg := pkindexgc.Config{
		Interval:       Params.DataCoordCfg.PKIndexGCInterval.GetAsDuration(time.Second),
		Grace:          Params.DataCoordCfg.PKIndexGCGrace.GetAsDuration(time.Second),
		RetainVersions: Params.DataCoordCfg.PKIndexManifestRetainVersions.GetAsInt(),
		RetainDuration: Params.DataCoordCfg.PKIndexManifestRetainDuration.GetAsDuration(time.Second),
	}
	gc, err := pkindexgc.New(meta, store, rootPath, cfg,
		// TODO: pass the outputs of the in-flight compactions of the primary
		// key index SSTs when that compaction exists.
		// TODO: add to the live set the tables that a streaming node still
		// reads on demand from object storage.
		nil)
	if err != nil {
		mlog.Error(s.ctx, "init primary key index gc failed, the gc is disabled", mlog.Err(err))
	} else {
		s.pkindexGC = gc
	}
	s.pkindexService.SetManager(manifest.New(meta, store, rootPath))
}
