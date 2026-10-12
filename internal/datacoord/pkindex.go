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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	pkindexgc "github.com/milvus-io/milvus/internal/pkindex/coordinator/gc"
	"github.com/milvus-io/milvus/internal/pkindex/coordinator/manifestmanager"
	"github.com/milvus-io/milvus/internal/pkindex/core/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// metaKVPointers is the manifest pointer store on the datacoord meta kv. Its
// keys are relative to the root path of the meta kv.
type metaKVPointers struct{ kv kv.MetaKv }

var _ manifestmanager.PointerStore = metaKVPointers{}

func (p metaKVPointers) Load(ctx context.Context, key string) (string, bool, error) {
	v, err := p.kv.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

func (p metaKVPointers) Create(ctx context.Context, key, value string) error {
	return p.kv.MultiSaveAndRemove(ctx, map[string]string{key: value}, nil, predicates.KeyNotExist(key))
}

func (p metaKVPointers) Swap(ctx context.Context, key, expected, value string) error {
	return p.kv.MultiSaveAndRemove(ctx, map[string]string{key: value}, nil, predicates.ValueEqual(key, expected))
}

// List returns the keys under prefix without the root path of the meta kv,
// which LoadWithPrefix includes.
func (p metaKVPointers) List(ctx context.Context, prefix string) ([]string, []string, error) {
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

// Read returns an error marked with pkerr.ErrCorrupted when the key does not exist.
func (s chunkManagerStore) Read(ctx context.Context, key string) ([]byte, error) {
	b, err := s.cm.Read(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, errors.Mark(errors.Wrapf(err, "read object %s", key), pkerr.ErrCorrupted)
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
// kv and cm, and gives the manager to the service.
func (s *Server) initPKIndex(cm storage.ChunkManager) error {
	pointers := metaKVPointers{kv: s.kv}
	store := chunkManagerStore{cm: cm}
	layout := manifest.Layout{RootPath: cm.RootPath()}
	gc, err := pkindexgc.New(pointers, store, layout, pkindexgc.Config{
		Interval:       Params.DataCoordCfg.PKIndexGCInterval.GetAsDuration(time.Second),
		Grace:          Params.DataCoordCfg.PKIndexGCGrace.GetAsDuration(time.Second),
		RetainVersions: Params.DataCoordCfg.PKIndexManifestRetainVersions.GetAsInt(),
		RetainDuration: Params.DataCoordCfg.PKIndexManifestRetainDuration.GetAsDuration(time.Second),
	}, nil)
	if err != nil {
		return merr.Wrap(err, "init primary key index gc")
	}
	s.pkindexGC = gc
	s.pkindexService.SetManager(manifestmanager.New(pointers, store, layout))
	return nil
}

// RegisterPKIndexCoordGRPCService registers the primary key index manifest service on server.
func (s *Server) RegisterPKIndexCoordGRPCService(server *grpc.Server) {
	pkindexpb.RegisterPKIndexCoordServiceServer(server, s.pkindexService)
}
