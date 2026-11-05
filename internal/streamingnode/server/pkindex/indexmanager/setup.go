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

package indexmanager

import (
	"context"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex/objstore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// openFileReserve is the number of file descriptors kept for everything other
// than the primary key index table cache, such as network connections.
const openFileReserve = 10000

// Setup builds the primary key index manager of the streaming node when
// streaming.pkindex.enabled is true. It returns (nil, nil, nil) when the
// index is off. newCommitter builds the Committer and the function that
// releases it; it is called only when the index is on. allocID allocates the
// IDs of new tables, from the ID allocator of the resource. syncIDAllocator
// discards cached IDs before reclaimed tables receive new IDs. The returned
// close function closes the manager, the committer and the shared caches, in
// this order; call it after the WAL manager closes.
func Setup(ctx context.Context, cm storage.ChunkManager, newCommitter func(context.Context) (manifest.Committer, func(), error), allocID func(context.Context) (int64, error), syncIDAllocator func()) (Manager, func(), error) {
	cfg := &paramtable.Get().StreamingCfg
	if !cfg.PKIndexEnabled.GetAsBool() {
		return nil, nil, nil
	}
	root := cfg.PKIndexDataDir.GetValue()
	if root == "" {
		root = filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), "pkindex")
	}
	maxOpenFiles := cfg.PKIndexMaxOpenFiles.GetAsInt()
	checkOpenFileLimit(ctx, maxOpenFiles)

	shared := engine.NewSharedResources(engine.SharedConfig{
		CacheBytes:   cfg.PKIndexBlockCacheSize.GetAsSize(),
		MaxOpenFiles: maxOpenFiles,
	})
	store := objstore.New(cm)
	committer, closeCommitter, err := newCommitter(ctx)
	if err != nil {
		shared.Release()
		return nil, nil, err
	}
	mgr := NewManager(ManagerConfig{
		Root:                    root,
		Engine:                  shared,
		AllocID:                 allocID,
		SyncIDAllocator:         syncIDAllocator,
		Store:                   store,
		StorageRootPath:         cm.RootPath(),
		Committer:               committer,
		Reader:                  store,
		Sched:                   nodescheduler.Get(),
		ManifestReadConcurrency: cfg.PKIndexRecoveryManifestReadConcurrency.GetAsInt(),
		Params:                  ParamsFromConfig,
	})
	mlog.Info(ctx, "primary key index manager created", mlog.String("root", root),
		mlog.Int64("blockCacheBytes", cfg.PKIndexBlockCacheSize.GetAsSize()), mlog.Int("maxOpenFiles", maxOpenFiles))
	return mgr, func() {
		mgr.Close()
		if closeCommitter != nil {
			closeCommitter()
		}
		shared.Release()
	}, nil
}

// checkOpenFileLimit logs a warning when the soft limit of open files is too
// low for the table cache plus a reserve. It never fails the start, because
// the number of vchannels is unknown at start.
func checkOpenFileLimit(ctx context.Context, maxOpenFiles int) {
	var rlim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rlim); err != nil {
		mlog.Warn(ctx, "read the open file limit failed", mlog.Err(err))
		return
	}
	if need, short := openFileLimitShort(rlim.Cur, maxOpenFiles); short {
		mlog.Warn(ctx, "the soft open file limit is lower than the primary key index needs, raise it with ulimit -n",
			mlog.Uint64("softLimit", rlim.Cur), mlog.Uint64("need", need),
			mlog.Int("maxOpenFiles", maxOpenFiles), mlog.Int("reserve", openFileReserve))
	}
}

// openFileLimitShort returns the number of open files that the node needs and
// whether soft is below it. A maxOpenFiles of 0 or less means the engine default.
func openFileLimitShort(soft uint64, maxOpenFiles int) (uint64, bool) {
	if maxOpenFiles <= 0 {
		maxOpenFiles = engine.DefaultMaxOpenFiles
	}
	need := uint64(maxOpenFiles) + openFileReserve
	return need, soft < need
}

// ParamsFromConfig reads the params from paramtable. ReuploadAfter is half
// of the grace window of the garbage collection of the manifest service: the
// garbage collection removes an uploaded table that no manifest references
// after the grace window, so a retry that waits longer uploads again first.
func ParamsFromConfig() Params {
	cfg := &paramtable.Get().StreamingCfg
	grace := paramtable.Get().DataCoordCfg.PKIndexGCGrace.GetAsDuration(time.Second)
	return Params{
		Params: index.Params{
			MemTableSize:  uint64(cfg.PKIndexMemTableSize.GetAsSize()),
			ReuploadAfter: grace / 2,
		},
		ActiveBytes:   uint64(cfg.PKIndexHandoverActiveBytes.GetAsSize()),
		TailBytes:     uint64(cfg.PKIndexHandoverTailBytes.GetAsSize()),
		SoftWatermark: uint64(cfg.WALRecoveryTailSoftWatermark.GetAsSize()),
		BackoffBase:   defaultBackoffBase,
		MaxBackoff:    cfg.PKIndexHandoverMaxBackoff.GetAsDurationByParse(),
		StallTimeout:  paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second),
	}
}
