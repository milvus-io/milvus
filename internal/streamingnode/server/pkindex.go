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

package server

import (
	"context"
	"path/filepath"

	"golang.org/x/sys/unix"

	pkindexclient "github.com/milvus-io/milvus/internal/datacoord/pkindex/client"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/objstore"
	"github.com/milvus-io/milvus/internal/storage"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The manifest service client and the object store satisfy the interfaces
// the index package defines for them.
var (
	_ snpkindex.ManifestService = pkindexclient.Client(nil)
	_ snpkindex.ObjectReader    = (*objstore.Store)(nil)
)

// openFileReserve is the number of file descriptors kept for everything other
// than the primary key index table cache, such as network connections.
const openFileReserve = 10000

// pkindexComponents holds the primary key index objects of the streaming node.
// A nil *pkindexComponents means the index is off.
type pkindexComponents struct {
	// root is the local directory of the index data.
	root   string
	mgr    *snpkindex.Manager
	shared *engine.SharedResources
	// manifestClient reaches the manifest service of the coordinator. The
	// manager commits and reads manifests through it.
	manifestClient pkindexclient.Client
}

// newPKIndexComponents builds the primary key index manager when
// streaming.pkindex.enabled is true. It returns nil when the index is off.
// newManifestClient builds the manifest service client; it is called only
// when the index is on.
func newPKIndexComponents(ctx context.Context, cm storage.ChunkManager, newManifestClient func() pkindexclient.Client) *pkindexComponents {
	cfg := &paramtable.Get().StreamingCfg
	if !cfg.PKIndexEnabled.GetAsBool() {
		return nil
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
	manifestClient := newManifestClient()
	mgr := snpkindex.NewManager(snpkindex.ManagerConfig{
		Root:   root,
		Shared: shared,
		// The resource is set before any WAL opens, so the call is safe.
		AllocID: func(ctx context.Context) (int64, error) {
			id, err := resource.Resource().IDAllocator().Allocate(ctx)
			return int64(id), err
		},
		Store:           store,
		StorageRootPath: cm.RootPath(),
		Committer:       snpkindex.NewManifestServiceCommitter(manifestClient, store),
		Sched:           nodescheduler.Get(),
		Params:          snpkindex.ParamsFromConfig,
	})
	mlog.Info(ctx, "primary key index manager created", mlog.String("root", root),
		mlog.Int64("blockCacheBytes", cfg.PKIndexBlockCacheSize.GetAsSize()), mlog.Int("maxOpenFiles", maxOpenFiles))
	return &pkindexComponents{root: root, mgr: mgr, shared: shared, manifestClient: manifestClient}
}

// manager returns the index manager, nil when the index is off.
func (c *pkindexComponents) manager() *snpkindex.Manager {
	if c == nil {
		return nil
	}
	return c.mgr
}

// close closes the manager, the manifest client and the shared caches in
// this order. Call it after the WAL manager closes.
func (c *pkindexComponents) close() {
	if c == nil {
		return
	}
	c.mgr.Close()
	c.manifestClient.Close()
	c.shared.Release()
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
