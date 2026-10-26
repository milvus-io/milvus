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

package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
)

func TestApplyOfADeleteBeforeTheInstallWritesEveryTombstone(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	// a committed table with pk 1 and 2, from a handover of another "life"
	idx.insert(t, 1, 1, 2)
	idx.observed.Store(50)
	p, err := idx.prepare(ctx)
	require.NoError(t, err)
	require.NotNil(t, p)
	ref := p.request.AddTables[0]
	require.NoError(t, idx.store.UploadFile(ctx, pkroot.SSTKey(idx.sstBasePath, int64(ref.ID)), p.local[ref.ID]))
	view := ManifestView{Version: 1, SSTBasePath: idx.sstBasePath, CheckpointTimeTick: 50, Tables: []TableRef{ref}}

	// restart: empty engine, not ready, the delete of pk 1 is applied before the install
	idx.reopen(t)
	idx.SetCommitted(view)
	_, err = idx.Apply(ctx, Write{Deletes: int64PKs(1), TimeTick: 60})
	require.NoError(t, err)
	require.NoError(t, idx.Install(ctx))
	require.True(t, idx.Ready())

	g, err := idx.Decider().Decide(ctx, insertRequest(1, 2))
	require.NoError(t, err)
	assert.Equal(t, int64PKs(2), g.CompanionDeletePKs(), "pk 1 stays deleted after the install")
	g.NotAppended()
}

func TestInstallReconcilesTheCommittedDir(t *testing.T) {
	ctx := context.Background()
	idx := openTestIndex(t)
	dir := idx.committedDir
	require.NoError(t, os.MkdirAll(dir, 0o755))
	stale := filepath.Join(dir, "99.sst")
	partial := filepath.Join(dir, "5.sst.download-abc")
	require.NoError(t, os.WriteFile(stale, []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(partial, []byte("x"), 0o600))
	idx.SetCommitted(ManifestView{Version: 1, CheckpointTimeTick: 1})
	require.NoError(t, idx.Install(ctx))
	assert.NoFileExists(t, stale)
	assert.NoFileExists(t, partial)
	assert.True(t, idx.Ready())
}

func TestInstallWithoutAManifestOnlyMarksReady(t *testing.T) {
	idx := openTestIndex(t)
	require.False(t, idx.Ready())
	require.NoError(t, idx.Install(context.Background()))
	assert.True(t, idx.Ready())
	assert.Equal(t, int64(0), idx.currentInstalledVersion())
}

func TestInstallRetriesAfterAFailedDownload(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	var refs []TableRef
	for i, tt := range []uint64{50, 60} {
		idx.insert(t, 1, int64(2*i+1), int64(2*i+2))
		idx.observed.Store(tt)
		p, err := idx.prepare(ctx)
		require.NoError(t, err)
		require.NotNil(t, p)
		for _, ref := range p.request.AddTables {
			require.NoError(t, idx.store.UploadFile(ctx, pkroot.SSTKey(idx.sstBasePath, int64(ref.ID)), p.local[ref.ID]))
			if i == 0 || ref.ID != refs[0].ID {
				refs = append(refs, ref)
			}
		}
		if i == 0 {
			require.NoError(t, idx.installIfNewer(ctx, ManifestView{Version: 1, SSTBasePath: idx.sstBasePath, CheckpointTimeTick: tt, Tables: refs}, p.local))
		}
	}
	require.Len(t, refs, 2)
	view := ManifestView{Version: 2, SSTBasePath: idx.sstBasePath, CheckpointTimeTick: 60, Tables: refs}
	dir := idx.committedDir

	idx.reopen(t)
	idx.SetCommitted(view)
	for _, ref := range refs {
		require.NoError(t, os.RemoveAll(committedPath(dir, ref.ID)))
	}
	idx.store.FailDownloads(1)
	require.Error(t, idx.Install(ctx))
	assert.False(t, idx.Ready())
	assert.Equal(t, int64(0), idx.currentInstalledVersion())

	require.NoError(t, idx.Install(ctx))
	assert.True(t, idx.Ready())
	assert.Equal(t, int64(2), idx.currentInstalledVersion())
	for _, ref := range refs {
		assert.FileExists(t, committedPath(dir, ref.ID))
	}
	require.NoError(t, idx.Install(ctx))
	assert.True(t, idx.Ready())
}
