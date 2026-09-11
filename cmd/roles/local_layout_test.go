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

package roles

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage/localmigrate"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func localLayoutTestParams(t *testing.T, storageType, root string) *paramtable.ComponentParam {
	t.Helper()
	base := paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true))
	require.NoError(t, base.Save("common.storageType", storageType))
	require.NoError(t, base.Save("localStorage.path", root))
	params := &paramtable.ComponentParam{}
	params.Init(base)
	return params
}

func writeLocalLayoutTestFile(t *testing.T, root, key, contents string) {
	t.Helper()
	filename := filepath.Join(root, key)
	require.NoError(t, os.MkdirAll(filepath.Dir(filename), 0o755))
	require.NoError(t, os.WriteFile(filename, []byte(contents), 0o600))
}

func TestMigrateLocalStorageLayoutAutomaticSources(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	params := localLayoutTestParams(t, "local", root)
	mr := &MilvusRoles{Local: true}
	t.Chdir(cwd)
	keys := map[string]string{
		cwd:                                  "text_log/7/0/1/2/3/100/milvus_packed_text_index.v3",
		localmigrate.DisplacedRoots(root)[0]: "insert_log/1/2/3/_data/a.parquet",
	}
	for sourceRoot, key := range keys {
		writeLocalLayoutTestFile(t, sourceRoot, key, key)
	}
	report, err := mr.migrateLocalStorageLayout(t.Context(), params)
	require.NoError(t, err)
	require.Equal(t, 2, report.Renamed)
	for sourceRoot, key := range keys {
		require.NoFileExists(t, filepath.Join(sourceRoot, key))
		contents, err := os.ReadFile(filepath.Join(root, key))
		require.NoError(t, err)
		require.Equal(t, key, string(contents))
	}
	// A new startup finds no old directories.
	mr = &MilvusRoles{Local: true}
	params = localLayoutTestParams(t, "local", root)
	report, err = mr.migrateLocalStorageLayout(t.Context(), params)
	require.NoError(t, err)
	require.Zero(t, report.Renamed)
	require.Zero(t, report.Bytes)
	// Simulate normal GC. A restart does not resurrect deleted canonical data.
	for _, key := range keys {
		require.NoError(t, os.Remove(filepath.Join(root, key)))
	}
	report, err = mr.migrateLocalStorageLayout(t.Context(), params)
	require.NoError(t, err)
	require.Zero(t, report.Renamed)
	for _, key := range keys {
		require.NoFileExists(t, filepath.Join(root, key))
	}
}

func TestMigrateLocalStorageLayoutDisabled(t *testing.T) {
	for _, test := range []struct {
		name        string
		standalone  bool
		storageType string
	}{
		{name: "remote standalone", standalone: true, storageType: "remote"},
		{name: "local distributed", storageType: "local"},
		{name: "remote distributed", storageType: "remote"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, cwd := t.TempDir(), t.TempDir()
			params := localLayoutTestParams(t, test.storageType, root)
			mr := &MilvusRoles{Local: test.standalone}
			t.Chdir(cwd)
			key := "text_log/7/0/1/2/3/100/milvus_packed_text_index.v3"
			writeLocalLayoutTestFile(t, cwd, key, "old-index")
			writeLocalLayoutTestFile(t, localmigrate.DisplacedRoots(root)[0], key, "displaced-index")
			report, err := mr.migrateLocalStorageLayout(t.Context(), params)
			require.NoError(t, err)
			require.Nil(t, report)
			require.NoFileExists(t, filepath.Join(root, key))
			require.NoFileExists(t, filepath.Join(root, ".milvus-local-layout.lock"))
			require.NoDirExists(t, filepath.Join(root, ".milvus-local-layout"))
			require.FileExists(t, filepath.Join(cwd, key))
			require.FileExists(t, filepath.Join(localmigrate.DisplacedRoots(root)[0], key))
		})
	}
}

func TestMigrateLocalStorageLayoutConflictStopsStartup(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	params := localLayoutTestParams(t, "local", root)
	mr := &MilvusRoles{Local: true}
	t.Chdir(cwd)
	key := "index_files/7/0/2/3/milvus_packed_inverted_index.v3"
	writeLocalLayoutTestFile(t, cwd, key, "old-index")
	writeLocalLayoutTestFile(t, root, key, "new-index")
	report, err := mr.migrateLocalStorageLayout(t.Context(), params)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.Equal(t, []string{filepath.Join(root, key)}, report.Conflicts)
	contents, err := os.ReadFile(filepath.Join(root, key))
	require.NoError(t, err)
	require.Equal(t, "new-index", string(contents))
}

func TestMigrateLocalStorageLayoutWithoutLegacyFiles(t *testing.T) {
	for _, layout := range []string{"existing-root", "missing-root"} {
		t.Run(layout, func(t *testing.T) {
			root, cwd := t.TempDir(), t.TempDir()
			if layout == "missing-root" {
				root = filepath.Join(root, "new-install")
			}
			params := localLayoutTestParams(t, "local", root)
			if layout == "missing-root" {
				// Configuration creates this empty directory when reading disk
				// capacity. Remove it so migration really receives a missing root.
				require.NoError(t, os.Remove(root))
				require.NoDirExists(t, root)
			}
			mr := &MilvusRoles{Local: true}
			t.Chdir(cwd)
			_, err := mr.migrateLocalStorageLayout(t.Context(), params)
			require.NoError(t, err)
			require.NoDirExists(t, filepath.Join(root, ".milvus-local-layout"))
			if layout == "missing-root" {
				require.NoDirExists(t, root, "migration must not create a missing storage root")
				require.NoFileExists(t, filepath.Join(root, ".milvus-local-layout.lock"))
			}
		})
	}
}

func TestMigrateLocalStorageLayoutRetriesAfterCopyFailure(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	key := "index_files/7/0/2/3/milvus_packed_inverted_index.v3"
	writeLocalLayoutTestFile(t, cwd, key, "old-index")
	// A regular file in place of the destination directory makes rename fail.
	blocker := filepath.Join(root, "index_files")
	require.NoError(t, os.WriteFile(blocker, []byte("not-a-directory"), 0o600))
	mr := &MilvusRoles{Local: true}
	_, err := mr.migrateLocalStorageLayout(t.Context(), localLayoutTestParams(t, "local", root))
	require.Error(t, err)
	source, err := os.ReadFile(filepath.Join(cwd, key))
	require.NoError(t, err)
	require.Equal(t, "old-index", string(source))

	// Once the filesystem problem is resolved, a fresh standalone startup
	// recovers directly from the same visible source without a shell handoff.
	require.NoError(t, os.Remove(blocker))
	mr = &MilvusRoles{Local: true}
	report, err := mr.migrateLocalStorageLayout(t.Context(), localLayoutTestParams(t, "local", root))
	require.NoError(t, err)
	require.Equal(t, 1, report.Renamed)
	require.Equal(t, 1, report.Renamed)
	require.NoFileExists(t, filepath.Join(cwd, key))
	contents, err := os.ReadFile(filepath.Join(root, key))
	require.NoError(t, err)
	require.Equal(t, "old-index", string(contents))
}

func TestMigrateLocalStorageLayoutProtectsLegacyManifestNamespace(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	params := localLayoutTestParams(t, "local", root)
	// This configured legacy prefix resolves to the same physical namespace as
	// the double-root source. A relative manifest still reads there after reload.
	relativeRoot, err := filepath.Rel(string(filepath.Separator), root)
	require.NoError(t, err)
	require.NoError(t, params.Save("minio.rootPath", filepath.ToSlash(relativeRoot)))
	key := "insert_log/1/2/3/_data/a.parquet"
	sourceRoot := localmigrate.DisplacedRoots(root)[0]
	writeLocalLayoutTestFile(t, sourceRoot, key, "still-referenced")
	mr := &MilvusRoles{Local: true}
	_, err = mr.migrateLocalStorageLayout(t.Context(), params)
	require.Error(t, err)
	require.FileExists(t, filepath.Join(sourceRoot, key))
	require.NoFileExists(t, filepath.Join(root, key))
	require.NoDirExists(t, filepath.Join(root, ".milvus-local-layout", "backups"))
}

// The migration blocks startup ahead of every component, so exceeding its budget
// must fail the start with an actionable message instead of hanging. The message
// has to say the work is resumable, because the operator's action is to grant
// more time, not to repair anything.
func TestMigrateLocalStorageLayoutTimeoutIsActionable(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	params := localLayoutTestParams(t, "local", root)
	key := "insert_log/1/2/3/_data/a.parquet"
	sourceRoot := localmigrate.DisplacedRoots(root)[0]
	writeLocalLayoutTestFile(t, sourceRoot, key, "payload")
	mr := &MilvusRoles{Local: true}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := mr.migrateLocalStorageLayout(ctx, params)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "resumes where it stopped")
	require.Contains(t, err.Error(), params.LocalStorageCfg.LayoutMigrationTimeout.Key)
	// Nothing was moved, so a restart with a larger budget still has the work.
	require.FileExists(t, filepath.Join(sourceRoot, key))
}

// A non-positive budget disables the limit instead of expiring immediately.
func TestMigrateLocalStorageLayoutTimeoutDisabled(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	params := localLayoutTestParams(t, "local", root)
	require.NoError(t, params.Save(params.LocalStorageCfg.LayoutMigrationTimeout.Key, "0"))
	key := "insert_log/1/2/3/_data/a.parquet"
	writeLocalLayoutTestFile(t, localmigrate.DisplacedRoots(root)[0], key, "payload")
	mr := &MilvusRoles{Local: true}

	report, err := mr.migrateLocalStorageLayout(context.Background(), params)

	require.NoError(t, err)
	require.Equal(t, 1, report.Renamed+report.Copied)
	require.FileExists(t, filepath.Join(root, key))
}
