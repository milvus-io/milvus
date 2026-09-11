// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package localmigrate

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestMigrateMergeSplitJSONShreddingSegment(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	segment := "insert_log/100/10/2001"
	canonical := map[string]string{
		segment + "/_stats/json_stats.103/meta.json":             "json-metadata",
		segment + "/_stats/json_stats.103/shared_key_index/keys": "shared-keys",
	}
	legacy := map[string]string{
		segment + "/_data/data.parquet":                             "segment-data",
		segment + "/_metadata/manifest-3.avro":                      "manifest",
		segment + "/_stats/text_index.102/packed":                   "text-index",
		segment + "/_stats/json_stats.103/shredding_data/0.parquet": "shredded-json",
	}
	for key, content := range canonical {
		writeFile(t, filepath.Join(root, key), content)
	}
	for key, content := range legacy {
		writeFile(t, filepath.Join(displaced(root), key), content)
	}
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	for key, content := range canonical {
		assert.Equal(t, content, readFile(t, filepath.Join(root, key)))
	}
	for key, content := range legacy {
		assert.Equal(t, content, readFile(t, filepath.Join(root, key)))
		assert.NoFileExists(t, filepath.Join(displaced(root), key))
	}
	assert.NoDirExists(t, filepath.Join(displaced(root), "insert_log"))
	assertMigrationRestartIsIdle(t, root)
}

func TestMigrateMergePreservesCanonicalV1Segment(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	v1 := "insert_log/100/10/2000/101/1234"
	v3 := "insert_log/100/10/2001/_data/data.parquet"
	writeFile(t, filepath.Join(root, v1), "v1-binlog")
	writeFile(t, filepath.Join(displaced(root), v3), "v3-data")
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, "v1-binlog", readFile(t, filepath.Join(root, v1)))
	assert.Equal(t, "v3-data", readFile(t, filepath.Join(root, v3)))
	assert.NoDirExists(t, filepath.Join(displaced(root), "insert_log"))
	assertMigrationRestartIsIdle(t, root)
}

func TestMigrateMergeDisplacedAndCWDIndexes(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	displacedKey := "index_files/8/0/2/3/milvus_packed_inverted_index.v3"
	cwdKey := "index_files/9/0/2/3/milvus_packed_inverted_index.v3"
	writeFile(t, filepath.Join(displaced(root), displacedKey), "displaced-index")
	writeFile(t, filepath.Join(cwd, cwdKey), "cwd-index")
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, "displaced-index", readFile(t, filepath.Join(root, displacedKey)))
	assert.Equal(t, "cwd-index", readFile(t, filepath.Join(root, cwdKey)))
	assert.NoDirExists(t, filepath.Join(displaced(root), "index_files"))
	assert.NoFileExists(t, filepath.Join(cwd, cwdKey))
	assertMigrationRestartIsIdle(t, root)
}

func TestMigrateCopyFallbackCrossDevice(t *testing.T) {
	for _, sourceKind := range []string{"displaced", "cwd"} {
		t.Run(sourceKind, func(t *testing.T) {
			root, cwd := t.TempDir(), t.TempDir()
			t.Chdir(cwd)
			key := "index_files/8/0/2/3/milvus_packed_inverted_index.v3"
			sourceRoot, sourceDirectory := displaced(root), "index_files"
			if sourceKind == "cwd" {
				sourceRoot, sourceDirectory = cwd, filepath.Dir(key)
			}
			sourceFile := filepath.Join(sourceRoot, key)
			writeFile(t, sourceFile, "complete-index")
			injected := injectSourceRenameError(t, filepath.Join(sourceRoot, filepath.Dir(sourceDirectory)), unix.EXDEV)
			_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.Positive(t, *injected, "must exercise the cross-device fallback")
			assert.Equal(t, "complete-index", readFile(t, filepath.Join(root, key)))
			assert.NoDirExists(t, filepath.Join(sourceRoot, sourceDirectory))
			assertMigrationRestartIsIdle(t, root)
		})
	}
}

func TestMigrateRenamePermissionErrorDoesNotCopy(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	key := "index_files/8/0/2/3/milvus_packed_inverted_index.v3"
	writeFile(t, filepath.Join(cwd, key), "source-index")
	injected := injectSourceRenameError(t, filepath.Join(cwd, filepath.Dir(filepath.Dir(key))), unix.EACCES)
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.ErrorIs(t, err, unix.EACCES)
	require.Positive(t, *injected)
	assert.Equal(t, "source-index", readFile(t, filepath.Join(cwd, key)))
	assert.NoFileExists(t, filepath.Join(root, key))
}

func TestMigrateCopyFailureLeavesSourceAndRetries(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	key := "index_files/8/0/2/3/milvus_packed_inverted_index.v3"
	source, target := filepath.Join(cwd, key), filepath.Join(root, key)
	writeFile(t, source, "complete-original-index")
	injected := injectSourceRenameError(t, filepath.Join(cwd, filepath.Dir(filepath.Dir(key))), unix.EXDEV)
	copyCalls := 0
	copyPatch := mockey.Mock(io.Copy).To(func(dst io.Writer, src io.Reader) (int64, error) {
		copyCalls++
		assert.Equal(t, "complete-original-index", readFile(t, source))
		assert.NoFileExists(t, target, "an incomplete copy must not become a canonical file")
		n, err := dst.Write([]byte("partial"))
		require.NoError(t, err)
		return int64(n), io.ErrShortWrite
	}).Build()
	t.Cleanup(func() { copyPatch.UnPatch() })
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Positive(t, *injected)
	require.Equal(t, 1, copyCalls)
	assert.Equal(t, "complete-original-index", readFile(t, source))
	assert.NoFileExists(t, target)
	copyPatch.UnPatch()
	_, err = Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, "complete-original-index", readFile(t, target))
	assert.NoFileExists(t, source)
	assertMigrationRestartIsIdle(t, root)
}

func TestMigrateCopyPublishedBeforeSourceRemovalRetries(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	key := "index_files/8/0/2/3/milvus_packed_inverted_index.v3"
	source, target := filepath.Join(cwd, key), filepath.Join(root, key)
	writeFile(t, source, "complete-index")
	injectSourceRenameError(t, filepath.Join(cwd, filepath.Dir(filepath.Dir(key))), unix.EXDEV)
	var originalRemove func(*os.Root, string) error
	removeCalls := 0
	removePatch := mockey.Mock((*os.Root).Remove).To(func(openedRoot *os.Root, name string) error {
		if filepath.Join(openedRoot.Name(), name) == source {
			removeCalls++
			assert.Equal(t, "complete-index", readFile(t, target), "publish a complete target before removing its source")
			return unix.EACCES
		}
		return originalRemove(openedRoot, name)
	}).Origin(&originalRemove).Build()
	t.Cleanup(func() { removePatch.UnPatch() })
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.ErrorIs(t, err, unix.EACCES)
	require.Equal(t, 1, removeCalls)
	assert.Equal(t, "complete-index", readFile(t, source))
	assert.Equal(t, "complete-index", readFile(t, target))
	removePatch.UnPatch()
	_, err = Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err, "an already-published copy must not become a same-name conflict on retry")
	assert.Equal(t, "complete-index", readFile(t, target))
	assert.NoFileExists(t, source)
	assertMigrationRestartIsIdle(t, root)
}

func TestMigrateMergeRejectsSameNameConflicts(t *testing.T) {
	for _, conflictKind := range []string{"different-files", "source-file-target-directory", "source-directory-target-file"} {
		t.Run(conflictKind, func(t *testing.T) {
			t.Chdir(t.TempDir())
			root := t.TempDir()
			key := "insert_log/100/10/2001/_data/conflict"
			source, target := filepath.Join(displaced(root), key), filepath.Join(root, key)
			sourceFile, targetFile := source, target
			if conflictKind == "source-file-target-directory" {
				targetFile = filepath.Join(target, "data")
			}
			if conflictKind == "source-directory-target-file" {
				sourceFile = filepath.Join(source, "data")
			}
			writeFile(t, sourceFile, "legacy")
			writeFile(t, targetFile, "canonical")
			report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			assert.NotEmpty(t, report.Conflicts)
			assert.Equal(t, "legacy", readFile(t, sourceFile))
			assert.Equal(t, "canonical", readFile(t, targetFile))
		})
	}
}

func assertMigrationRestartIsIdle(t *testing.T, root string) {
	t.Helper()
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Zero(t, report.Renamed)
	assert.Zero(t, report.Bytes)
}

// Only the old source filesystem rejects rename. Temporary-file publication
// inside the target filesystem must still execute the real system call.
func injectSourceRenameError(t *testing.T, sourceParent string, injectedErr error) *int {
	t.Helper()
	var parents []unix.Stat_t
	require.NoError(t, filepath.WalkDir(sourceParent, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			var stat unix.Stat_t
			if err := unix.Stat(name, &stat); err != nil {
				return err
			}
			parents = append(parents, stat)
		}
		return nil
	}))
	var original func(int, string, int, string) error
	injected := 0
	patch := mockey.Mock(unix.Renameat).To(func(oldfd int, oldname string, newfd int, newname string) error {
		var actual unix.Stat_t
		if err := unix.Fstat(oldfd, &actual); err == nil {
			for _, expected := range parents {
				if actual.Dev == expected.Dev && actual.Ino == expected.Ino {
					injected++
					return injectedErr
				}
			}
		}
		return original(oldfd, oldname, newfd, newname)
	}).Origin(&original).Build()
	t.Cleanup(func() { patch.UnPatch() })
	return &injected
}
