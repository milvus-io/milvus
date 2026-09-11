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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/gofrs/flock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func writeFile(t *testing.T, p, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	return string(b)
}

func displaced(root string) string {
	return filepath.Join(root, strings.TrimPrefix(root, string(filepath.Separator)))
}

func TestDisplacedRoots(t *testing.T) {
	assert.Equal(t, []string{"/var/lib/milvus/data/var/lib/milvus/data"}, DisplacedRoots("/var/lib/milvus/data/"))
	assert.Equal(t, []string{"/r/r"}, DisplacedRoots("/r"))
	assert.Empty(t, DisplacedRoots("data"))
	assert.Empty(t, DisplacedRoots(""))
	assert.Empty(t, DisplacedRoots("/"))
}

func TestMigrateRenamesDisplacedLayouts(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	for _, layout := range []string{"insert_log", "text_log", "json_stats", "index_files", "index_v1"} {
		writeFile(t, filepath.Join(displaced(root), layout, "1", "data"), layout)
	}
	writeFile(t, filepath.Join(displaced(root), "delta_log", "1", "data"), "untouched")
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, 5, report.Renamed)
	assert.EqualValues(t, 10+8+10+11+8, report.Bytes)
	for _, layout := range []string{"insert_log", "text_log", "json_stats", "index_files", "index_v1"} {
		assert.Equal(t, layout, readFile(t, filepath.Join(root, layout, "1", "data")))
		assert.NoDirExists(t, filepath.Join(displaced(root), layout))
	}
	assert.Equal(t, "untouched", readFile(t, filepath.Join(displaced(root), "delta_log", "1", "data")))
	second, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Zero(t, second.Renamed)
}

func TestMigrateMergesEmptyTargetDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	key := "insert_log/1/2/3/data"
	source := filepath.Join(displaced(root), key)
	target := filepath.Join(root, "insert_log")
	writeFile(t, source, "source")
	require.NoError(t, os.MkdirAll(target, 0o755))
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Empty(t, report.Conflicts)
	assert.Equal(t, "source", readFile(t, filepath.Join(root, key)))
	assert.NoFileExists(t, source)
}

func TestMigrateLeavesLegacyMinioNamespaceUntouched(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	key := "insert_log/1/2/5/_manifest/1.avro"
	writeFile(t, filepath.Join(root, "files", key), "relative-manifest-data")
	source := filepath.Join(displaced(root), key)
	writeFile(t, source, "complete-key-data")
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, 1, report.Renamed)
	assert.Equal(t, "complete-key-data", readFile(t, filepath.Join(root, key)))
	assert.Equal(t, "relative-manifest-data", readFile(t, filepath.Join(root, "files", key)))
}

func TestMigrateValidatesRootAndLegacyPrefix(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, root := range []string{"", "relative/root"} {
		_, err := Migrate(t.Context(), root, Options{})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
	file := filepath.Join(t.TempDir(), "file")
	writeFile(t, file, "data")
	_, err := Migrate(t.Context(), file, Options{})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	for _, prefix := range []string{"../outside", "/absolute"} {
		_, err := Migrate(t.Context(), t.TempDir(), Options{LegacyPrefix: prefix})
		require.Error(t, err)
	}
}

func TestMigrateFilesystemRootStillMigratesCWD(t *testing.T) {
	for _, withIndex := range []bool{false, true} {
		t.Run(fmt.Sprintf("index=%t", withIndex), func(t *testing.T) {
			cwd, sandbox := t.TempDir(), t.TempDir()
			t.Chdir(cwd)
			key := "index_files/7/0/2/3/milvus_packed_inverted_index.v3"
			if withIndex {
				writeFile(t, filepath.Join(cwd, key), "old-index")
			}
			// Exercise the configured filesystem root without writing to the
			// machine's actual root directory.
			var original func(string) (*os.Root, error)
			mock := mockey.Mock(os.OpenRoot).Origin(&original).To(func(name string) (*os.Root, error) {
				if name == "/" {
					return original(sandbox)
				}
				return original(name)
			}).Build()
			defer mock.UnPatch()
			report, err := Migrate(t.Context(), "/", Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			if withIndex {
				assert.Equal(t, "old-index", readFile(t, filepath.Join(sandbox, key)))
				assert.NoFileExists(t, filepath.Join(cwd, key))
			} else {
				assert.Zero(t, report.Renamed)
			}
		})
	}
}

func TestMigrateLockContention(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	writeFile(t, filepath.Join(displaced(root), "insert_log/1/data"), "data")
	lock := flock.New(filepath.Join(root, lockFileName))
	require.NoError(t, lock.Lock())
	defer lock.Unlock()
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

func TestMigrateRejectsSymlinks(t *testing.T) {
	t.Chdir(t.TempDir())
	root, outside := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(outside, "sentinel"), "outside")
	require.NoError(t, os.MkdirAll(filepath.Dir(displaced(root)), 0o755))
	require.NoError(t, os.Symlink(outside, displaced(root)))
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.Error(t, err)
	assert.Equal(t, "outside", readFile(t, filepath.Join(outside, "sentinel")))
}

func TestMigrateProtectsLiveManifestNamespace(t *testing.T) {
	t.Chdir(t.TempDir())
	root := t.TempDir()
	prefix := strings.TrimPrefix(root, string(filepath.Separator))
	source := filepath.Join(displaced(root), "insert_log/1/2/3/data")
	writeFile(t, source, "live")
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: prefix})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.Equal(t, "live", readFile(t, source))
}
