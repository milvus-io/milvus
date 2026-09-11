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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrateLegacyCWDUnifiedIndexesRenamesDeepestDirectories(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	files := map[string]string{
		"text_log/7/0/1/2/3/100/milvus_packed_text_index.v3":  "text-index",
		"index_files/8/0/2/3/milvus_packed_inverted_index.v3": "scalar-index",
	}
	for key, content := range files {
		writeFile(t, filepath.Join(cwd, key), content)
	}
	writeFile(t, filepath.Join(cwd, "notes.txt"), "unrelated")
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, 2, report.Renamed)
	assert.EqualValues(t, len("text-index")+len("scalar-index"), report.Bytes)
	for key, content := range files {
		assert.Equal(t, content, readFile(t, filepath.Join(root, key)))
		assert.NoFileExists(t, filepath.Join(cwd, key))
	}
	assert.Equal(t, "unrelated", readFile(t, filepath.Join(cwd, "notes.txt")))
	second, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Zero(t, second.Renamed)
}

func TestMigrateLegacyCWDUnknownTargetConflicts(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	key := "index_files/7/0/2/3/milvus_packed_inverted_index.v3"
	writeFile(t, filepath.Join(cwd, key), "source")
	writeFile(t, filepath.Join(root, key), "target")
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.Error(t, err)
	assert.NotEmpty(t, report.Conflicts)
	assert.Equal(t, "source", readFile(t, filepath.Join(cwd, key)))
}

func TestMigrateLegacyCWDMatchesDisplacedRoot(t *testing.T) {
	for _, name := range []string{"direct", "symlink_alias"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			cwd := displaced(root)
			files := map[string]string{
				"text_log/7/0/1/2/3/100/milvus_packed_text_index.v3":  "text-index",
				"index_files/8/0/2/3/milvus_packed_inverted_index.v3": "scalar-index",
			}
			for key, content := range files {
				writeFile(t, filepath.Join(cwd, key), content)
			}
			if name == "symlink_alias" {
				alias := filepath.Join(t.TempDir(), "legacy")
				require.NoError(t, os.Symlink(cwd, alias))
				t.Chdir(alias)
			} else {
				t.Chdir(cwd)
			}

			report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			assert.Empty(t, report.Conflicts)
			assert.Len(t, report.Candidates, 1)
			assert.Equal(t, 2, report.Renamed)
			assert.EqualValues(t, len("text-index")+len("scalar-index"), report.Bytes)
			for key, content := range files {
				assert.Equal(t, content, readFile(t, filepath.Join(root, key)))
				assert.NoFileExists(t, filepath.Join(cwd, key))
			}

			second, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			assert.Zero(t, second.Renamed)
		})
	}
}

func TestMigrateLegacyCWDDistinctFromDisplacedRootStillConflicts(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	key := "index_files/8/0/2/3/milvus_packed_inverted_index.v3"
	writeFile(t, filepath.Join(displaced(root), key), "displaced")
	writeFile(t, filepath.Join(cwd, key), "cwd")

	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.Error(t, err)
	assert.NotEmpty(t, report.Conflicts)
	assert.Zero(t, report.Renamed)
	assert.NoFileExists(t, filepath.Join(root, key))
	assert.Equal(t, "displaced", readFile(t, filepath.Join(displaced(root), key)))
	assert.Equal(t, "cwd", readFile(t, filepath.Join(cwd, key)))
}

func TestMigrateLegacyCWDSkipsUnrelatedFiles(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	for _, key := range []string{
		"notes.txt",
		"index_files/7/0/2/3/legacy-index",
		"index_files/7/0/2/3/milvus_packed_inverted_index.v2",
		"text_log/7/0/1/2/3/100/index_meta.json",
	} {
		writeFile(t, filepath.Join(cwd, key), "unrelated")
	}
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Zero(t, report.Renamed)
	assert.Equal(t, "unrelated", readFile(t, filepath.Join(cwd, "notes.txt")))
	assert.NoDirExists(t, filepath.Join(root, "index_files"))
}

func TestMigrateLegacyCWDNestedCanonicalRootIsSkipped(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	root := filepath.Join(cwd, "data")
	key := "index_files/7/0/2/3/milvus_packed_inverted_index.v3"
	writeFile(t, filepath.Join(root, key), "canonical")
	writeFile(t, filepath.Join(cwd, "index_files/8/0/2/3/milvus_packed_inverted_index.v3"), "legacy")
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	assert.Equal(t, 1, report.Renamed)
	assert.Equal(t, "canonical", readFile(t, filepath.Join(root, key)))
	assert.Equal(t, "legacy", readFile(t, filepath.Join(root, "index_files/8/0/2/3/milvus_packed_inverted_index.v3")))
}

func TestMigrateUnavailableCurrentDirectoryFailsBeforeMigration(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	source := filepath.Join(displaced(root), "insert_log/1/2/3/data")
	writeFile(t, source, "old-data")
	require.NoError(t, os.Remove(cwd))
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.Error(t, err)
	assert.NoFileExists(t, filepath.Join(root, "insert_log"))
	assert.Equal(t, "old-data", readFile(t, source))
}
