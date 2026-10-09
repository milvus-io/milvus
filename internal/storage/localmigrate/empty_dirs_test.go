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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMigrateLegacyCWDRemovesEmptyAncestors(t *testing.T) {
	for _, key := range []string{
		"index_files/7/0/2/3/milvus_packed_inverted_index.v3",
		"text_log/7/0/1/2/3/100/milvus_packed_text_index.v3",
	} {
		layout := strings.Split(key, "/")[0]
		t.Run(layout, func(t *testing.T) {
			root, cwd := t.TempDir(), t.TempDir()
			t.Chdir(cwd)
			writeFile(t, filepath.Join(cwd, key), "legacy-index")
			_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.Equal(t, "legacy-index", readFile(t, filepath.Join(root, key)))
			require.NoDirExists(t, filepath.Join(cwd, layout), "remove the empty layout root, not just the packed leaf")
			require.DirExists(t, cwd)
			report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.Empty(t, report.Candidates)
			require.Zero(t, report.Renamed)
			require.Zero(t, report.Copied)
		})
	}
}

func TestMigrateLegacyCWDCleansEmptySkeletonOnRestart(t *testing.T) {
	for _, skeleton := range []string{
		"index_files/7/0/2/3",
		"text_log/7/0/1/2/3/100",
	} {
		layout := strings.Split(skeleton, "/")[0]
		t.Run(layout, func(t *testing.T) {
			root, cwd := t.TempDir(), t.TempDir()
			t.Chdir(cwd)
			// Simulate interruption after the final packed file was moved:
			// only eligible empty directories remain, so no file selects a leaf.
			require.NoError(t, os.MkdirAll(filepath.Join(cwd, skeleton), 0o755))
			canonicalEmpty := filepath.Join(root, skeleton)
			require.NoError(t, os.MkdirAll(canonicalEmpty, 0o755))
			report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.Zero(t, report.Renamed)
			require.Zero(t, report.Copied)
			require.NoDirExists(t, filepath.Join(cwd, layout))
			require.DirExists(t, cwd)
			require.DirExists(t, canonicalEmpty, "canonical empty directories are not legacy cleanup candidates")
		})
	}
}

func TestMigrateLegacyCWDEmptyCleanupPreservesUnrelatedEntries(t *testing.T) {
	for _, key := range []string{
		"index_files/7/0/2/3/milvus_packed_inverted_index.v3",
		"text_log/7/0/1/2/3/100/milvus_packed_text_index.v3",
	} {
		layout := strings.Split(key, "/")[0]
		t.Run(layout, func(t *testing.T) {
			root, cwd := t.TempDir(), t.TempDir()
			t.Chdir(cwd)
			writeFile(t, filepath.Join(cwd, key), "legacy-index")
			unrelatedFile := filepath.Join(cwd, filepath.Dir(filepath.Dir(key)), "999", "legacy-index")
			writeFile(t, unrelatedFile, "unrelated-data")
			unknownEmpty := filepath.Join(cwd, layout, "notes", "empty")
			tooDeepEmpty := filepath.Join(cwd, strings.Replace(filepath.Dir(key), "/7/", "/9/", 1), "111")
			outsideEmpty := filepath.Join(cwd, "unrelated-empty")
			for _, directory := range []string{unknownEmpty, tooDeepEmpty, outsideEmpty} {
				require.NoError(t, os.MkdirAll(directory, 0o755))
			}
			_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.Equal(t, "legacy-index", readFile(t, filepath.Join(root, key)))
			require.Equal(t, "unrelated-data", readFile(t, unrelatedFile))
			for _, directory := range []string{unknownEmpty, tooDeepEmpty, outsideEmpty, filepath.Join(cwd, layout)} {
				require.DirExists(t, directory, "cleanup must stop at nonempty or unrecognized directories")
			}
		})
	}
}

func TestMigrateLegacyCWDEmptyCleanupProtectsCanonicalRoot(t *testing.T) {
	for _, nested := range []bool{false, true} {
		name := "cwd-is-root"
		if nested {
			name = "root-inside-legacy-layout"
		}
		t.Run(name, func(t *testing.T) {
			cwd := t.TempDir()
			t.Chdir(cwd)
			root := cwd
			if nested {
				root = filepath.Join(cwd, "index_files/7/0/2")
			}
			canonicalEmpty := filepath.Join(root, "3")
			if !nested {
				canonicalEmpty = filepath.Join(root, "index_files/7/0/2/3")
			}
			require.NoError(t, os.MkdirAll(canonicalEmpty, 0o755))
			_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.DirExists(t, cwd)
			require.DirExists(t, root)
			require.DirExists(t, canonicalEmpty)
		})
	}
}

func TestMigrateLegacyCWDEmptyCleanupDoesNotDeleteReplacementFile(t *testing.T) {
	for _, kind := range []string{"leaf", "ancestor"} {
		t.Run(kind, func(t *testing.T) {
			root, cwd := t.TempDir(), t.TempDir()
			t.Chdir(cwd)
			key := "index_files/7/0/2/3/milvus_packed_inverted_index.v3"
			writeFile(t, filepath.Join(cwd, key), "legacy-index")
			emptyLeaf := filepath.Join(cwd, "index_files/8/0/2/3")
			require.NoError(t, os.MkdirAll(emptyLeaf, 0o755))
			replacement := emptyLeaf
			if kind == "ancestor" {
				replacement = filepath.Dir(emptyLeaf)
			}
			replaced := false
			_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files", OnDirDone: func(Dir, time.Duration) {
				if !replaced {
					// Discovery already saw directories here. Change the leaf
					// or its parent before cleanup; neither may unlink a file.
					if kind == "ancestor" {
						require.NoError(t, os.Remove(emptyLeaf))
					}
					require.NoError(t, os.Remove(replacement))
					writeFile(t, replacement, "keep-replacement")
					replaced = true
				}
			}})
			require.NoError(t, err)
			require.True(t, replaced)
			require.Equal(t, "keep-replacement", readFile(t, replacement))
		})
	}
}

func TestMigrateLegacyCWDEmptyCleanupWaitsForSuccessfulMigration(t *testing.T) {
	root, cwd := t.TempDir(), t.TempDir()
	t.Chdir(cwd)
	key := "index_files/7/0/2/3/milvus_packed_inverted_index.v3"
	writeFile(t, filepath.Join(cwd, key), "legacy-index")
	writeFile(t, filepath.Join(root, key), "conflicting-target")
	empty := filepath.Join(cwd, "index_files/8/0/2/3")
	require.NoError(t, os.MkdirAll(empty, 0o755))
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.Error(t, err)
	require.NotEmpty(t, report.Conflicts)
	require.DirExists(t, empty, "a failed migration must not proceed to empty-directory cleanup")
	require.Equal(t, "legacy-index", readFile(t, filepath.Join(cwd, key)))
	require.Equal(t, "conflicting-target", readFile(t, filepath.Join(root, key)))
}

func TestMigrateLegacyCWDEmptyCleanupProtectsActiveAndReservedNamespaces(t *testing.T) {
	for _, prefix := range []string{"files/insert_log/1/2/3", ".milvus-local-layout"} {
		t.Run(prefix, func(t *testing.T) {
			root := t.TempDir()
			cwd := filepath.Join(root, prefix)
			empty := filepath.Join(cwd, "index_files/7/0/2/3")
			require.NoError(t, os.MkdirAll(empty, 0o755))
			t.Chdir(cwd)
			_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.DirExists(t, empty)
			require.DirExists(t, cwd)
		})
	}
}
