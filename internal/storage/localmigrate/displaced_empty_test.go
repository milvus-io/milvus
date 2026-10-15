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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestMigrateDisplacedEmptySkeleton(t *testing.T) {
	for _, layout := range LayoutDirs {
		t.Run(layout, func(t *testing.T) {
			root := t.TempDir()
			t.Chdir(t.TempDir())
			skeleton := filepath.Join(layout, "1/2/3/empty")
			require.NoError(t, os.MkdirAll(filepath.Join(displaced(root), skeleton), 0o755))
			canonical := filepath.Join(root, skeleton)
			require.NoError(t, os.MkdirAll(canonical, 0o755))
			unrelated := filepath.Join(displaced(root), "unrelated/empty")
			require.NoError(t, os.MkdirAll(unrelated, 0o755))

			report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.Empty(t, report.Candidates)
			require.Zero(t, report.Renamed)
			require.Zero(t, report.Copied)
			require.NoDirExists(t, filepath.Join(displaced(root), layout))
			require.DirExists(t, canonical)
			require.DirExists(t, unrelated)
			require.DirExists(t, root)
		})
	}
}

func TestMigrateDisplacedEmptyRecoveryAfterLastFile(t *testing.T) {
	root := t.TempDir()
	t.Chdir(t.TempDir())
	key := "insert_log/100/10/2001/_data/data.parquet"
	source := filepath.Join(displaced(root), key)
	writeFile(t, source, "complete-data")
	// Existing canonical files force a recursive merge instead of a subtree rename.
	writeFile(t, filepath.Join(root, "insert_log/100/10/2001/_data/other.parquet"), "canonical-data")
	var originalRemove func(*os.Root, string) error
	patch := mockey.Mock((*os.Root).Remove).Origin(&originalRemove).To(func(opened *os.Root, name string) error {
		if filepath.Join(opened.Name(), name) == filepath.Dir(source) {
			return unix.EIO
		}
		return originalRemove(opened, name)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.ErrorIs(t, err, unix.EIO)
	require.Equal(t, 1, report.Renamed)
	require.NoFileExists(t, source)
	require.Equal(t, "complete-data", readFile(t, filepath.Join(root, key)))
	patch.UnPatch()

	report, err = Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
	require.Empty(t, report.Candidates)
	require.Zero(t, report.Renamed)
	require.NoDirExists(t, filepath.Join(displaced(root), "insert_log"))
	require.Equal(t, "canonical-data", readFile(t, filepath.Join(root, "insert_log/100/10/2001/_data/other.parquet")))

	// Once repaired, another startup must not traverse the old empty skeleton.
	walk := mockey.Mock(fs.WalkDir).To(func(fs.FS, string, fs.WalkDirFunc) error {
		t.Fatal("repaired startup must not walk missing legacy layouts")
		return nil
	}).Build()
	t.Cleanup(func() { walk.UnPatch() })
	_, err = Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.NoError(t, err)
}

func TestMigrateDisplacedEmptyPreservesActiveNamespace(t *testing.T) {
	root := t.TempDir()
	t.Chdir(t.TempDir())
	empty := filepath.Join(displaced(root), "insert_log/1/2/3")
	require.NoError(t, os.MkdirAll(empty, 0o755))
	legacyPrefix, err := filepath.Rel(root, displaced(root))
	require.NoError(t, err)
	_, err = Migrate(t.Context(), root, Options{LegacyPrefix: legacyPrefix})
	require.NoError(t, err)
	require.DirExists(t, empty)
}

func TestMigrateDisplacedEmptyWaitsForSuccessfulMigration(t *testing.T) {
	root := t.TempDir()
	t.Chdir(t.TempDir())
	empty := filepath.Join(displaced(root), "text_log/1/2/3")
	require.NoError(t, os.MkdirAll(empty, 0o755))
	key := "insert_log/1/2/3/data.parquet"
	writeFile(t, filepath.Join(displaced(root), key), "source")
	writeFile(t, filepath.Join(root, key), "conflicting-target")
	_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
	require.Error(t, err)
	require.DirExists(t, empty)
}
