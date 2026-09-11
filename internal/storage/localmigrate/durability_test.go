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
	"os"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestMigrateCopySyncsDestinationAncestorsBeforeSourceRemoval(t *testing.T) {
	for _, publication := range []string{"fresh", "resumed"} {
		t.Run(publication, func(t *testing.T) {
			root, cwd, key := setupDurabilityCopy(t, publication == "resumed")
			expected := destinationAncestorPaths(root, key)
			var synced []string
			var originalSync func(*os.File) error
			syncPatch := mockey.Mock((*os.File).Sync).Origin(&originalSync).To(func(file *os.File) error {
				if err := originalSync(file); err != nil {
					return err
				}
				for _, directory := range expected {
					if filepath.Clean(file.Name()) == directory {
						synced = append(synced, directory)
					}
				}
				return nil
			}).Build()
			defer syncPatch.UnPatch()
			var originalRemove func(*os.Root, string) error
			removed := 0
			removePatch := mockey.Mock((*os.Root).Remove).Origin(&originalRemove).To(func(opened *os.Root, name string) error {
				if filepath.Join(opened.Name(), name) == filepath.Join(cwd, key) {
					require.Equal(t, expected, synced, "persist the complete destination directory chain before unlinking the source")
					removed++
				}
				return originalRemove(opened, name)
			}).Build()
			defer removePatch.UnPatch()
			report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
			require.NoError(t, err)
			require.Equal(t, 1, removed)
			require.Equal(t, 1, report.Copied)
			require.Equal(t, "complete-index", readFile(t, filepath.Join(root, key)))
			require.NoFileExists(t, filepath.Join(cwd, key))
		})
	}
}

func TestMigrateCopyAncestorSyncFailureRetainsSourceAndRetries(t *testing.T) {
	for _, publication := range []string{"fresh", "resumed"} {
		for _, ancestor := range []string{"index_files/8/0/2/3", "index_files/8/0/2", "index_files/8/0", "index_files/8", "index_files", "."} {
			t.Run(publication+"/"+ancestor, func(t *testing.T) {
				root, cwd, key := setupDurabilityCopy(t, publication == "resumed")
				failedDirectory := filepath.Join(root, ancestor)
				failures := 0
				var originalSync func(*os.File) error
				syncPatch := mockey.Mock((*os.File).Sync).Origin(&originalSync).To(func(file *os.File) error {
					if filepath.Clean(file.Name()) == failedDirectory {
						failures++
						return unix.EIO
					}
					return originalSync(file)
				}).Build()
				defer syncPatch.UnPatch()
				_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
				require.ErrorIs(t, err, unix.EIO)
				require.Equal(t, 1, failures)
				require.Equal(t, "complete-index", readFile(t, filepath.Join(cwd, key)))
				target := filepath.Join(root, key)
				require.Equal(t, "complete-index", readFile(t, target))
				markers, err := filepath.Glob(filepath.Join(filepath.Dir(target), ".milvus-local-copy-*"))
				require.NoError(t, err)
				require.Len(t, markers, 1)
				markerInfo, err := os.Stat(markers[0])
				require.NoError(t, err)
				targetInfo, err := os.Stat(target)
				require.NoError(t, err)
				require.True(t, os.SameFile(markerInfo, targetInfo), "retain proof that the complete published target belongs to this source")
				syncPatch.UnPatch()

				copyCalls := 0
				copyPatch := mockey.Mock(io.Copy).To(func(io.Writer, io.Reader) (int64, error) {
					copyCalls++
					return 0, unix.EIO
				}).Build()
				defer copyPatch.UnPatch()
				report, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
				require.NoError(t, err, "retry should finish the already-published copy")
				require.Zero(t, copyCalls)
				require.Equal(t, 1, report.Copied)
				require.Equal(t, "complete-index", readFile(t, target))
				require.NoFileExists(t, filepath.Join(cwd, key))
				require.NoFileExists(t, markers[0])
			})
		}
	}
}

func setupDurabilityCopy(t *testing.T, resumed bool) (root, cwd, key string) {
	t.Helper()
	root, cwd = t.TempDir(), t.TempDir()
	key = "index_files/8/0/2/3/milvus_packed_inverted_index.v3"
	t.Chdir(cwd)
	writeFile(t, filepath.Join(cwd, key), "complete-index")
	injectSourceRenameError(t, filepath.Join(cwd, filepath.Dir(filepath.Dir(key))), unix.EXDEV)
	if resumed {
		var originalRemove func(*os.Root, string) error
		patch := mockey.Mock((*os.Root).Remove).Origin(&originalRemove).To(func(opened *os.Root, name string) error {
			if filepath.Join(opened.Name(), name) == filepath.Join(cwd, key) {
				return unix.EACCES
			}
			return originalRemove(opened, name)
		}).Build()
		defer patch.UnPatch()
		_, err := Migrate(t.Context(), root, Options{LegacyPrefix: "files"})
		require.ErrorIs(t, err, unix.EACCES)
	}
	return root, cwd, key
}

func destinationAncestorPaths(root, key string) []string {
	var directories []string
	for relative := filepath.Dir(key); ; relative = filepath.Dir(relative) {
		directories = append(directories, filepath.Join(root, relative))
		if relative == "." {
			return directories
		}
	}
}
