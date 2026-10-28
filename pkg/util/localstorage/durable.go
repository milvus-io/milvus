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

// Package localstorage synchronizes completed local file writes.
package localstorage

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WriteFile writes directly to the target path, syncing the file and its
// directory entries before returning success.
func WriteFile(name string, data []byte, perm fs.FileMode, root ...string) error {
	return Write(name, perm, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	}, root...)
}

// Write syncs once after all callback writes. An error can leave an incomplete
// file; callers must not commit metadata referencing a failed write.
func Write(name string, perm fs.FileMode, write func(io.Writer) error, root ...string) error {
	boundary, err := prepareRoot(name, root)
	if err != nil {
		return err
	}
	if err := writeFile(name, perm, write); err != nil {
		return err
	}
	return SyncDirectories(boundary, parentPath(name))
}

// WriteFiles coalesces directory synchronization across a batch of complete
// objects. Every completed file is synced; common directory ancestors
// are synced once before the batch is acknowledged.
func WriteFiles(files map[string][]byte, perm fs.FileMode, root string) error {
	if len(files) == 0 {
		return nil
	}
	var boundary string
	for name := range files {
		var err error
		boundary, err = prepareRoot(name, []string{root})
		if err != nil {
			return err
		}
		break
	}
	var errs error
	dirs := make([]string, 0, len(files))
	for name, data := range files {
		if err := writeFile(name, perm, func(w io.Writer) error {
			_, err := w.Write(data)
			return err
		}); err != nil {
			errs = errors.CombineErrors(errs, err)
		} else {
			dirs = append(dirs, parentPath(name))
		}
	}
	return errors.CombineErrors(errs, SyncDirectories(boundary, dirs...))
}

func writeFile(name string, perm fs.FileMode, write func(io.Writer) error) (err error) {
	if err := os.MkdirAll(parentPath(name), 0o777); err != nil {
		return err
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer func() { err = errors.CombineErrors(err, f.Close()) }()
	if err := write(f); err != nil {
		return err
	}
	return f.Sync()
}

// SyncFileDirectory syncs the physical parent without lexically cleaning name.
func SyncFileDirectory(root, name string) error {
	return SyncDirectories(root, parentPath(name))
}

// parentPath preserves symlink/.. semantics: filepath.Dir cleans the path
// before the kernel has resolved symlinks, potentially selecting another parent.
func parentPath(name string) string {
	end := strings.LastIndexByte(name, filepath.Separator)
	if end < 0 {
		return "."
	}
	if end == 0 {
		return string(filepath.Separator)
	}
	return name[:end]
}

// prepareRoot uses an explicit provisioned data root for storage managers.
// Standalone file utilities use the nearest existing, provisioned directory.
// The root and external symlinks leading to it must be durable and stable;
// GC may recreate descendants, but must not remove this boundary.
func prepareRoot(name string, roots []string) (string, error) {
	root := parentPath(name)
	if len(roots) > 0 && roots[0] != "" {
		root = roots[0]
	}
	return PrepareRoot(root)
}

// PrepareRoot establishes the directory synchronization boundary once when a
// local manager is initialized. Existing roots are caller-provisioned; missing
// roots are created and synchronized through their existing ancestor.
func PrepareRoot(root string) (string, error) {
	if root == "" {
		root = "."
	}
	ancestor := root
	for {
		info, err := os.Stat(ancestor)
		if err == nil {
			if !info.IsDir() {
				return "", &os.PathError{Op: "mkdir", Path: ancestor, Err: errors.New("not a directory")}
			}
			break
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		ancestor = parentPath(strings.TrimRight(ancestor, string(filepath.Separator)))
	}
	if err := os.MkdirAll(root, 0o777); err != nil {
		return "", err
	}
	// Persist creation during initialization, including the link in the existing
	// ancestor. Steady writes never walk outside the provisioned data root.
	if ancestor != root {
		if err := SyncDirectories(ancestor, root); err != nil {
			return "", err
		}
	}
	physical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	return filepath.Abs(physical)
}

// SyncDirectories persists each physical directory through the data root.
// Do not skip existing descendants: they may have been recreated by another
// writer or GC. Deduplication is per batch and never survives directory removal.
func SyncDirectories(root string, dirs ...string) error {
	physicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	boundary, err := filepath.Abs(physicalRoot)
	if err != nil {
		return err
	}
	unique := make(map[string]struct{})
	for _, dir := range dirs {
		physical, err := filepath.EvalSymlinks(dir)
		if err != nil {
			return err
		}
		absolute, err := filepath.Abs(physical)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(boundary, absolute)
		if err != nil {
			return err
		}
		if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return merr.WrapErrParameterInvalidMsg("directory %s is outside durability root %s", dir, root)
		}
		for {
			unique[absolute] = struct{}{}
			if absolute == boundary {
				break
			}
			absolute = filepath.Dir(absolute)
		}
	}
	ordered := make([]string, 0, len(unique))
	for dir := range unique {
		ordered = append(ordered, dir)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return strings.Count(ordered[i], string(filepath.Separator)) > strings.Count(ordered[j], string(filepath.Separator))
	})
	for _, dir := range ordered {
		if err := syncDirectory(dir); err != nil {
			return err
		}
	}
	return nil
}

func syncDirectory(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	err = f.Sync()
	return errors.CombineErrors(err, f.Close())
}
