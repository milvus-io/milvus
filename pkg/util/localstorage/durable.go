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
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WriteFile writes directly to the target path, syncing the file and its
// directory entries before returning success. See Write for root.
func WriteFile(name string, data []byte, perm fs.FileMode, root ...string) error {
	return Write(name, perm, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	}, root...)
}

// Write syncs once after all callback writes. An error can leave an incomplete
// file; callers must not commit metadata referencing a failed write.
//
// root is the provisioned data root that bounds directory synchronization.
// Omitted or empty, there is no provisioned root: the file's parent is its own
// boundary, so only the parent and directories this write creates are synced.
func Write(name string, perm fs.FileMode, write func(io.Writer) error, root ...string) error {
	boundary := parentPath(name)
	if len(root) > 0 && root[0] != "" {
		boundary = root[0]
	}
	boundary, err := PrepareRoot(boundary)
	if err != nil {
		return err
	}
	if err := writeFile(name, perm, write); err != nil {
		return err
	}
	return SyncDirectories(boundary, parentPath(name))
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

// PrepareRoot returns the physical directory synchronization boundary for root.
// Existing roots are caller-provisioned; a missing root is created and
// synchronized through its nearest existing ancestor. An empty root means no
// provisioned root and returns an empty boundary.
func PrepareRoot(root string) (string, error) {
	if root == "" {
		return "", nil
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
	return physicalPath(root)
}

// physicalPath resolves symlinks before '..', as the kernel does, and anchors a
// relative path at the working directory without lexically cleaning it first.
// The working directory's own symlinks are resolved too, so a relative path
// and an absolute root name the same physical directory.
func physicalPath(name string) (string, error) {
	if !filepath.IsAbs(name) {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		name = wd + string(filepath.Separator) + name
	}
	return filepath.EvalSymlinks(name)
}

// SyncDirectories persists dir and each ancestor up to root, deepest first, by
// physical path. Existing directories are not skipped: another writer or GC may
// have just created them. An empty root is no provisioned boundary: only dir is
// synced. The root and external symlinks leading to it must be durable and
// stable; GC may recreate descendants, but must not remove the root.
func SyncDirectories(root, dir string) error {
	current, err := physicalPath(dir)
	if err != nil {
		return err
	}
	boundary := current
	if root != "" {
		if boundary, err = physicalPath(root); err != nil {
			return err
		}
	}
	relative, err := filepath.Rel(boundary, current)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		// A Milvus path/configuration fault, not request content: System class.
		return merr.WrapErrIoFailedMsg("directory %s is outside durability root %s", dir, root)
	}
	for {
		if err := syncDirectory(current); err != nil {
			return err
		}
		if current == boundary {
			return nil
		}
		current = filepath.Dir(current)
	}
}

func syncDirectory(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	err = f.Sync()
	return errors.CombineErrors(err, f.Close())
}
