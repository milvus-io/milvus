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

// Package localstorage atomically publishes and synchronizes local file writes.
package localstorage

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// tempPrefix names a write that is not yet published. Listings skip it, as
// object stores hide in-progress uploads.
const tempPrefix = ".milvus-tmp-"

// IsTemp reports whether the base name of path marks an unpublished write.
func IsTemp(path string) bool {
	return strings.HasPrefix(filepath.Base(path), tempPrefix)
}

// WriteFile atomically publishes data at name, syncing the file and its
// directory entries before returning success. See Write for boundary.
func WriteFile(name string, data []byte, perm fs.FileMode, boundary string) error {
	return Write(name, perm, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	}, boundary)
}

// Write publishes name with the semantics of an object store PUT: callback
// writes go to a temporary file in the same directory, which is synced and
// then renamed onto name. Readers, including those that opened or mapped the
// previous file, see either the previous content or the complete new one,
// never a partial file. A failed write leaves the previous content in place.
// A crash may leave a temporary file behind, which IsTemp identifies.
//
// boundary is the physical data root returned by PrepareRoot, prepared once by
// its owner; directory synchronization stops there. Empty, there is no
// provisioned root: the file's parent is prepared as its own boundary, so only
// the parent and directories this write creates are synced.
func Write(name string, perm fs.FileMode, write func(io.Writer) error, boundary string) error {
	if boundary == "" {
		var err error
		if boundary, err = PrepareRoot(parentPath(name)); err != nil {
			return err
		}
	}
	if err := writeFile(name, perm, write); err != nil {
		return err
	}
	// Persist the rename and any directories created for it.
	return SyncDirectories(boundary, parentPath(name))
}

func writeFile(name string, perm fs.FileMode, write func(io.Writer) error) (err error) {
	dir := parentPath(name)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return err
	}
	f, err := createTemp(dir, perm)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// Best effort: a leftover is hidden from listings like a crash leftover.
			_ = os.Remove(f.Name())
		}
	}()
	if err := writeAndSync(f, write); err != nil {
		return err
	}
	return os.Rename(f.Name(), name)
}

func writeAndSync(f *os.File, write func(io.Writer) error) (err error) {
	defer func() { err = errors.CombineErrors(err, f.Close()) }()
	if err := write(f); err != nil {
		return err
	}
	return f.Sync()
}

// createTemp creates a unique file in dir. Unlike os.CreateTemp, it applies
// perm subject to the umask, as os.OpenFile does.
func createTemp(dir string, perm fs.FileMode) (*os.File, error) {
	var suffix [8]byte
	for {
		if _, err := rand.Read(suffix[:]); err != nil {
			return nil, err
		}
		name := dir + string(filepath.Separator) + tempPrefix + hex.EncodeToString(suffix[:])
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
		if !os.IsExist(err) {
			return f, err
		}
	}
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
// synchronized through its nearest existing ancestor.
func PrepareRoot(root string) (string, error) {
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
	boundary, err := physicalPath(root)
	if err != nil {
		return "", err
	}
	// Persist creation during initialization, including the link in the existing
	// ancestor. Steady writes never walk outside the provisioned data root.
	if ancestor != root {
		top, err := physicalPath(ancestor)
		if err != nil {
			return "", err
		}
		if err := SyncDirectories(top, boundary); err != nil {
			return "", err
		}
	}
	return boundary, nil
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

// SyncDirectories persists dir and each ancestor up to boundary, deepest
// first, by physical path. boundary must already be physical (see PrepareRoot).
// Existing directories are not skipped: another writer or GC may have just
// created them. The root and external symlinks leading to it must be durable
// and stable; GC may recreate descendants, but must not remove the root.
func SyncDirectories(boundary, dir string) error {
	current, err := physicalPath(dir)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(boundary, current)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		// A Milvus path/configuration fault, not request content: System class.
		return merr.WrapErrIoFailedMsg("directory %s is outside durability root %s", dir, boundary)
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
