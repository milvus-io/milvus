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

// Package localstorage publishes local files with object store semantics.
package localstorage

import (
	"crypto/rand"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// tempPrefix names a write that is not yet published. Listings skip it, as
// object stores hide in-progress uploads.
const tempPrefix = ".milvus-tmp-"

// IsTemp reports whether the base name of path marks an unpublished write.
func IsTemp(path string) bool {
	return strings.HasPrefix(filepath.Base(path), tempPrefix)
}

// WriteFileAtomic publishes data at name like an object store PUT. It writes a
// temporary file in the same directory, fsyncs it and renames it onto name, so
// a reader, including one that opened the previous file, sees either the
// previous content or the complete new one. A failed write leaves the
// previous content in place. The parent directory must exist.
//
// After a crash, name holds the previous or the complete new content, and a
// temporary file may remain, which IsTemp identifies. The rename itself is not
// fsynced, so a write that returned success may still be lost.
func WriteFileAtomic(name string, data []byte, perm fs.FileMode) (err error) {
	f, err := createTemp(parentPath(name), perm)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// Best effort: a leftover is hidden from listings like a crash leftover.
			_ = os.Remove(f.Name())
		}
	}()
	if err := writeAndSync(f, data); err != nil {
		return err
	}
	return os.Rename(f.Name(), name)
}

func writeAndSync(f *os.File, data []byte) (err error) {
	defer func() { err = errors.CombineErrors(err, f.Close()) }()
	if _, err := f.Write(data); err != nil {
		return err
	}
	// Persist the content before the rename can publish it.
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

// parentPath preserves symlink/.. semantics, so the temporary file is created
// in the directory the kernel resolves for name: filepath.Dir cleans the path
// before symlinks are resolved, potentially selecting another directory.
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
