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

// Package localstorage implements durable publication of complete local objects.
package localstorage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WriteFile publishes a complete object atomically and persists both its bytes
// and directory entries before returning. Readers keep seeing the old object
// until the replacement is complete, including for repeatedly updated manifests.
func WriteFile(name string, data []byte, perm fs.FileMode) error {
	return Write(name, perm, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	})
}

// Write buffers a complete object in a sibling temporary file. The callback can
// issue arbitrarily many small writes; synchronization happens only at completion.
func Write(name string, perm fs.FileMode, write func(io.Writer) error) error {
	if err := writeAtomic(name, perm, write); err != nil {
		return err
	}
	return SyncDirectories(filepath.Dir(name))
}

// WriteFiles coalesces directory synchronization across a batch of complete
// objects. Every file is synced before publication; common directory ancestors
// are synced once before the batch is acknowledged.
func WriteFiles(files map[string][]byte, perm fs.FileMode) error {
	var errs []error
	dirs := make([]string, 0, len(files))
	for name, data := range files {
		if err := writeAtomic(name, perm, func(w io.Writer) error {
			_, err := w.Write(data)
			return err
		}); err != nil {
			errs = append(errs, err)
		} else {
			dirs = append(dirs, filepath.Dir(name))
		}
	}
	errs = append(errs, SyncDirectories(dirs...))
	return errors.Join(errs...)
}

func writeAtomic(name string, perm fs.FileMode, write func(io.Writer) error) (err error) {
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return err
	}
	// Match os.WriteFile's permission behavior for an existing regular file.
	preserveMode := false
	if info, statErr := os.Stat(name); statErr == nil && info.Mode().IsRegular() {
		perm = info.Mode().Perm()
		preserveMode = true
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return err
	}
	temporary := filepath.Join(dir, ".milvus-tmp-"+hex.EncodeToString(random[:]))
	f, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
		if temporary != "" {
			_ = os.Remove(temporary)
		}
	}()
	if preserveMode {
		if err := f.Chmod(perm); err != nil {
			return err
		}
	}
	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closeErr := f.Close()
	f = nil
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(temporary, name); err != nil {
		return err
	}
	temporary = ""
	return nil
}

// SyncDirectories persists each directory and its ancestors. Existing parents
// can belong to another writer or an interrupted operation, so existence alone
// cannot be used to skip their synchronization. Deduplication is per operation;
// no cache can outlive deletion/recreation of a directory by GC.
func SyncDirectories(dirs ...string) error {
	unique := make(map[string]struct{})
	for _, dir := range dirs {
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		for {
			unique[absolute] = struct{}{}
			parent := filepath.Dir(absolute)
			if parent == absolute {
				break
			}
			absolute = parent
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
	return errors.Join(err, f.Close())
}
