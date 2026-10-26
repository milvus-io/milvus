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
)

// WriteFile writes directly to the target path, syncing the file and its
// directory entries before returning success.
func WriteFile(name string, data []byte, perm fs.FileMode) error {
	return Write(name, perm, func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	})
}

// Write syncs once after all callback writes. An error can leave an incomplete
// file; callers must not commit metadata referencing a failed write.
func Write(name string, perm fs.FileMode, write func(io.Writer) error) error {
	if err := writeFile(name, perm, write); err != nil {
		return err
	}
	return SyncDirectories(filepath.Dir(name))
}

// WriteFiles coalesces directory synchronization across a batch of complete
// objects. Every completed file is synced; common directory ancestors
// are synced once before the batch is acknowledged.
func WriteFiles(files map[string][]byte, perm fs.FileMode) error {
	var errs error
	dirs := make([]string, 0, len(files))
	for name, data := range files {
		if err := writeFile(name, perm, func(w io.Writer) error {
			_, err := w.Write(data)
			return err
		}); err != nil {
			errs = errors.CombineErrors(errs, err)
		} else {
			dirs = append(dirs, filepath.Dir(name))
		}
	}
	return errors.CombineErrors(errs, SyncDirectories(dirs...))
}

func writeFile(name string, perm fs.FileMode, write func(io.Writer) error) (err error) {
	if err := os.MkdirAll(filepath.Dir(name), 0o777); err != nil {
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
	return errors.CombineErrors(err, f.Close())
}
