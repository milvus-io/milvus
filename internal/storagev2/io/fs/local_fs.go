// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fs

import (
	"net/url"
	"os"
	"path/filepath"

	"github.com/milvus-io/milvus/internal/storagev2/common/log"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs/file"
)

type LocalFS struct {
	path string
}

func (l *LocalFS) MkdirAll(dir string, i int) error {
	return os.MkdirAll(dir, os.FileMode(i))
}

func (l *LocalFS) OpenFile(path string) (file.File, error) {
	// Extract the directory from the path
	dir := filepath.Dir(path)
	// Create the directory (including all necessary parent directories)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	open, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	return file.NewLocalFile(open), nil
}

// Rename renames (moves) a file. If newpath already exists and is not a directory, Rename replaces it.
func (l *LocalFS) Rename(src string, dst string) error {
	return os.Rename(src, dst)
}

func (l *LocalFS) DeleteFile(path string) error {
	return os.Remove(path)
}

func (l *LocalFS) CreateDir(path string) error {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		log.Error(err.Error())
	}
	return nil
}

func (l *LocalFS) List(path string) ([]FileEntry, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	ret := make([]FileEntry, 0, len(entries))
	for _, entry := range entries {
		ret = append(ret, FileEntry{Path: filepath.Join(path, entry.Name())})
	}

	return ret, nil
}

func (l *LocalFS) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (l *LocalFS) Exist(path string) (bool, error) {
	panic("not implemented")
}

func (l *LocalFS) Path() string {
	return l.path
}

func NewLocalFs(uri *url.URL) *LocalFS {
	return &LocalFS{uri.Path}
}
