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
	"github.com/milvus-io/milvus/internal/storagev2/io/fs/file"
)

type MemoryFs struct {
	files map[string]*file.MemoryFile
}

func (m *MemoryFs) MkdirAll(dir string, i int) error {
	// TODO implement me
	panic("implement me")
}

func (m *MemoryFs) List(path string) ([]FileEntry, error) {
	// TODO implement me
	panic("implement me")
}

func (m *MemoryFs) OpenFile(path string) (file.File, error) {
	if f, ok := m.files[path]; ok {
		return file.NewMemoryFile(f.Bytes()), nil
	}
	f := file.NewMemoryFile(nil)
	m.files[path] = f
	return f, nil
}

func (m *MemoryFs) Rename(path string, path2 string) error {
	if _, ok := m.files[path]; !ok {
		return nil
	}
	m.files[path2] = m.files[path]
	delete(m.files, path)
	return nil
}

func (m *MemoryFs) DeleteFile(path string) error {
	delete(m.files, path)
	return nil
}

func (m *MemoryFs) CreateDir(path string) error {
	return nil
}

func (m *MemoryFs) ReadFile(path string) ([]byte, error) {
	panic("implement me")
}

func (m *MemoryFs) Exist(path string) (bool, error) {
	panic("not implemented")
}

func (m *MemoryFs) Path() string {
	panic("not implemented")
}

func NewMemoryFs() *MemoryFs {
	return &MemoryFs{
		files: make(map[string]*file.MemoryFile),
	}
}
