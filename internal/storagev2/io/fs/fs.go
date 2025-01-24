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

type Fs interface {
	OpenFile(path string) (file.File, error)
	Rename(src string, dst string) error
	DeleteFile(path string) error
	CreateDir(path string) error
	List(path string) ([]FileEntry, error)
	ReadFile(path string) ([]byte, error)
	Exist(path string) (bool, error)
	Path() string
	MkdirAll(dir string, i int) error
}
type FileEntry struct {
	Path string
}
