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

	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

type Factory struct{}

func (f *Factory) Create(fsType options.FsType, uri *url.URL) (Fs, error) {
	switch fsType {
	case options.InMemory:
		return NewMemoryFs(), nil
	case options.LocalFS:
		return NewLocalFs(uri), nil
	case options.S3:
		return NewMinioFs(uri)
	default:
		panic("unknown fs type")
	}
}

func NewFsFactory() *Factory {
	return &Factory{}
}
