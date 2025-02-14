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

package fragment

import (
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
	"github.com/milvus-io/milvus/internal/storagev2/storage/schema"
)

type (
	pkType               any
	DeleteFragmentVector []DeleteFragment
	DeleteFragment       struct {
		id     int64
		schema *schema.Schema
		fs     fs.Fs
		data   map[pkType][]int64
	}
)

func NewDeleteFragment(id int64, schema *schema.Schema, fs fs.Fs) *DeleteFragment {
	return &DeleteFragment{
		id:     id,
		schema: schema,
		fs:     fs,
		data:   make(map[pkType][]int64),
	}
}

func Make(f fs.Fs, s *schema.Schema, frag Fragment) DeleteFragment {
	// TODO: implement
	panic("implement me")
}
