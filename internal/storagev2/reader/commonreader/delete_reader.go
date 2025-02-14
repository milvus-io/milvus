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

package commonreader

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"

	"github.com/milvus-io/milvus/internal/storagev2/file/fragment"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
	"github.com/milvus-io/milvus/internal/storagev2/storage/schema"
)

type DeleteReader struct {
	recordReader    array.RecordReader
	schemaOptions   *schema.SchemaOptions
	deleteFragments fragment.DeleteFragmentVector
	options         *options.ReadOptions
}

func (d DeleteReader) Retain() {
	// TODO implement me
	panic("implement me")
}

func (d DeleteReader) Release() {
	// TODO implement me
	panic("implement me")
}

func (d DeleteReader) Schema() *arrow.Schema {
	// TODO implement me
	panic("implement me")
}

func (d DeleteReader) Next() bool {
	// TODO implement me
	panic("implement me")
}

func (d DeleteReader) Record() arrow.Record {
	// TODO implement me
	panic("implement me")
}

func (d DeleteReader) Err() error {
	// TODO implement me
	panic("implement me")
}

func NewDeleteReader(recordReader array.RecordReader, schemaOptions *schema.SchemaOptions, deleteFragments fragment.DeleteFragmentVector, options *options.ReadOptions) *DeleteReader {
	return &DeleteReader{recordReader: recordReader, schemaOptions: schemaOptions, deleteFragments: deleteFragments, options: options}
}
