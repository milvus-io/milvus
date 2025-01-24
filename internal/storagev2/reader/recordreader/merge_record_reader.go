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

package recordreader

import (
	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus/internal/storagev2/file/fragment"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
	"github.com/milvus-io/milvus/internal/storagev2/storage/schema"
)

type MergeRecordReader struct {
	ref             int64
	schema          *schema.Schema
	options         *options.ReadOptions
	fs              fs.Fs
	scalarFragments fragment.FragmentVector
	vectorFragments fragment.FragmentVector
	deleteFragments fragment.DeleteFragmentVector
	record          arrow.Record
}

func (m MergeRecordReader) Retain() {
	// TODO implement me
	panic("implement me")
}

func (m MergeRecordReader) Release() {
	// TODO implement me
	panic("implement me")
}

func (m MergeRecordReader) Schema() *arrow.Schema {
	// TODO implement me
	panic("implement me")
}

func (m MergeRecordReader) Next() bool {
	// TODO implement me
	panic("implement me")
}

func (m MergeRecordReader) Record() arrow.Record {
	// TODO implement me
	panic("implement me")
}

func (m MergeRecordReader) Err() error {
	// TODO implement me
	panic("implement me")
}

func NewMergeRecordReader(
	s *schema.Schema,
	options *options.ReadOptions,
	f fs.Fs,
	scalarFragment fragment.FragmentVector,
	vectorFragment fragment.FragmentVector,
	deleteFragments fragment.DeleteFragmentVector,
) *MergeRecordReader {
	// TODO implement me
	panic("implement me")
}
