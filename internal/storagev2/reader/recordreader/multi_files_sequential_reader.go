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
	"sync/atomic"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"

	"github.com/milvus-io/milvus/internal/storagev2/common/arrowutil"
	"github.com/milvus-io/milvus/internal/storagev2/file/fragment"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

type MultiFilesSequentialReader struct {
	fs         fs.Fs
	schema     *arrow.Schema
	files      []string
	nextPos    int
	options    *options.ReadOptions
	currReader array.RecordReader
	err        error
	ref        int64
}

func (m *MultiFilesSequentialReader) Retain() {
	atomic.AddInt64(&m.ref, 1)
}

func (m *MultiFilesSequentialReader) Release() {
	if atomic.AddInt64(&m.ref, -1) == 0 {
		if m.currReader != nil {
			m.currReader.Release()
			m.currReader = nil
		}
	}
}

func (m *MultiFilesSequentialReader) Schema() *arrow.Schema {
	return m.schema
}

func (m *MultiFilesSequentialReader) Next() bool {
	for {
		if m.currReader == nil {
			if m.nextPos >= len(m.files) {
				return false
			}

			m.nextReader()
			if m.err != nil {
				return false
			}
			m.nextPos++
		}
		if m.currReader.Next() {
			return true
		}
		if m.currReader.Err() != nil {
			m.err = m.currReader.Err()
			return false
		}
		if m.currReader != nil {
			m.currReader.Release()
			m.currReader = nil
		}
	}
}

func (m *MultiFilesSequentialReader) Record() arrow.Record {
	if m.currReader != nil {
		return m.currReader.Record()
	}
	return nil
}

func (m *MultiFilesSequentialReader) Err() error {
	return m.err
}

func (m *MultiFilesSequentialReader) nextReader() {
	var fileReader *pqarrow.FileReader
	fileReader, m.err = arrowutil.MakeArrowFileReader(m.fs, m.files[m.nextPos])
	if m.err != nil {
		return
	}
	m.currReader, m.err = arrowutil.MakeArrowRecordReader(fileReader, m.options)
}

func NewMultiFilesSequentialReader(fs fs.Fs, fragments fragment.FragmentVector, schema *arrow.Schema, options *options.ReadOptions) *MultiFilesSequentialReader {
	files := make([]string, 0, len(fragments))
	for _, f := range fragments {
		files = append(files, f.Files()...)
	}

	return &MultiFilesSequentialReader{
		fs:      fs,
		schema:  schema,
		options: options,
		files:   files,
		nextPos: 0,
		ref:     1,
	}
}
