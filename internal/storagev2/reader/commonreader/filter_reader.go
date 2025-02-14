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

	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

type FilterReader struct {
	recordReader               array.RecordReader
	option                     *options.ReadOptions
	currentFilteredBatchReader array.RecordReader
}

func (r *FilterReader) Retain() {
	// TODO implement me
	panic("implement me")
}

func (r *FilterReader) Release() {
	// TODO implement me
	panic("implement me")
}

func (r *FilterReader) Schema() *arrow.Schema {
	// TODO implement me
	panic("implement me")
}

func (r *FilterReader) Record() arrow.Record {
	// TODO implement me
	panic("implement me")
}

func (r *FilterReader) Err() error {
	// TODO implement me
	panic("implement me")
}

func MakeFilterReader(recordReader array.RecordReader, option *options.ReadOptions) *FilterReader {
	return &FilterReader{
		recordReader: recordReader,
		option:       option,
	}
}

func (r *FilterReader) Next() bool {
	//for {
	//	if r.currentFilteredBatchReader != nil {
	//		filteredBatch := r.currentFilteredBatchReader.Next()
	//		if err != nil {
	//			return false
	//		}
	//		if filteredBatch == nil {
	//			r.currentFilteredBatchReader = nil
	//			continue
	//		}
	//		return filteredBatch, nil
	//	}
	//	err := r.NextFilteredBatchReader()
	//	if err != nil {
	//		return nil
	//	}
	//	if r.currentFilteredBatchReader == nil {
	//		return nil
	//	}
	//}
	return false
}
