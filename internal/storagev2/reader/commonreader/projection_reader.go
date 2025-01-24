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

	"github.com/milvus-io/milvus/internal/storagev2/common/utils"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

type ProjectionReader struct {
	array.RecordReader
	reader  array.RecordReader
	options *options.ReadOptions
	schema  *arrow.Schema
}

func NewProjectionReader(reader array.RecordReader, options *options.ReadOptions, schema *arrow.Schema) array.RecordReader {
	projectionSchema := utils.ProjectSchema(schema, options.Columns)
	return &ProjectionReader{reader: reader, options: options, schema: projectionSchema}
}
