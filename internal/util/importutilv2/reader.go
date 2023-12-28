// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importutilv2

import (
	"context"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/importutilv2/json"
	"github.com/milvus-io/milvus/internal/util/importutilv2/numpy"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

//go:generate mockery --name=Reader --structname=MockReader --output=./  --filename=mock_reader.go --with-expecter --inpackage
type Reader interface {
	Next(count int64) (*storage.InsertData, error)
	Close()
}

type ColumnReader interface {
	Next(count int64) (storage.FieldData, error)
	Close()
}

func NewReader(cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	importFile *milvuspb.ImportFile,
	options Options,
) (Reader, error) {
	//schema, err := GetSchemaWithoutAutoID(schema)
	//if err != nil {
	//	return nil, err
	//}
	if IsBackup(options) {
		tsStart, tsEnd, err := ParseTimeRange(options)
		if err != nil {
			return nil, err
		}
		paths := importFile.GetColumnBasedFile().GetFiles()
		return binlog.NewReader(cm, schema, paths, tsStart, tsEnd)
	}

	fileType, paths, err := GetFileTypeAndPaths(importFile)
	if err != nil {
		return nil, err
	}
	switch fileType {
	case JSON:
		reader, err := cm.Reader(context.Background(), paths[0])
		if err != nil {
			return nil, WrapReadFileError(paths[0], err)
		}
		return json.NewReader(reader, schema)
	case Numpy:
		readers, err := CreateReaders(paths, cm, schema)
		if err != nil {
			return nil, err
		}
		return numpy.NewReader(schema, readers)
	case Parquet:
		return nil, merr.ErrServiceUnimplemented
	}
	return nil, merr.WrapErrImportFailed("unexpected import file")
}
