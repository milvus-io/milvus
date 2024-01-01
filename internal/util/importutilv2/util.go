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
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"io"
	"path/filepath"
	"strings"
)

type FileType int

const (
	JSON    FileType = 0
	Numpy   FileType = 1
	Parquet FileType = 2

	JSONFileExt    = ".json"
	NumpyFileExt   = ".npy"
	ParquetFileExt = ".parquet"
)

var FileTypeName = map[int]string{
	0: "JSON",
	1: "Numpy",
	2: "Parquet",
}

func (f FileType) String() string {
	return FileTypeName[int(f)]
}

func WrapIllegalFileTypeError(file string) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("unrecognized file type with name %s", file))
}

func WrapReadFileError(file string, err error) error {
	return merr.WrapErrImportFailed(fmt.Sprintf("failed to read the file '%s', error: %s", file, err.Error()))
}

func GetFileTypeAndPaths(file *milvuspb.ImportFile) (FileType, []string, error) {
	switch file.GetFile().(type) {
	case *milvuspb.ImportFile_RowBasedFile:
		filePath := file.GetRowBasedFile()
		if filepath.Ext(filePath) != JSONFileExt {
			return 0, nil, WrapIllegalFileTypeError(filePath)
		}
		return JSON, []string{filePath}, nil
	case *milvuspb.ImportFile_ColumnBasedFile:
		paths := file.GetColumnBasedFile().GetFiles()
		if len(paths) == 0 {
			return 0, nil, merr.WrapErrImportFailed("no file to import")
		}
		ext := filepath.Ext(paths[0])
		for i := 1; i < len(paths); i++ {
			if filepath.Ext(paths[i]) != ext {
				return 0, nil, merr.WrapErrImportFailed(
					fmt.Sprintf("inconsistency in file types, (%s) vs (%s)", paths[0], paths[i]))
			}
		}
		if ext == NumpyFileExt {
			return Numpy, paths, nil
		} else if ext == ParquetFileExt {
			return Parquet, paths, nil
		}
		return 0, nil, WrapIllegalFileTypeError(paths[0])
	}
	return 0, nil, merr.WrapErrImportFailed("unexpect file type when import")
}

func CreateReaders(paths []string,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
) (map[int64]io.Reader, error) {
	readers := make(map[int64]io.Reader)
	nameToPath := lo.SliceToMap(paths, func(path string) (string, string) {
		nameWithExt := filepath.Base(path)
		name := strings.TrimSuffix(nameWithExt, filepath.Ext(nameWithExt))
		return name, path
	})
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			continue
		}
		if _, ok := nameToPath[field.GetName()]; !ok {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("no file for field: %s, files: %v", field.GetName(), lo.Values(nameToPath)))
		}
		reader, err := cm.Reader(context.Background(), nameToPath[field.GetName()])
		if err != nil {
			return nil, WrapReadFileError(nameToPath[field.GetName()], err)
		}
		readers[field.GetFieldID()] = reader
	}
	return readers, nil
}

func GetSchemaWithoutAutoID(schema *schemapb.CollectionSchema) (*schemapb.CollectionSchema, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	if !pkField.GetAutoID() {
		return schema, nil
	}
	newSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	newSchema.Fields = lo.Filter(newSchema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		return field.GetFieldID() != pkField.GetFieldID()
	})
	return newSchema, nil
}
