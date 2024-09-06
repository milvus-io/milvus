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
	"fmt"
	"path/filepath"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type FileType int

const (
	Invalid FileType = 0
	JSON    FileType = 1
	Numpy   FileType = 2
	Parquet FileType = 3
	CSV     FileType = 4

	JSONFileExt    = ".json"
	NumpyFileExt   = ".npy"
	ParquetFileExt = ".parquet"
	CSVFileExt     = ".csv"
)

var FileTypeName = map[int]string{
	0: "Invalid",
	1: "JSON",
	2: "Numpy",
	3: "Parquet",
	4: "CSV",
}

func (f FileType) String() string {
	return FileTypeName[int(f)]
}

func GetFileType(file *internalpb.ImportFile) (FileType, error) {
	if len(file.GetPaths()) == 0 {
		return Invalid, merr.WrapErrImportFailed("no file to import")
	}
	exts := lo.Map(file.GetPaths(), func(path string, _ int) string {
		return filepath.Ext(path)
	})

	ext := exts[0]
	for i := 1; i < len(exts); i++ {
		if exts[i] != ext {
			return Invalid, merr.WrapErrImportFailed(
				fmt.Sprintf("inconsistency in file types, (%s) vs (%s)",
					file.GetPaths()[0], file.GetPaths()[i]))
		}
	}

	switch ext {
	case JSONFileExt:
		if len(file.GetPaths()) != 1 {
			return Invalid, merr.WrapErrImportFailed("for JSON import, accepts only one file")
		}
		return JSON, nil
	case NumpyFileExt:
		return Numpy, nil
	case ParquetFileExt:
		if len(file.GetPaths()) != 1 {
			return Invalid, merr.WrapErrImportFailed("for Parquet import, accepts only one file")
		}
		return Parquet, nil
	case CSVFileExt:
		if len(file.GetPaths()) != 1 {
			return Invalid, merr.WrapErrImportFailed("for CSV import, accepts only one file")
		}
		return CSV, nil
	}
	return Invalid, merr.WrapErrImportFailed(fmt.Sprintf("unexpected file type, files=%v", file.GetPaths()))
}
