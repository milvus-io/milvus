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

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	JSONFileExt    = ".json"
	JSONLFileExt   = ".jsonl"
	NDJSONFileExt  = ".ndjson"
	NumpyFileExt   = ".npy"
	ParquetFileExt = ".parquet"
	CSVFileExt     = ".csv"
)

func isJSONLinesType(ft string) bool {
	return ft == JSONLFileExt || ft == NDJSONFileExt
}

func GetFileType(file *internalpb.ImportFile) (datapb.ImportFileType, error) {
	if len(file.GetPaths()) == 0 {
		return datapb.ImportFileType_Unspecified, merr.WrapErrImportFailed("no file to import")
	}
	exts := lo.Map(file.GetPaths(), func(path string, _ int) string {
		return filepath.Ext(path)
	})

	ext := exts[0]
	for i := 1; i < len(exts); i++ {
		// *.jsonl equals *.ndjson
		if isJSONLinesType(exts[i]) && isJSONLinesType(ext) {
			continue
		}
		if exts[i] != ext {
			return datapb.ImportFileType_Unspecified, merr.WrapErrImportFailed(
				fmt.Sprintf("inconsistency in file types, (%s) vs (%s)",
					file.GetPaths()[0], file.GetPaths()[i]))
		}
	}

	switch ext {
	case JSONFileExt, JSONLFileExt, NDJSONFileExt:
		if len(file.GetPaths()) != 1 {
			return datapb.ImportFileType_Unspecified, merr.WrapErrImportFailed("for JSON import, accepts only one file")
		}
		if isJSONLinesType(ext) {
			return datapb.ImportFileType_JsonLines, nil
		} else {
			return datapb.ImportFileType_Json, nil
		}
	case NumpyFileExt:
		return datapb.ImportFileType_Numpy, nil
	case ParquetFileExt:
		if len(file.GetPaths()) != 1 {
			return datapb.ImportFileType_Unspecified, merr.WrapErrImportFailed("for Parquet import, accepts only one file")
		}
		return datapb.ImportFileType_Parquet, nil
	case CSVFileExt:
		if len(file.GetPaths()) != 1 {
			return datapb.ImportFileType_Unspecified, merr.WrapErrImportFailed("for CSV import, accepts only one file")
		}
		return datapb.ImportFileType_Csv, nil
	}
	return datapb.ImportFileType_Unspecified, merr.WrapErrImportFailedMsg("unexpected file type, files=%v", file.GetPaths())
}
