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
	"github.com/apache/arrow/go/v12/arrow/array"

	"github.com/milvus-io/milvus/internal/storagev2/file/fragment"
	"github.com/milvus-io/milvus/internal/storagev2/filter"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
	"github.com/milvus-io/milvus/internal/storagev2/storage/manifest"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
	"github.com/milvus-io/milvus/internal/storagev2/storage/schema"
)

func MakeRecordReader(
	m *manifest.Manifest,
	s *schema.Schema,
	f fs.Fs,
	deleteFragments fragment.DeleteFragmentVector,
	options *options.ReadOptions,
) array.RecordReader {
	relatedColumns := make([]string, 0)
	relatedColumns = append(relatedColumns, options.Columns...)

	for _, filter := range options.Filters {
		relatedColumns = append(relatedColumns, filter.GetColumnName())
	}

	scalarData := m.GetScalarFragments()
	vectorData := m.GetVectorFragments()

	onlyScalar := onlyContainScalarColumns(s, relatedColumns)
	onlyVector := onlyContainVectorColumns(s, relatedColumns)

	if onlyScalar || onlyVector {
		var dataFragments fragment.FragmentVector
		if onlyScalar {
			dataFragments = scalarData
		} else {
			dataFragments = vectorData
		}
		return NewScanRecordReader(s, options, f, dataFragments, deleteFragments)
	}
	if len(options.Filters) > 0 && filtersOnlyContainPKAndVersion(s, options.FiltersV2) {
		return NewMergeRecordReader(s, options, f, scalarData, vectorData, deleteFragments)
	}
	return NewFilterQueryReader(s, options, f, scalarData, vectorData, deleteFragments)
}

func onlyContainVectorColumns(schema *schema.Schema, relatedColumns []string) bool {
	for _, column := range relatedColumns {
		if schema.Options().VectorColumn != column && schema.Options().PrimaryColumn != column && schema.Options().VersionColumn != column {
			return false
		}
	}
	return true
}

func onlyContainScalarColumns(schema *schema.Schema, relatedColumns []string) bool {
	for _, column := range relatedColumns {
		if schema.Options().VectorColumn == column {
			return false
		}
	}
	return true
}

func filtersOnlyContainPKAndVersion(s *schema.Schema, filters []filter.Filter) bool {
	for _, f := range filters {
		if f.GetColumnName() != s.Options().PrimaryColumn &&
			f.GetColumnName() != s.Options().VersionColumn {
			return false
		}
	}
	return true
}

func MakeScanDeleteReader(manifest *manifest.Manifest, fs fs.Fs) array.RecordReader {
	return NewMultiFilesSequentialReader(fs, manifest.GetDeleteFragments(), manifest.GetSchema().DeleteSchema(), options.NewReadOptions())
}
