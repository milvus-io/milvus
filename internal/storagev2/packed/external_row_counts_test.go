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

//go:build dynamic

package packed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// Exercise the real native footer reader, including empty and multi-row-group
// files, rather than mocking the producer of the planner's row estimates.
func TestFetchExternalFileRowCountsRealParquet(t *testing.T) {
	dir := t.TempDir()
	want := []int64{0, 1, 37, 2049}
	files := make([]FileInfo, 0, len(want))
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	for i, rows := range want {
		filePath := filepath.Join(dir, fmt.Sprintf("part-%d.parquet", i))
		func() {
			out, err := os.Create(filePath)
			require.NoError(t, err)
			defer out.Close()
			writer, err := pqarrow.NewFileWriter(schema, out,
				parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(4)), pqarrow.DefaultWriterProps())
			require.NoError(t, err)
			builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			defer builder.Release()
			ids := builder.Field(0).(*array.Int64Builder)
			for id := int64(0); id < rows; id++ {
				ids.Append(id)
			}
			record := builder.NewRecord()
			defer record.Release()
			require.NoError(t, writer.Write(record))
			require.NoError(t, writer.Close())
		}()
		files = append(files, FileInfo{FilePath: filePath, NumRows: -1})
	}
	cfg := &indexpb.StorageConfig{StorageType: "local", BucketName: dir, RootPath: dir}
	got, err := FetchExternalFileRowCounts(context.Background(), "parquet", files, cfg, ExternalSpecContext{})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.EqualValues(t, -1, files[0].NumRows, "resolver must not mutate Explore metadata")
}
