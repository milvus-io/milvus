// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package featureusage

import (
	"bytes"
	"context"
	"fmt"
	"path"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/sbinet/npyio"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	pq "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// importDim is fixed because the numpy writer needs the vector width as a
// compile-time array length.
const importDim = 8

// TestBinaryImportFileTypesAreCounted covers the two import formats the text
// writers cannot produce. Together with the JSON, JSONLines and CSV cases,
// every file type in the catalog is driven by a real accepted import job.
func (s *Suite) TestBinaryImportFileTypesAreCounted() {
	ctx := context.Background()
	name := "fu_binimport_" + funcutil.GenRandomStr()
	schema := s.createImportCollection(ctx, name)
	defer s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})

	for _, tc := range []struct {
		counter string
		files   func() []*internalpb.ImportFile
	}{
		{
			counter: "import_file_type=Parquet",
			files: func() []*internalpb.ImportFile {
				return []*internalpb.ImportFile{{Paths: []string{s.writeParquetImportFile(ctx, schema)}}}
			},
		},
		{
			counter: "import_file_type=Numpy",
			files: func() []*internalpb.ImportFile {
				return []*internalpb.ImportFile{{Paths: s.writeNumpyImportFiles(ctx, schema)}}
			},
		},
	} {
		s.Run(tc.counter, func() {
			before := counters(s.report(ctx), typeutil.MixCoordRole)
			resp, err := s.Cluster.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
				CollectionName: name,
				Files:          tc.files(),
			})
			s.Require().NoError(err)
			s.Require().NoError(merr.Error(resp.GetStatus()))
			after := counters(s.report(ctx), typeutil.MixCoordRole)
			s.Equal(before[tc.counter]+1, after[tc.counter], "%s should have been counted", tc.counter)
		})
	}
}

// createImportCollection builds a collection simple enough that the generic
// row generator can fill every field: a non-auto primary key, one scalar and
// one float vector.
func (s *Suite) createImportCollection(ctx context.Context, name string) *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: false},
			{
				FieldID: 101, Name: "extra", DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
			},
			{
				FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprint(importDim)}},
			},
		},
	}
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: marshaled, ShardsNum: 1,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, status.GetErrorCode(), status.GetReason())
	return schema
}

// writeParquetImportFile writes one parquet file holding a few generated rows.
func (s *Suite) writeParquetImportFile(ctx context.Context, schema *schemapb.CollectionSchema) string {
	const rows = 10
	insertData, err := testutil.CreateInsertData(schema, rows)
	s.Require().NoError(err)

	buf := bytes.NewBuffer(make([]byte, 0, 10240))
	s.Require().NoError(s.serializeParquet(buf, schema, insertData, rows))

	filePath := path.Join(s.Cluster.RootPath(), "fu_import", uuid.New().String()+".parquet")
	s.Require().NoError(s.Cluster.ChunkManager.Write(ctx, filePath, buf.Bytes()))
	return filePath
}

// serializeParquet closes the writer before the caller reads the buffer; the
// footer is written by Close.
func (s *Suite) serializeParquet(buf *bytes.Buffer, schema *schemapb.CollectionSchema, insertData *storage.InsertData, rows int) error {
	pqSchema, err := pq.ConvertToArrowSchemaForUT(schema, false)
	if err != nil {
		return err
	}
	fw, err := pqarrow.NewFileWriter(pqSchema, buf,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(int64(rows))),
		pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer fw.Close()

	columns, err := testutil.BuildArrayData(schema, insertData, false)
	if err != nil {
		return err
	}
	return fw.Write(array.NewRecord(pqSchema, columns, int64(rows)))
}

// writeNumpyImportFiles writes one .npy per field into one directory, which is
// how the numpy reader expects a row set.
func (s *Suite) writeNumpyImportFiles(ctx context.Context, schema *schemapb.CollectionSchema) []string {
	const rows = 10
	insertData, err := testutil.CreateInsertData(schema, rows)
	s.Require().NoError(err)

	dir := path.Join(s.Cluster.RootPath(), "fu_import", uuid.New().String())
	paths := make([]string, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		fieldData := insertData.Data[field.GetFieldID()]
		var payload any
		if field.GetDataType() == schemapb.DataType_FloatVector {
			// npyio needs a fixed-size array element type, not a flat slice.
			flat := fieldData.GetDataRows().([]float32)
			chunks := lo.Chunk(flat, importDim)
			rowsOut := make([][importDim]float32, len(chunks))
			for i, c := range chunks {
				copy(rowsOut[i][:], c)
			}
			payload = rowsOut
		} else {
			payload = fieldData.GetDataRows()
		}

		buf := bytes.NewBuffer(make([]byte, 0, 10240))
		s.Require().NoError(npyio.Write(buf, payload))
		filePath := path.Join(dir, field.GetName()+".npy")
		s.Require().NoError(s.Cluster.ChunkManager.Write(ctx, filePath, buf.Bytes()))
		paths = append(paths, filePath)
	}
	return paths
}
