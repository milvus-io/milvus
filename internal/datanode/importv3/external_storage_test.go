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

package importv3

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestExternalStoragePreImportAndReshard(t *testing.T) {
	paramtable.Init()
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{42, 43}, nil)
	values := builder.NewArray()
	defer values.Release()
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "pk", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecord(arrowSchema, []arrow.Array{values}, 2)
	defer record.Release()
	table := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})
	defer table.Release()
	var parquetData bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &parquetData, 1024, nil, pqarrow.DefaultWriterProps()))

	var numpyData bytes.Buffer
	require.NoError(t, npyio.Write(&numpyData, []int64{42, 43}))

	for name, data := range map[string][]byte{"data.parquet": parquetData.Bytes(), "pk.npy": numpyData.Bytes()} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Contains(t, r.Header.Get("Authorization"), "source-key/")
				assert.Equal(t, "/source-bucket/folder/"+name, r.URL.Path)
				if r.Method != http.MethodGet && r.Method != http.MethodHead {
					t.Errorf("unexpected source write: %s", r.Method)
					w.WriteHeader(http.StatusForbidden)
					return
				}
				http.ServeContent(w, r, name, time.Unix(1, 0), bytes.NewReader(data))
			}))
			defer server.Close()
			options := []*commonpb.KeyValuePair{
				{Key: importutilv2.StorageEndpoint, Value: strings.TrimPrefix(server.URL, "http://")},
				{Key: importutilv2.StorageBucket, Value: "source-bucket"},
				{Key: importutilv2.StorageAccessKeyID, Value: "source-key"},
				{Key: importutilv2.StorageSecretAccessKey, Value: "source-secret"},
				{Key: importutilv2.StorageUseSSL, Value: "false"},
				{Key: importutilv2.StorageRegion, Value: "us-east-1"},
			}
			cm := mocks.NewChunkManager(t) // Any destination read fails the test.
			config := &indexpb.StorageConfig{BucketName: "destination", RootPath: "milvus", AccessKeyID: "destination-key", UseIAM: true}
			before := proto.Clone(config)
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}}
			file := &internalpb.ImportFile{Id: 7, Paths: []string{"folder/" + name}}
			task := NewPreImportTask(&datapb.PreImportRequest{
				JobID: 1, TaskID: 2, CollectionID: 3, Schema: schema,
				ImportFiles: []*internalpb.ImportFile{file}, Options: options, StorageConfig: config,
			}, cm)
			result, err := task.Execute(context.Background())
			require.NoError(t, err)
			stats := result.([]*datapb.ImportV3FileStats)
			require.Len(t, stats, 1)
			require.Equal(t, int64(2), stats[0].GetTotalRows())
			require.Equal(t, int64(len(data)), stats[0].GetFileSize())
			require.Same(t, cm, task.(*preImportTask).cm)

			reader, err := newReshardSourceReader(context.Background(), cm, schema, file, options, 1024*1024, config, nil)
			require.NoError(t, err)
			defer reader.Close()
			rows, err := reader.Read()
			require.NoError(t, err)
			require.Equal(t, 2, rows.GetRowNum())
			require.Equal(t, int64(42), rows.Data[100].GetRow(0))
			require.True(t, proto.Equal(before, config))
		})
	}
}
