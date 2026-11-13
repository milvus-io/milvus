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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestResolveSourceStorageDefault(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	config := &indexpb.StorageConfig{BucketName: "destination"}
	sourceCM, sourceConfig, err := ResolveSourceStorage(context.Background(), nil, cm, config)
	require.NoError(t, err)
	require.Same(t, cm, sourceCM)
	require.Same(t, config, sourceConfig)
}

func TestExternalImportReader(t *testing.T) {
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

	for name, data := range map[string][]byte{
		"nested//../data.json": []byte(`{"rows":[{"pk":42},{"pk":43}]}`),
		"data.parquet":         parquetData.Bytes(),
		"data.csv":             []byte("pk\n42\n43\n"),
		"data.jsonl":           []byte("{\"pk\":42}\n{\"pk\":43}\n"),
		"pk.npy":               numpyData.Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// The source endpoint must never receive writes or destination credentials.
				assert.Contains(t, r.Header.Get("Authorization"), "external-access-key/")
				assert.NotContains(t, r.Header.Get("Authorization"), "destination-key")
				if r.Method != http.MethodHead && r.Method != http.MethodGet {
					t.Errorf("unexpected source operation: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusForbidden)
					return
				}
				switch r.URL.Path {
				case "/external-bucket/external_folder/" + name:
					http.ServeContent(w, r, name, time.Unix(1, 0), bytes.NewReader(data))
				default:
					t.Errorf("unexpected source path: %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			options := externalStorageOptions()
			options[1].Value = strings.TrimPrefix(server.URL, "http://")
			options = append(options, &commonpb.KeyValuePair{Key: StorageUseSSL, Value: "false"},
				&commonpb.KeyValuePair{Key: StorageRegion, Value: "us-east-1"})
			// No mock expectations: any access to destination storage fails the test.
			cm := mocks.NewChunkManager(t)
			destination := &indexpb.StorageConfig{
				BucketName: "destination", RootPath: "milvus",
				AccessKeyID: "destination-key", SecretAccessKey: "destination-secret", UseIAM: true,
			}
			before := proto.Clone(destination)
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}}
			reader, err := NewReader(context.Background(), cm, schema,
				&internalpb.ImportFile{Paths: []string{"external_folder/" + name}}, options, 1024*1024, destination, 0)
			require.NoError(t, err)
			defer reader.Close()
			size, err := reader.Size()
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), size)
			rows, err := reader.Read()
			require.NoError(t, err)
			require.Equal(t, 2, rows.GetRowNum())
			require.Equal(t, int64(42), rows.Data[100].GetRow(0))
			require.Equal(t, int64(43), rows.Data[100].GetRow(1))
			_, err = reader.Read()
			require.ErrorIs(t, err, io.EOF)
			require.True(t, proto.Equal(before, destination))
		})
	}
}

func TestExternalImportSourceFailures(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name   string
		status int
		code   string
		want   error
	}{
		{"missing object", http.StatusNotFound, "NoSuchKey", merr.ErrIoKeyNotFound},
		{"missing bucket", http.StatusNotFound, "NoSuchBucket", merr.ErrIoBucketNotFound},
		{"access denied", http.StatusForbidden, "AccessDenied", merr.ErrIoPermissionDenied},
		{"invalid credentials", http.StatusForbidden, "InvalidAccessKeyId", merr.ErrIoPermissionDenied},
		{"malformed JSON", http.StatusOK, "", merr.ErrImportFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/external-bucket/data.json", r.URL.Path)
				assert.Contains(t, []string{http.MethodHead, http.MethodGet}, r.Method)
				if tc.status == http.StatusOK {
					http.ServeContent(w, r, "data.json", time.Unix(1, 0), strings.NewReader("malformed-json"))
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				// HEAD has no XML body. Preserve the MinIO error code in its
				// response header when the reader probes the object size first.
				w.Header().Set("X-Minio-Error-Code", tc.code)
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, "<Error><Code>%s</Code><Message>source read failed</Message></Error>", tc.code)
			}))
			defer server.Close()
			options := externalStorageOptions()
			options[1].Value = strings.TrimPrefix(server.URL, "http://")
			options = append(options, &commonpb.KeyValuePair{Key: StorageUseSSL, Value: "false"},
				&commonpb.KeyValuePair{Key: StorageRegion, Value: "us-east-1"})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reader, err := NewReader(ctx, mocks.NewChunkManager(t),
				&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				}}, &internalpb.ImportFile{Paths: []string{"data.json"}}, options, 1024, nil, 0)
			require.Nil(t, reader)
			require.ErrorIs(t, err, tc.want)
			require.NotContains(t, err.Error(), "external-secret-key")
			require.NotContains(t, err.Error(), "external-access-key")
		})
	}
}

func TestExternalImportTransientSourceFailure(t *testing.T) {
	paramtable.Init()
	var requests atomic.Int32
	data := []byte(`[{"pk":42}]`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/external-bucket/data.json", r.URL.Path)
		assert.Contains(t, r.Header.Get("Authorization"), "external-access-key/")
		assert.Contains(t, []string{http.MethodHead, http.MethodGet}, r.Method)
		if r.Method == http.MethodGet && requests.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "<Error><Code>SlowDown</Code><Message>retry later</Message></Error>")
			return
		}
		http.ServeContent(w, r, "data.json", time.Unix(1, 0), bytes.NewReader(data))
	}))
	defer server.Close()
	options := externalStorageOptions()
	options[1].Value = strings.TrimPrefix(server.URL, "http://")
	options = append(options, &commonpb.KeyValuePair{Key: StorageUseSSL, Value: "false"},
		&commonpb.KeyValuePair{Key: StorageRegion, Value: "us-east-1"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reader, err := NewReader(ctx, mocks.NewChunkManager(t),
		&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}}, &internalpb.ImportFile{Paths: []string{"data.json"}}, options, 1024, nil, 0)
	require.NoError(t, err)
	defer reader.Close()
	rows, err := reader.Read()
	require.NoError(t, err)
	require.Equal(t, int64(42), rows.Data[100].GetRow(0))
	require.GreaterOrEqual(t, requests.Load(), int32(2))
}

func TestExternalImportSourceCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cm, config, err := ResolveSourceStorage(ctx, externalStorageOptions(), mocks.NewChunkManager(t), nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, cm)
	require.Nil(t, config)
}
