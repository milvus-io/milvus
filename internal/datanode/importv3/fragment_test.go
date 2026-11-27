// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importv3

import (
	"context"
	"reflect"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestSourceFromFragmentRejectsMalformedRefs pins every boundary guard of the
// fragment adapter: a nil ref, an empty path, a non-positive row count and a
// missing schema/storage config are all rejected before any object is opened.
func TestSourceFromFragmentRejectsMalformedRefs(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	storageConfig := &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"}

	t.Run("nil ref", func(t *testing.T) {
		_, err := SourceFromFragment(nil, schema, 1, storageConfig, nil)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
	})
	t.Run("empty path", func(t *testing.T) {
		_, err := SourceFromFragment(&importv3pb.FragmentRef{Rows: 3}, schema, 1, storageConfig, nil)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
	})
	t.Run("non-positive rows", func(t *testing.T) {
		_, err := SourceFromFragment(&importv3pb.FragmentRef{Path: "f.parquet", Rows: 0}, schema, 1, storageConfig, nil)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
	})
	t.Run("nil schema", func(t *testing.T) {
		_, err := SourceFromFragment(&importv3pb.FragmentRef{Path: "f.parquet", Rows: 3}, nil, 1, storageConfig, nil)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
	})
	t.Run("nil storage config", func(t *testing.T) {
		_, err := SourceFromFragment(&importv3pb.FragmentRef{Path: "f.parquet", Rows: 3}, schema, 1, nil, nil)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
	})
}

// TestSourceFromFragmentOpensWithExactSpec pins that Source.Open forwards the
// ref path/rows, the temporary schema and the caller's buffer size, storage
// config and plugin context to the packed fragment reader unchanged: the
// reader validates exact rows and every field the schema carries, so a dropped
// argument would silently mis-read the fragment.
func TestSourceFromFragmentOpensWithExactSpec(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	storageConfig := &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"}
	pluginContext := &indexcgopb.StoragePluginContext{CollectionId: 7}
	const bufferSize = int64(1 << 20)
	const rows = int64(3)

	var (
		capturedSpec     storage.ImportFragmentReaderSpec
		capturedSchema   *schemapb.CollectionSchema
		capturedOptions  []storage.RwOption
		readerOpenCalled bool
	)
	readerMock := mockey.Mock(storage.NewImportFragmentRecordReader).To(
		func(_ context.Context, spec storage.ImportFragmentReaderSpec, s *schemapb.CollectionSchema, option ...storage.RwOption) (storage.RecordReader, error) {
			readerOpenCalled = true
			capturedSpec = spec
			capturedSchema = s
			capturedOptions = option
			return &testReader{}, nil
		}).Build()
	defer readerMock.UnPatch()

	source, err := SourceFromFragment(
		&importv3pb.FragmentRef{Path: "fragments/0/10/3_7.parquet", Rows: rows},
		schema, bufferSize, storageConfig, pluginContext,
	)
	require.NoError(t, err)
	require.Equal(t, "fragments/0/10/3_7.parquet", source.ID)
	require.Equal(t, rows, source.Rows)
	require.NotNil(t, source.Open)

	// Open is lazy: nothing is read until it is called.
	require.False(t, readerOpenCalled)
	reader, err := source.Open(context.Background())
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.True(t, readerOpenCalled)
	require.Equal(t, "fragments/0/10/3_7.parquet", capturedSpec.Path)
	require.Equal(t, rows, capturedSpec.Rows)
	require.Same(t, schema, capturedSchema)

	// The three options must carry the caller's exact values. GetStorageConfig
	// reads the storage config; the buffer size and plugin context are read
	// through reflection since the storage package keeps the options struct
	// unexported.
	require.Same(t, storageConfig, storage.GetStorageConfig(capturedOptions...))
	opts := storage.DefaultReaderOptions()
	for _, opt := range capturedOptions {
		opt(opts)
	}
	value := reflect.ValueOf(opts).Elem()
	require.Equal(t, bufferSize, value.FieldByName("bufferSize").Int())
	require.Equal(t, reflect.ValueOf(pluginContext).Pointer(), value.FieldByName("pluginContext").Pointer())
}
