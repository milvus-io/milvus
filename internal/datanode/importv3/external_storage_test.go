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
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/bytedance/mockey"
	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestExternalStoragePreImportAndReshard(t *testing.T) {
	paramtable.Init()
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key) })
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ReshardFlushConcurrency.Key) })
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ReshardFlushConcurrency.Key, "1")
	totalMem := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1) << 40).Build()
	t.Cleanup(func() { totalMem.UnPatch() })
	usedMem := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(0)).Build()
	t.Cleanup(func() { usedMem.UnPatch() })

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
	jsonData := []byte(`[{"pk":42},{"pk":43}]`)

	for name, data := range map[string][]byte{
		"data.parquet":      parquetData.Bytes(),
		"pk.npy":            numpyData.Bytes(),
		"v1:2026/data.json": jsonData,
	} {
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
			file := &internalpb.ImportFile{
				Id:      7,
				Paths:   []string{"folder/" + name},
				IdRange: &commonpb.IDRange{Begin: 1000, End: 1002},
			}
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

			var gotPKs []int64
			writerMock := mockey.Mock(writeFragment).To(
				func(spec *writeSpec, _ *SpillManager, vchannelOrdinal, partitionOrdinal int, input fragmentInput, seq int64) (*fragmentOutcome, error) {
					assert.True(t, proto.Equal(before, spec.req.GetStorageConfig()))
					for _, p := range input.pieces {
						gotPKs = append(gotPKs, p.mem.Data[100].(*storage.Int64FieldData).Data...)
					}
					return &fragmentOutcome{
						descriptor: &importv3pb.FragmentDescriptor{
							VchannelIndex: int32(vchannelOrdinal),
							PartitionId:   spec.plan.GetPartitionIds()[partitionOrdinal],
							Seq:           seq,
							Path:          "frag-0.parquet",
							Rows:          input.rows,
							LogicalBytes:  input.logicalBytes,
						},
						timings:             &storage.SortTimings{},
						writtenUncompressed: uint64(input.logicalBytes),
					}, nil
				}).Build()
			defer writerMock.UnPatch()

			var published *importv3pb.ReshardManifest
			cm.EXPECT().RootPath().Return("milvus").Once()
			cm.EXPECT().Write(mock.Anything, mock.MatchedBy(func(p string) bool {
				return strings.HasPrefix(p, "milvus/")
			}), mock.Anything).RunAndReturn(func(_ context.Context, _ string, payload []byte) error {
				manifest := &importv3pb.ReshardManifest{}
				if err := proto.Unmarshal(payload, manifest); err != nil {
					return err
				}
				published = manifest
				return nil
			}).Once()

			plan := &importv3pb.ReshardTaskPlan{
				CollectionId:     3,
				CollectionSchema: schema,
				Vchannels:        []string{"ch0"},
				PartitionIds:     []int64{10},
				Files:            []*internalpb.ImportFile{file},
				Options:          options,
				FragmentSize:     8 << 20,
			}
			reshardReq := &datapb.ReshardTaskRequest{
				JobId: 1, TaskId: 2, RunId: 3, Slot: 1,
				StorageConfig: config,
				Plan:          plan,
			}
			reshard := NewReshardTask(reshardReq, cm, nil, nil, NewMetrics(1))
			_, err = reshard.Execute(context.Background())
			require.NoError(t, err)
			require.Equal(t, []int64{42, 43}, gotPKs)
			require.NotNil(t, published)
			require.Len(t, published.GetFragments(), 1)
			require.Equal(t, int64(2), published.GetFragments()[0].GetRows())
			require.Same(t, cm, reshard.(*reshardTask).cm)
			require.True(t, proto.Equal(before, config))

			cleanedPlan := proto.Clone(plan).(*importv3pb.ReshardTaskPlan)
			cleanedPlan.Options = importutilv2.RemoveSourceStorageCredentials(options)
			cleanedReshard := NewReshardTask(&datapb.ReshardTaskRequest{
				JobId: 1, TaskId: 2, RunId: 4, Slot: 1,
				StorageConfig: config,
				Plan:          cleanedPlan,
			}, mocks.NewChunkManager(t), nil, nil, NewMetrics(1))
			_, err = cleanedReshard.Execute(context.Background())
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
}

func buildInt64Parquet(t *testing.T, pks []int64) []byte {
	t.Helper()
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(pks, nil)
	values := builder.NewArray()
	defer values.Release()
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "pk", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecord(arrowSchema, []arrow.Array{values}, int64(len(pks)))
	defer record.Release()
	table := array.NewTableFromRecords(arrowSchema, []arrow.Record{record})
	defer table.Release()
	var buf bytes.Buffer
	require.NoError(t, pqarrow.WriteTable(table, &buf, 1024, nil, pqarrow.DefaultWriterProps()))
	return buf.Bytes()
}

func TestExternalStorageWorkerSharesClientAcrossFiles(t *testing.T) {
	paramtable.Init()
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key) })
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ReshardFlushConcurrency.Key) })
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ReshardFlushConcurrency.Key, "1")
	totalMem := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1) << 40).Build()
	t.Cleanup(func() { totalMem.UnPatch() })
	usedMem := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(0)).Build()
	t.Cleanup(func() { usedMem.UnPatch() })

	part0 := buildInt64Parquet(t, []int64{10, 11})
	part1 := buildInt64Parquet(t, []int64{20, 21, 22})
	objects := map[string][]byte{
		"/source-bucket/folder/part-0.parquet": part0,
		"/source-bucket/folder/part-1.parquet": part1,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "source-key/")
		payload, ok := objects[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		http.ServeContent(w, r, "part.parquet", time.Unix(1, 0), bytes.NewReader(payload))
	}))
	defer server.Close()

	var clientBuilds atomic.Int32
	var origNewSourceCM func(context.Context, importutilv2.Options, storage.ChunkManager) (storage.ChunkManager, error)
	sourceCMMock := mockey.Mock(importutilv2.NewSourceChunkManager).Origin(&origNewSourceCM).To(
		func(ctx context.Context, options importutilv2.Options, fallback storage.ChunkManager) (storage.ChunkManager, error) {
			clientBuilds.Add(1)
			return origNewSourceCM(ctx, options, fallback)
		}).Build()
	t.Cleanup(func() { sourceCMMock.UnPatch() })

	options := []*commonpb.KeyValuePair{
		{Key: importutilv2.StorageEndpoint, Value: strings.TrimPrefix(server.URL, "http://")},
		{Key: importutilv2.StorageBucket, Value: "source-bucket"},
		{Key: importutilv2.StorageAccessKeyID, Value: "source-key"},
		{Key: importutilv2.StorageSecretAccessKey, Value: "source-secret"},
		{Key: importutilv2.StorageUseSSL, Value: "false"},
		{Key: importutilv2.StorageRegion, Value: "us-east-1"},
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	config := &indexpb.StorageConfig{BucketName: "destination", RootPath: "milvus", AccessKeyID: "destination-key"}
	files := []*internalpb.ImportFile{
		{Id: 1, Paths: []string{"folder/part-0.parquet"}, IdRange: &commonpb.IDRange{Begin: 1000, End: 1002}},
		{Id: 2, Paths: []string{"folder/part-1.parquet"}, IdRange: &commonpb.IDRange{Begin: 2000, End: 2003}},
	}

	cm := mocks.NewChunkManager(t)
	preTask := NewPreImportTask(&datapb.PreImportRequest{
		JobID: 1, TaskID: 2, CollectionID: 3, Schema: schema,
		ImportFiles: files, Options: options, StorageConfig: config,
	}, cm)
	result, err := preTask.Execute(context.Background())
	require.NoError(t, err)
	stats := result.([]*datapb.ImportV3FileStats)
	require.Len(t, stats, 2)
	require.Equal(t, int64(2), stats[0].GetTotalRows())
	require.Equal(t, int64(len(part0)), stats[0].GetFileSize())
	require.Equal(t, int64(3), stats[1].GetTotalRows())
	require.Equal(t, int64(len(part1)), stats[1].GetFileSize())
	require.Equal(t, int32(1), clientBuilds.Load(), "preimport must construct one source client per task attempt")

	clientBuilds.Store(0)
	var gotPKs []int64
	writerMock := mockey.Mock(writeFragment).To(
		func(spec *writeSpec, _ *SpillManager, vchannelOrdinal, partitionOrdinal int, input fragmentInput, seq int64) (*fragmentOutcome, error) {
			for _, p := range input.pieces {
				gotPKs = append(gotPKs, p.mem.Data[100].(*storage.Int64FieldData).Data...)
			}
			return &fragmentOutcome{
				descriptor: &importv3pb.FragmentDescriptor{
					VchannelIndex: int32(vchannelOrdinal),
					PartitionId:   spec.plan.GetPartitionIds()[partitionOrdinal],
					Seq:           seq,
					Path:          "frag-0.parquet",
					Rows:          input.rows,
					LogicalBytes:  input.logicalBytes,
				},
				timings:             &storage.SortTimings{},
				writtenUncompressed: uint64(input.logicalBytes),
			}, nil
		}).Build()
	t.Cleanup(func() { writerMock.UnPatch() })

	var published *importv3pb.ReshardManifest
	cm.EXPECT().RootPath().Return("milvus").Once()
	cm.EXPECT().Write(mock.Anything, mock.MatchedBy(func(p string) bool {
		return strings.HasPrefix(p, "milvus/")
	}), mock.Anything).RunAndReturn(func(_ context.Context, _ string, payload []byte) error {
		manifest := &importv3pb.ReshardManifest{}
		if err := proto.Unmarshal(payload, manifest); err != nil {
			return err
		}
		published = manifest
		return nil
	}).Once()

	plan := &importv3pb.ReshardTaskPlan{
		CollectionId:     3,
		CollectionSchema: schema,
		Vchannels:        []string{"ch0"},
		PartitionIds:     []int64{10},
		Files:            files,
		Options:          options,
		FragmentSize:     8 << 20,
	}
	reshard := NewReshardTask(&datapb.ReshardTaskRequest{
		JobId: 1, TaskId: 2, RunId: 3, Slot: 1,
		StorageConfig: config,
		Plan:          plan,
	}, cm, nil, nil, NewMetrics(1))
	_, err = reshard.Execute(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{10, 11, 20, 21, 22}, gotPKs)
	require.NotNil(t, published)
	require.Len(t, published.GetFragments(), 1)
	require.Equal(t, int64(5), published.GetFragments()[0].GetRows())
	require.Equal(t, int32(1), clientBuilds.Load(), "reshard must construct one source client per task attempt")
}
