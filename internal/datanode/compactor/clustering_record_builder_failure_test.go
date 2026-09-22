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

package compactor

import (
	"context"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"sync"
	"testing"
	"time"
)

type clusteringFailingWriter struct {
	storage.BinlogRecordWriter
	writeErr error
	closeErr error
	writes   int
	closes   int
}

func (w *clusteringFailingWriter) GetWrittenUncompressed() uint64 { return 0 }
func (w *clusteringFailingWriter) GetBufferUncompressed() uint64  { return 0 }
func (w *clusteringFailingWriter) FlushChunk() error              { return nil }
func (w *clusteringFailingWriter) Write(storage.Record) error {
	w.writes++
	return w.writeErr
}
func (w *clusteringFailingWriter) Close() error {
	w.closes++
	return w.closeErr
}

func TestClusterBufferRejectsWritesAfterPartialAppend(t *testing.T) {
	schema := clusteringWideSchema()
	writer := &clusteringFailingWriter{closeErr: errors.New("injected close error")}
	buffer := newClusterBuffer(0, &MultiSegmentWriter{
		schema: schema, binLogMaxSize: 64 << 20, segmentSize: 1 << 30, allocator: &compactionAlloactor{},
		writer: storage.NewBinlogValueWriter(writer, 100),
	}, nil)
	defer buffer.releaseBuilder()
	good := clusteringTestRecord(t, schema, 1, 1)
	defer good.Release()
	require.NoError(t, buffer.WriteRecord(good, 0))
	badSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
	badSchema.Fields[len(badSchema.Fields)-1].DataType = schemapb.DataType_Int64
	bad := clusteringTestRecord(t, badSchema, 2, 1)
	defer bad.Release()
	appendErr := buffer.WriteRecord(bad, 0)
	require.ErrorContains(t, appendErr, "failed to append value")

	// Simulate workers that were already mapping when another worker failed.
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.ErrorIs(t, buffer.WriteRecord(good, 0), appendErr)
		}()
	}
	wg.Wait()
	require.ErrorIs(t, buffer.FlushChunk(), appendErr)
	require.Nil(t, buffer.builder, "the partially appended batch must be discarded")
	require.Zero(t, writer.writes, "no partial or later batch may reach the writer")
}

func TestClusteringMappingCancelsWorkersOnFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task := &clusteringCompactionTask{
		plan:        &datapb.CompactionPlan{SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 0}, {SegmentID: 1}}},
		mappingPool: conc.NewPool[any](2),
	}
	defer task.mappingPool.Release()
	started := make(chan struct{})
	exited := make(chan struct{})
	want := errors.New("injected append failure")
	patch := mockey.Mock((*clusteringCompactionTask).mappingSegment).To(func(_ *clusteringCompactionTask, ctx context.Context, segment *datapb.CompactionSegmentBinlogs) error {
		if segment.SegmentID == 0 {
			close(started)
			<-ctx.Done()
			close(exited)
			return ctx.Err()
		}
		<-started
		return want
	}).Build()
	defer patch.UnPatch()
	segments, stats, err := task.mapping(ctx)
	require.ErrorIs(t, err, want, "sibling cancellation must not mask the original failure")
	require.Nil(t, segments)
	require.Nil(t, stats)
	select {
	case <-exited:
	default:
		t.Fatal("mapping returned before the cancelled worker exited")
	}
}

func TestClusterBufferClosesWriterAfterFlushFailure(t *testing.T) {
	schema := clusteringWideSchema()
	writeErr := errors.New("injected write error")
	closeErr := errors.New("injected close error")
	writer := &clusteringFailingWriter{writeErr: writeErr, closeErr: closeErr}
	buffer := newClusterBuffer(0, &MultiSegmentWriter{
		schema: schema, binLogMaxSize: 64 << 20, segmentSize: 1 << 30, allocator: &compactionAlloactor{},
		writer: storage.NewBinlogValueWriter(writer, 100),
	}, nil)
	record := clusteringTestRecord(t, schema, 1, 1)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	err := buffer.Close()
	require.ErrorIs(t, err, writeErr)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, 1, writer.closes)
	require.Nil(t, buffer.builder)
	require.Nil(t, buffer.writer.writer)
	require.NoError(t, buffer.Close())
	require.Equal(t, 1, writer.closes)
}

func TestClusteringCleanupClosesWriterWithoutFlushingPendingRows(t *testing.T) {
	schema := clusteringWideSchema()
	writer := &clusteringFailingWriter{closeErr: errors.New("injected close error")}
	buffer := newClusterBuffer(0, &MultiSegmentWriter{
		schema: schema, binLogMaxSize: 64 << 20, segmentSize: 1 << 30, allocator: &compactionAlloactor{},
		writer: storage.NewBinlogValueWriter(writer, 100),
	}, nil)
	record := clusteringTestRecord(t, schema, 1, 1)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	task := &clusteringCompactionTask{clusterBuffers: []*ClusterBuffer{buffer}}
	task.cleanUp(context.Background())
	require.Zero(t, writer.writes)
	require.Equal(t, 1, writer.closes)
	require.Nil(t, buffer.builder)
	require.Nil(t, buffer.writer.writer)
	task.cleanUp(context.Background())
	require.Equal(t, 1, writer.closes)
}
