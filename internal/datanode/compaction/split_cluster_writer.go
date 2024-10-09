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

package compaction

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
)

type SplitClusterWriter struct {
	// construct field
	binlogIO           io.BinlogIO
	allocator          *compactionAlloactor
	collectionID       int64
	partitionID        int64
	channel            string
	schema             *schemapb.CollectionSchema
	segmentMaxRowCount int64
	segmentMaxSize     int64
	clusterNum         int32
	mappingFunc        func(*storage.Value) (string, error)
	memoryBufferSize   int64
	workerPoolSize     int

	// inner field
	clusterWriters map[string]*MultiSegmentWriter
	writtenRowNum  *atomic.Int64
	flushPool      *conc.Pool[any]
	flushMutex     sync.Mutex
}

func (c *SplitClusterWriter) Write(value *storage.Value) error {
	clusterKey, err := c.mappingFunc(value)
	if err != nil {
		return err
	}
	// c.clusterLocks.Lock(clusterKey)
	// defer c.clusterLocks.Unlock(clusterKey)
	_, exist := c.clusterWriters[clusterKey]
	if !exist {
		return errors.New(fmt.Sprintf("cluster key=%s not exist", clusterKey))
	}
	err = c.clusterWriters[clusterKey].Write(value)
	if err != nil {
		return err
	}
	c.writtenRowNum.Inc()
	return nil
}

func (c *SplitClusterWriter) Finish() (map[string][]*datapb.CompactionSegment, error) {
	resultSegments := make(map[string][]*datapb.CompactionSegment, 0)
	for id, writer := range c.clusterWriters {
		log.Info("Finish", zap.String("id", id), zap.Int("current", writer.current), zap.Int64("current", writer.GetRowNum()))
		segments, err := writer.Finish()
		if err != nil {
			return nil, err
		}
		//for _, segment := range segments {
		//	segment.VshardId = id
		//}
		resultSegments[id] = segments
	}
	return resultSegments, nil
}

func (c *SplitClusterWriter) GetRowNum() int64 {
	return c.writtenRowNum.Load()
}

func (c *SplitClusterWriter) FlushLargest() error {
	// only one flushLargest or flushAll should do at the same time
	getLock := c.flushMutex.TryLock()
	if !getLock {
		return nil
	}
	defer c.flushMutex.Unlock()
	currentMemorySize := c.getTotalUsedMemorySize()
	if currentMemorySize <= c.getMemoryBufferLowWatermark() {
		log.Info("memory low water mark", zap.Int64("memoryBufferSize", c.getTotalUsedMemorySize()))
		return nil
	}
	bufferIDs := make([]string, 0)
	bufferRowNums := make([]int64, 0)
	for id, writer := range c.clusterWriters {
		bufferIDs = append(bufferIDs, id)
		// c.clusterLocks.RLock(id)
		bufferRowNums = append(bufferRowNums, writer.GetRowNum())
		// c.clusterLocks.RUnlock(id)
	}
	sort.Slice(bufferIDs, func(i, j int) bool {
		return bufferRowNums[i] > bufferRowNums[j]
	})
	log.Info("start flushLargestBuffers", zap.Strings("bufferIDs", bufferIDs), zap.Int64("currentMemorySize", currentMemorySize))

	futures := make([]*conc.Future[any], 0)
	for _, bufferId := range bufferIDs {
		writer := c.clusterWriters[bufferId]
		log.Info("currentMemorySize after flush writer binlog",
			zap.Int64("currentMemorySize", currentMemorySize),
			zap.String("bufferID", bufferId),
			zap.Uint64("writtenMemorySize", writer.WrittenMemorySize()),
			zap.Int64("RowNum", writer.GetRowNum()))
		future := c.flushPool.Submit(func() (any, error) {
			err := writer.Flush()
			if err != nil {
				return nil, err
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)

		if currentMemorySize <= c.getMemoryBufferLowWatermark() {
			log.Info("reach memory low water mark", zap.Int64("memoryBufferSize", c.getTotalUsedMemorySize()))
			break
		}
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}
	return nil
}

func (c *SplitClusterWriter) getTotalUsedMemorySize() int64 {
	var totalBufferSize int64 = 0
	for _, writer := range c.clusterWriters {
		totalBufferSize = totalBufferSize + int64(writer.WrittenMemorySize())
	}
	return totalBufferSize
}

func (c *SplitClusterWriter) getMemoryBufferLowWatermark() int64 {
	return int64(float64(c.memoryBufferSize) * 0.3)
}

func (c *SplitClusterWriter) getMemoryBufferHighWatermark() int64 {
	return int64(float64(c.memoryBufferSize) * 0.7)
}

// Builder for SplitClusterWriter
type SplitClusterWriterBuilder struct {
	binlogIO           io.BinlogIO
	allocator          *compactionAlloactor
	collectionID       int64
	partitionID        int64
	channel            string
	schema             *schemapb.CollectionSchema
	segmentMaxRowCount int64
	segmentMaxSize     int64
	splitKeys          []string
	mappingFunc        func(*storage.Value) (string, error)
	memoryBufferSize   int64
	workerPoolSize     int
}

// NewSplitClusterWriterBuilder creates a new builder instance
func NewSplitClusterWriterBuilder() *SplitClusterWriterBuilder {
	return &SplitClusterWriterBuilder{}
}

func (b *SplitClusterWriterBuilder) SetBinlogIO(binlogIO io.BinlogIO) *SplitClusterWriterBuilder {
	b.binlogIO = binlogIO
	return b
}

func (b *SplitClusterWriterBuilder) SetAllocator(allocator *compactionAlloactor) *SplitClusterWriterBuilder {
	b.allocator = allocator
	return b
}

// SetCollectionID sets the collectionID field
func (b *SplitClusterWriterBuilder) SetCollectionID(collectionID int64) *SplitClusterWriterBuilder {
	b.collectionID = collectionID
	return b
}

// SetPartitionID sets the partitionID field
func (b *SplitClusterWriterBuilder) SetPartitionID(partitionID int64) *SplitClusterWriterBuilder {
	b.partitionID = partitionID
	return b
}

func (b *SplitClusterWriterBuilder) SetChannel(channel string) *SplitClusterWriterBuilder {
	b.channel = channel
	return b
}

// SetSchema sets the schema field
func (b *SplitClusterWriterBuilder) SetSchema(schema *schemapb.CollectionSchema) *SplitClusterWriterBuilder {
	b.schema = schema
	return b
}

func (b *SplitClusterWriterBuilder) SetSegmentMaxSize(segmentMaxSize int64) *SplitClusterWriterBuilder {
	b.segmentMaxSize = segmentMaxSize
	return b
}

// SetSegmentMaxRowCount sets the segmentMaxRowCount field
func (b *SplitClusterWriterBuilder) SetSegmentMaxRowCount(segmentMaxRowCount int64) *SplitClusterWriterBuilder {
	b.segmentMaxRowCount = segmentMaxRowCount
	return b
}

// SetSplitKeys sets the splitKeys field
func (b *SplitClusterWriterBuilder) SetSplitKeys(keys []string) *SplitClusterWriterBuilder {
	b.splitKeys = keys
	return b
}

// SetMappingFunc sets the mappingFunc field
func (b *SplitClusterWriterBuilder) SetMappingFunc(mappingFunc func(*storage.Value) (string, error)) *SplitClusterWriterBuilder {
	b.mappingFunc = mappingFunc
	return b
}

func (b *SplitClusterWriterBuilder) SetMemoryBufferSize(memoryBufferSize int64) *SplitClusterWriterBuilder {
	b.memoryBufferSize = memoryBufferSize
	return b
}

func (b *SplitClusterWriterBuilder) SetWorkerPoolSize(workerPoolSize int) *SplitClusterWriterBuilder {
	b.workerPoolSize = workerPoolSize
	return b
}

// Build creates the final SplitClusterWriter instance
func (b *SplitClusterWriterBuilder) Build() (*SplitClusterWriter, error) {
	writer := &SplitClusterWriter{
		binlogIO:           b.binlogIO,
		allocator:          b.allocator,
		collectionID:       b.collectionID,
		partitionID:        b.partitionID,
		schema:             b.schema,
		channel:            b.channel,
		segmentMaxSize:     b.segmentMaxSize,
		segmentMaxRowCount: b.segmentMaxRowCount,
		clusterWriters:     make(map[string]*MultiSegmentWriter, len(b.splitKeys)),
		mappingFunc:        b.mappingFunc,
		workerPoolSize:     b.workerPoolSize,
		flushPool:          conc.NewPool[any](b.workerPoolSize),
		memoryBufferSize:   b.memoryBufferSize,
		writtenRowNum:      atomic.NewInt64(0),
	}

	for _, key := range b.splitKeys {
		writer.clusterWriters[key] = NewMultiSegmentWriter(writer.binlogIO, writer.allocator, writer.schema, writer.channel, writer.segmentMaxSize, writer.segmentMaxRowCount, writer.partitionID, writer.collectionID, true)
	}

	return writer, nil
}
