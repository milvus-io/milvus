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

// SplitClusterWriter is a thread-safe writer that supports writing to multiple clusters of segments.
// It requires clusterKeys and a mapping function, with data being written to the appropriate cluster based on the mapping function
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
	clusterKeys        []string
	mappingFunc        func(*storage.Value) (string, error)
	workerPoolSize     int

	// inner field
	clusterWriters map[string]*MultiSegmentWriter
	writtenRowNum  *atomic.Int64
	flushPool      *conc.Pool[any]
	flushMutex     sync.Mutex
	flushCount     *atomic.Int64
}

// Write value to the cluster based on mappingFunc
func (c *SplitClusterWriter) Write(value *storage.Value) error {
	clusterKey, err := c.mappingFunc(value)
	if err != nil {
		return err
	}
	return c.WriteToCluster(value, clusterKey)
}

// WriteToCluster write value to specific cluster
func (c *SplitClusterWriter) WriteToCluster(value *storage.Value, clusterKey string) error {
	_, exist := c.clusterWriters[clusterKey]
	if !exist {
		return errors.New(fmt.Sprintf("cluster key=%s not exist", clusterKey))
	}
	err := c.clusterWriters[clusterKey].Write(value)
	if err != nil {
		return err
	}
	c.writtenRowNum.Inc()
	return nil
}

// Finish all writers and return compactionSegments map(clusterKey->segments)
func (c *SplitClusterWriter) Finish() (map[string][]*datapb.CompactionSegment, error) {
	resultSegments := make(map[string][]*datapb.CompactionSegment, 0)
	for clusterKey, writer := range c.clusterWriters {
		log.Debug("Finish writer", zap.String("clusterKey", clusterKey), zap.Int32("current", writer.current.Load()), zap.Uint64("size", writer.WrittenMemorySize()))
		segments, err := writer.Finish()
		if err != nil {
			return nil, err
		}
		resultSegments[clusterKey] = segments
	}
	return resultSegments, nil
}

func (c *SplitClusterWriter) GetRowNum() int64 {
	return c.writtenRowNum.Load()
}

// FlushTo flush the largest writers to the given memory
func (c *SplitClusterWriter) FlushTo(targetMemory int64) error {
	c.flushMutex.Lock()
	defer c.flushMutex.Unlock()
	currentMemorySize := c.getTotalUsedMemorySize()
	if currentMemorySize <= targetMemory {
		log.Info("memory is under target", zap.Int64("memoryBufferSize", c.getTotalUsedMemorySize()), zap.Int64("target", targetMemory))
		return nil
	}

	// collect the cluster memory size and sort the clusters by size
	// flush the top largest clusters until the data in memory less than watermark
	clusterKeys := make([]string, 0)
	clusterMemorySizeDict := make(map[string]int64, 0)
	clusterMemorySizes := make([]int64, 0)
	for clusterKey, writer := range c.clusterWriters {
		clusterKeys = append(clusterKeys, clusterKey)
		memorySize := int64(writer.WrittenMemorySize())
		clusterMemorySizeDict[clusterKey] = memorySize
		clusterMemorySizes = append(clusterMemorySizes, memorySize)
	}
	sort.Slice(clusterKeys, func(i, j int) bool {
		return clusterMemorySizes[i] > clusterMemorySizes[j]
	})

	afterFlushSize := currentMemorySize
	toFlushClusterKeys := make([]string, 0)
	for _, clusterKey := range clusterKeys {
		memorySize := clusterMemorySizeDict[clusterKey]
		toFlushClusterKeys = append(toFlushClusterKeys, clusterKey)
		afterFlushSize -= memorySize
		if afterFlushSize < targetMemory {
			break
		}
	}

	log.Info("start flush largest buffers", zap.Int("flushClusterNum", len(toFlushClusterKeys)), zap.Int64("currentMemorySize", currentMemorySize))
	futures := make([]*conc.Future[any], 0)
	for _, clusterKey := range toFlushClusterKeys {
		writer := c.clusterWriters[clusterKey]
		future, err := writer.Flush(c.flushPool)
		if err != nil {
			return err
		}
		futures = append(futures, future)
		c.flushCount.Inc()
		log.Info("flush count", zap.Int64("cnt", c.flushCount.Load()))
	}
	if err := conc.AwaitAll(futures...); err != nil {
		return err
	}
	// inc the flush count, for UT
	return nil
}

func (c *SplitClusterWriter) getTotalUsedMemorySize() int64 {
	var totalBufferSize int64 = 0
	for _, writer := range c.clusterWriters {
		//log.Debug("getTotalUsedMemorySize", zap.String("clusterKey", clusterKey), zap.Uint64("writer.WrittenMemorySize()", writer.WrittenMemorySize()))
		totalBufferSize += int64(writer.WrittenMemorySize())
	}
	return totalBufferSize
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
	clusterKeys        []string
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

// SetClusterKeys sets the clusterKeys field
func (b *SplitClusterWriterBuilder) SetClusterKeys(keys []string) *SplitClusterWriterBuilder {
	b.clusterKeys = keys
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
		clusterKeys:        b.clusterKeys,
		clusterWriters:     make(map[string]*MultiSegmentWriter, len(b.clusterKeys)),
		mappingFunc:        b.mappingFunc,
		workerPoolSize:     b.workerPoolSize,
		flushPool:          conc.NewPool[any](b.workerPoolSize),
		writtenRowNum:      atomic.NewInt64(0),
		flushCount:         atomic.NewInt64(0),
	}
	bm25FieldIds := GetBM25FieldIDs(b.schema)
	for _, key := range b.clusterKeys {
		writer.clusterWriters[key] = NewMultiSegmentWriter(writer.binlogIO, writer.allocator, writer.schema, writer.channel, writer.segmentMaxSize, writer.segmentMaxRowCount, writer.partitionID, writer.collectionID, bm25FieldIds, true)
	}

	return writer, nil
}
