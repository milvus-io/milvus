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
	"math"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func TestSegmentWriterSuite(t *testing.T) {
	suite.Run(t, new(SegmentWriterSuite))
}

type SegmentWriterSuite struct {
	suite.Suite
	mockBinlogIO *io.MockBinlogIO
	allocator    allocator.Interface
	meta         *etcdpb.CollectionMeta
}

func (s *SegmentWriterSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *SegmentWriterSuite) SetupTest() {
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())
	s.meta = genTestCollectionMeta()
	s.allocator = allocator.NewLocalAllocator(time.Now().UnixMilli(), math.MaxInt64)

	paramtable.Get().Save(paramtable.Get().CommonCfg.EntityExpirationTTL.Key, "0")
}

func (s *SegmentWriterSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *SegmentWriterSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
}

func (s *SegmentWriterSuite) TestFlushBinlog() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	allocator := NewCompactionAllocator(s.allocator, s.allocator)
	writer := NewMultiSegmentWriter(s.mockBinlogIO, allocator, s.meta.GetSchema(), "ch-1", 1000000, 1000, 2, 1, true)

	for i := int64(0); i < 500; i++ {
		err := writer.Write(generateInt64PKEntitiy(i))
		s.NoError(err)
	}
	writer.Flush()

	for i := int64(500); i < 1000; i++ {
		err := writer.Write(generateInt64PKEntitiy(i))
		s.NoError(err)
	}

	compactionSegments, err := writer.Finish()
	s.NoError(err)
	s.Equal(2, len(compactionSegments[0].InsertLogs[0].Binlogs))
	s.Equal(int64(1000), compactionSegments[0].GetNumOfRows())
}

func (s *SegmentWriterSuite) TestGetRowNum() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	allocator := NewCompactionAllocator(s.allocator, s.allocator)
	writer := NewMultiSegmentWriter(s.mockBinlogIO, allocator, s.meta.GetSchema(), "ch-1", 200000, 1000, 2, 1, true)

	for i := int64(0); i < 500; i++ {
		err := writer.Write(generateInt64PKEntitiy(i))
		s.NoError(err)
	}
	s.Equal(int64(500), writer.GetRowNum())
	s.Equal(int64(65000), int64(writer.WrittenMemorySize()))

	writer.Flush()
	s.Equal(int64(500), writer.GetRowNum())
	s.Equal(int64(0), int64(writer.WrittenMemorySize()))

	for i := int64(500); i < 1000; i++ {
		err := writer.Write(generateInt64PKEntitiy(i))
		s.NoError(err)
	}
	s.Equal(int64(1000), writer.GetRowNum())
	s.Equal(int64(65000), int64(writer.WrittenMemorySize()))

	compactionSegments, err := writer.Finish()
	s.NoError(err)

	s.Equal(int64(1000), writer.GetRowNum())
	s.Equal(int64(0), int64(writer.WrittenMemorySize()))

	s.Equal(2, len(compactionSegments[0].InsertLogs[0].Binlogs))
	s.Equal(int64(1000), compactionSegments[0].GetNumOfRows())
}

func (s *SegmentWriterSuite) TestWriteMultiSegments() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	allocator := NewCompactionAllocator(s.allocator, s.allocator)
	writer := NewMultiSegmentWriter(s.mockBinlogIO, allocator, s.meta.GetSchema(), "ch-1", 100000, 500, 2, 1, true)

	for i := int64(0); i < 1000; i++ {
		err := writer.Write(generateInt64PKEntitiy(i))
		s.NoError(err)
	}

	compactionSegments, err := writer.Finish()
	s.NoError(err)
	s.Equal(2, len(compactionSegments))
}

func (s *SegmentWriterSuite) TestConcurrentWriteMultiSegments() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	allocator := NewCompactionAllocator(s.allocator, s.allocator)
	writer := NewMultiSegmentWriter(s.mockBinlogIO, allocator, s.meta.GetSchema(), "ch-1", 100000, 500, 2, 1, true)

	pool := conc.NewPool[any](10)
	futures := make([]*conc.Future[any], 0)
	for i := 0; i < 10; i++ {
		j := int64(i * 1000)
		future := pool.Submit(func() (any, error) {
			for i := j + int64(0); i < j+1000; i++ {
				err := writer.Write(generateInt64PKEntitiy(i))
				if i == j+100 {
					writer.Flush()
				}
				s.NoError(err)
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	err := conc.AwaitAll(futures...)
	s.NoError(err)

	compactionSegments, err := writer.Finish()
	s.NoError(err)

	totalRows := lo.Reduce(lo.Map(compactionSegments, func(segment *datapb.CompactionSegment, i int) int64 {
		return segment.GetNumOfRows()
	}), func(i int64, j int64, x int) int64 {
		return i + j
	}, 0)
	s.Equal(int64(10000), totalRows)
	s.Equal(13, len(compactionSegments))

	s.Equal(1, len(compactionSegments[0].GetField2StatslogPaths()))
	s.Equal(1, len(compactionSegments[1].GetField2StatslogPaths()))
}

func generateInt64PKEntitiy(magic int64) *storage.Value {
	return &storage.Value{
		PK:        storage.NewInt64PrimaryKey(magic),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
		Value:     getRow(magic),
	}
}
