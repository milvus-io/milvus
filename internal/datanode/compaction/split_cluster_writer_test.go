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
	"github.com/milvus-io/milvus/pkg/util/conc"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestSplitClusterWriterSuite(t *testing.T) {
	suite.Run(t, new(SplitClusterWriterSuite))
}

type SplitClusterWriterSuite struct {
	suite.Suite
	mockBinlogIO *io.MockBinlogIO
	allocator    allocator.Interface
	meta         *etcdpb.CollectionMeta
}

func (s *SplitClusterWriterSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *SplitClusterWriterSuite) SetupTest() {
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())
	s.meta = genTestCollectionMeta()
	s.allocator = allocator.NewLocalAllocator(time.Now().UnixMilli(), math.MaxInt64)

	paramtable.Get().Save(paramtable.Get().CommonCfg.EntityExpirationTTL.Key, "0")
}

func (s *SplitClusterWriterSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *SplitClusterWriterSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
}

func (s *SplitClusterWriterSuite) TestSplitByHash() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	mappingFunc := func(value *storage.Value) (string, error) {
		pkHash, err := value.PK.Hash()
		if err != nil {
			return "", err
		}
		return fmt.Sprint(pkHash % uint32(2)), nil
	}

	splitWriter, err := NewSplitClusterWriterBuilder().
		SetBinlogIO(s.mockBinlogIO).
		SetAllocator(&compactionAlloactor{
			segmentAlloc: s.allocator,
			logIDAlloc:   s.allocator,
		}).
		SetCollectionID(1).
		SetPartitionID(2).
		SetSchema(s.meta.Schema).
		SetChannel("ch-1").
		SetSegmentMaxSize(1000000).
		SetSegmentMaxRowCount(1000).
		SetSplitKeys([]string{"0", "1"}).
		SetMemoryBufferSize(math.MaxInt64).
		SetMappingFunc(mappingFunc).
		SetWorkerPoolSize(1).
		Build()
	s.NoError(err)

	for i := int64(0); i < 1000; i++ {
		err := splitWriter.Write(generateInt64PKEntitiy(i))
		s.NoError(err)
	}

	s.Equal(int64(500), splitWriter.clusterWriters["1"].GetRowNum())
	s.Equal(int64(500), splitWriter.clusterWriters["0"].GetRowNum())

	segments, err := splitWriter.Finish()
	s.NoError(err)
	s.Equal(2, len(segments))
	s.Equal(1, len(segments["0"]))
	s.Equal(1, len(segments["0"][0].GetField2StatslogPaths()))
	s.Equal(1, len(segments["1"]))
	s.Equal(1, len(segments["1"][0].GetField2StatslogPaths()))
}

func (s *SplitClusterWriterSuite) TestSplitByRange() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	mappingFunc := func(value *storage.Value) (string, error) {
		if value.PK.LT(storage.NewInt64PrimaryKey(200)) {
			return "[0,200)", nil
		} else if value.PK.LT(storage.NewInt64PrimaryKey(400)) {
			return "[200,400)", nil
		} else if value.PK.LT(storage.NewInt64PrimaryKey(600)) {
			return "[400,600)", nil
		} else if value.PK.LT(storage.NewInt64PrimaryKey(800)) {
			return "[600,800)", nil
		} else {
			return "[800,1000)", nil
		}
	}

	splitWriter, err := NewSplitClusterWriterBuilder().
		SetBinlogIO(s.mockBinlogIO).
		SetAllocator(&compactionAlloactor{
			segmentAlloc: s.allocator,
			logIDAlloc:   s.allocator,
		}).
		SetCollectionID(1).
		SetPartitionID(2).
		SetSchema(s.meta.Schema).
		SetChannel("ch-1").
		SetSegmentMaxSize(1000000).
		SetSegmentMaxRowCount(10000).
		SetSplitKeys([]string{"[0,200)", "[200,400)", "[400,600)", "[600,800)", "[800,1000)"}).
		SetMemoryBufferSize(math.MaxInt64).
		SetMappingFunc(mappingFunc).
		SetWorkerPoolSize(1).
		Build()
	s.NoError(err)

	for i := int64(0); i < 1000; i++ {
		err := splitWriter.Write(generateInt64PKEntitiy(i))
		s.NoError(err)
	}

	s.Equal(5, len(splitWriter.clusterWriters))
	s.Equal(int64(200), splitWriter.clusterWriters["[0,200)"].GetRowNum())
	s.Equal(int64(200), splitWriter.clusterWriters["[200,400)"].GetRowNum())
	s.Equal(int64(200), splitWriter.clusterWriters["[400,600)"].GetRowNum())
	s.Equal(int64(200), splitWriter.clusterWriters["[600,800)"].GetRowNum())
	s.Equal(int64(200), splitWriter.clusterWriters["[800,1000)"].GetRowNum())

	result, err := splitWriter.Finish()
	s.NoError(err)
	s.Equal(5, len(result))

	var totalRows int64 = 0
	for _, segments := range result {
		for _, seg := range segments {
			totalRows = totalRows + seg.GetNumOfRows()
		}
	}
	s.Equal(int64(1000), totalRows)

	s.Equal(1, len(result["[0,200)"][0].GetField2StatslogPaths()))
	s.Equal(1, len(result["[200,400)"][0].GetField2StatslogPaths()))
	s.Equal(1, len(result["[400,600)"][0].GetField2StatslogPaths()))
	s.Equal(1, len(result["[600,800)"][0].GetField2StatslogPaths()))
	s.Equal(1, len(result["[800,1000)"][0].GetField2StatslogPaths()))
}

func (s *SplitClusterWriterSuite) TestConcurrentSplitByHash() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)

	mappingFunc := func(value *storage.Value) (string, error) {
		pkHash, err := value.PK.Hash()
		if err != nil {
			return "", err
		}
		return fmt.Sprint(pkHash % uint32(2)), nil
	}

	splitWriter, err := NewSplitClusterWriterBuilder().
		SetBinlogIO(s.mockBinlogIO).
		SetAllocator(&compactionAlloactor{
			segmentAlloc: s.allocator,
			logIDAlloc:   s.allocator,
		}).
		SetCollectionID(1).
		SetPartitionID(2).
		SetSchema(s.meta.Schema).
		SetChannel("ch-1").
		SetSegmentMaxRowCount(1000).
		SetSplitKeys([]string{"0", "1"}).
		SetMemoryBufferSize(math.MaxInt64).
		SetMappingFunc(mappingFunc).
		SetWorkerPoolSize(1).
		Build()
	s.NoError(err)

	pool := conc.NewPool[any](10)
	futures := make([]*conc.Future[any], 0)
	for i := 0; i < 10; i++ {
		j := int64(i * 1000)
		future := pool.Submit(func() (any, error) {
			for i := j + int64(0); i < j+1000; i++ {
				err := splitWriter.Write(generateInt64PKEntitiy(i))
				if i == j+100 {
					splitWriter.FlushLargest()
				}
				s.NoError(err)
			}
			return struct{}{}, nil
		})
		futures = append(futures, future)
	}
	err = conc.AwaitAll(futures...)
	s.NoError(err)

	for id, buffer := range splitWriter.clusterWriters {
		println(id)
		println(buffer.GetRowNum())
	}

	result, err := splitWriter.Finish()
	s.NoError(err)

	var totalRows int64 = 0
	for _, segments := range result {
		for _, seg := range segments {
			totalRows = totalRows + seg.GetNumOfRows()
		}
	}

	s.Equal(1, len(result["0"][0].GetField2StatslogPaths()))
	s.Equal(1, len(result["1"][0].GetField2StatslogPaths()))
}
