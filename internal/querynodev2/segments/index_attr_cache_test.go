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

package segments

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type IndexAttrCacheSuite struct {
	suite.Suite

	c *IndexAttrCache
}

func (s *IndexAttrCacheSuite) SetupSuite() {
	paramtable.Init()
}

func (s *IndexAttrCacheSuite) SetupTest() {
	s.c = NewIndexAttrCache()
}

func (s *IndexAttrCacheSuite) TestCacheMissing() {
	info := &querypb.FieldIndexInfo{
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "test"},
		},
		CurrentIndexVersion: 0,
	}

	_, _, err := s.c.GetIndexResourceUsage(info, paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(), nil)
	s.Require().NoError(err)

	_, has := s.c.loadWithDisk.Get(typeutil.NewPair[string, int32]("test", 0))
	s.True(has)
}

func (s *IndexAttrCacheSuite) TestDiskANN() {
	info := &querypb.FieldIndexInfo{
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "DISKANN"},
		},
		CurrentIndexVersion: 0,
		IndexSize:           100,
	}

	memory, disk, err := s.c.GetIndexResourceUsage(info, paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(), nil)
	s.Require().NoError(err)

	_, has := s.c.loadWithDisk.Get(typeutil.NewPair[string, int32]("DISKANN", 0))
	s.False(has, "DiskANN shall never be checked load with disk")

	s.EqualValues(25, memory)
	s.EqualValues(75, disk)
}

func (s *IndexAttrCacheSuite) TestInvertedIndex() {
	info := &querypb.FieldIndexInfo{
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: indexparamcheck.IndexINVERTED},
		},
		CurrentIndexVersion: 0,
		IndexSize:           50,
	}
	binlog := &datapb.FieldBinlog{
		Binlogs: []*datapb.Binlog{
			{LogSize: 60},
		},
	}

	memory, disk, err := s.c.GetIndexResourceUsage(info, paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(), binlog)
	s.Require().NoError(err)
	s.EqualValues(uint64(0), memory)
	s.EqualValues(uint64(110), disk)
}

func (s *IndexAttrCacheSuite) TestLoadWithDisk() {
	info := &querypb.FieldIndexInfo{
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "test"},
		},
		CurrentIndexVersion: 0,
		IndexSize:           100,
	}

	s.Run("load_with_disk", func() {
		s.c.loadWithDisk.Insert(typeutil.NewPair[string, int32]("test", 0), true)
		memory, disk, err := s.c.GetIndexResourceUsage(info, paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(), nil)
		s.Require().NoError(err)

		s.EqualValues(100, memory)
		s.EqualValues(100, disk)
	})

	s.Run("load_with_disk", func() {
		s.c.loadWithDisk.Insert(typeutil.NewPair[string, int32]("test", 0), false)
		memory, disk, err := s.c.GetIndexResourceUsage(info, paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(), nil)
		s.Require().NoError(err)

		s.Equal(uint64(250), memory)
		s.Equal(uint64(0), disk)
	})

	s.Run("corrupted_index_info", func() {
		info := &querypb.FieldIndexInfo{
			IndexParams: []*commonpb.KeyValuePair{},
		}

		_, _, err := s.c.GetIndexResourceUsage(info, paramtable.Get().QueryNodeCfg.MemoryIndexLoadPredictMemoryUsageFactor.GetAsFloat(), nil)
		s.Error(err)
	})
}

func TestIndexAttrCache(t *testing.T) {
	suite.Run(t, new(IndexAttrCacheSuite))
}
