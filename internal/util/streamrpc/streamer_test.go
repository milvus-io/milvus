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

package streamrpc

import (
	"context"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type ResultCacheServerSuite struct {
	suite.Suite
}

type recordingQueryStreamServer struct {
	results []*internalpb.RetrieveResults
}

func (s *recordingQueryStreamServer) Send(result *internalpb.RetrieveResults) error {
	s.results = append(s.results, result)
	return nil
}

func (s *recordingQueryStreamServer) Context() context.Context {
	return context.Background()
}

func (s *ResultCacheServerSuite) TestSend() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewLocalQueryClient(ctx)
	srv := client.CreateServer()
	cacheSrv := NewResultCacheServer(srv, 1024, math.MaxInt)

	err := cacheSrv.Send(&internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}},
		},
	})
	s.NoError(err)
	s.False(cacheSrv.cache.IsEmpty())

	err = cacheSrv.Send(&internalpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4, 5, 6}}},
		},
	})
	s.NoError(err)

	err = cacheSrv.Flush()
	s.NoError(err)
	s.True(cacheSrv.cache.IsEmpty())

	msg, err := client.Recv()
	s.NoError(err)
	// Data: []int64{1,2,3,4,5,6}
	s.Equal(6, len(msg.GetIds().GetIntId().GetData()))
}

func generateIntIDs(num int) *schemapb.IDs {
	data := make([]int64, num)
	for i := 0; i < num; i++ {
		data[i] = int64(i)
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data}},
	}
}

func generateStrIDs(num int) *schemapb.IDs {
	data := make([]string, num)
	for i := 0; i < num; i++ {
		data[i] = strconv.FormatInt(int64(i), 10)
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: data}},
	}
}

func (s *ResultCacheServerSuite) TestSplit() {
	s.Run("metadata-only result remains intact when max is below batch", func() {
		sink := &recordingQueryStreamServer{}
		cacheSrv := NewResultCacheServer(sink, 4*1024*1024, 1)
		s.Equal(1, cacheSrv.cache.cap)
		s.Equal(1, cacheSrv.maxMsgSize)
		result := &internalpb.RetrieveResults{
			Status:                    merr.Success(),
			Ids:                       &schemapb.IDs{},
			AllRetrieveCount:          7,
			ScannedRemoteBytes:        11,
			ScannedTotalBytes:         13,
			StorageCostValid:          true,
			SealedSegmentIDsRetrieved: []int64{17},
			CostAggregation: &internalpb.CostAggregation{
				TotalRelatedDataSize: 19,
			},
		}

		s.NoError(cacheSrv.Send(result))
		s.Len(sink.results, 1)
		s.True(proto.Equal(result, sink.results[0]))
		s.NoError(cacheSrv.Flush())
		s.Len(sink.results, 1)
	})

	s.Run("tiny max size still advances int64 splitting", func() {
		sink := &recordingQueryStreamServer{}
		cacheSrv := NewResultCacheServer(sink, 1, 1)

		s.NoError(cacheSrv.Send(&internalpb.RetrieveResults{Ids: generateIntIDs(2)}))
		s.NoError(cacheSrv.Flush())
		s.Len(sink.results, 2)
		s.Equal([]int64{0}, sink.results[0].GetIds().GetIntId().GetData())
		s.Equal([]int64{1}, sink.results[1].GetIds().GetIntId().GetData())
	})

	s.Run("split int64 message", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := NewLocalQueryClient(ctx)
		srv := client.CreateServer()
		cacheSrv := NewResultCacheServer(srv, 1024, 1024)

		err := cacheSrv.Send(&internalpb.RetrieveResults{
			Ids:              generateIntIDs(1024),
			StorageCostValid: true,
		})
		s.NoError(err)

		err = cacheSrv.Flush()
		s.NoError(err)

		srv.FinishSend(nil)

		rev := 0
		for {
			result, err := client.Recv()
			if err != nil {
				s.Equal(err, io.EOF)
				break
			}
			cnt := len(result.Ids.GetIntId().GetData())
			rev += cnt
			s.LessOrEqual(4*cnt, 1024)
			s.True(result.GetStorageCostValid())
		}
		s.Equal(1024, rev)
	})

	s.Run("split string message", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := NewLocalQueryClient(ctx)
		srv := client.CreateServer()
		cacheSrv := NewResultCacheServer(srv, 1024, 1024)

		err := cacheSrv.Send(&internalpb.RetrieveResults{
			Ids: generateStrIDs(2048),
		})
		s.NoError(err)

		err = cacheSrv.Flush()
		s.NoError(err)

		srv.FinishSend(nil)

		rev := 0
		for {
			result, err := client.Recv()
			if err != nil {
				s.Equal(err, io.EOF)
				break
			}

			rev += len(result.Ids.GetStrId().GetData())
			size := 0
			for _, str := range result.Ids.GetStrId().GetData() {
				size += len(str)
			}
			s.LessOrEqual(size, 1024)
		}
		s.Equal(rev, 2048)
	})
}

func (s *ResultCacheServerSuite) TestInvalidBatchSizes() {
	s.Run("non-positive values disable splitting", func() {
		cacheSrv := NewResultCacheServer(&recordingQueryStreamServer{}, 0, 0)
		s.Equal(math.MaxInt, cacheSrv.cache.cap)
		s.Equal(math.MaxInt, cacheSrv.maxMsgSize)
	})

	s.Run("non-positive batch uses max", func() {
		cacheSrv := NewResultCacheServer(&recordingQueryStreamServer{}, 0, 1024)
		s.Equal(1024, cacheSrv.cache.cap)
		s.Equal(1024, cacheSrv.maxMsgSize)
	})

	s.Run("non-positive max is unbounded", func() {
		cacheSrv := NewResultCacheServer(&recordingQueryStreamServer{}, 1024, 0)
		s.Equal(1024, cacheSrv.cache.cap)
		s.Equal(math.MaxInt, cacheSrv.maxMsgSize)
	})

	s.Run("batch larger than max is lowered", func() {
		cacheSrv := NewResultCacheServer(&recordingQueryStreamServer{}, 2048, 1024)
		s.Equal(1024, cacheSrv.cache.cap)
		s.Equal(1024, cacheSrv.maxMsgSize)
	})
}

func (s *ResultCacheServerSuite) TestMerge() {
	s.Nil(mergeCostAggregation(nil, nil))

	cost := &internalpb.CostAggregation{}
	s.Equal(cost, mergeCostAggregation(nil, cost))
	s.Equal(cost, mergeCostAggregation(cost, nil))

	a := &internalpb.CostAggregation{ResponseTime: 1, ServiceTime: 1, TotalNQ: 2, TotalRelatedDataSize: 1}
	b := &internalpb.CostAggregation{ResponseTime: 2, ServiceTime: 2, TotalNQ: 2, TotalRelatedDataSize: 2}
	c := mergeCostAggregation(a, b)
	s.Equal(int64(3), c.ResponseTime)
	s.Equal(int64(3), c.ServiceTime)
	s.Equal(int64(2), c.TotalNQ)
	s.Equal(int64(3), c.TotalRelatedDataSize)

	cache := &RetrieveResultCache{cap: math.MaxInt}
	cache.Put(&internalpb.RetrieveResults{
		Ids:                &schemapb.IDs{},
		ScannedRemoteBytes: 1,
		ScannedTotalBytes:  2,
		StorageCostValid:   true,
	})
	cache.Put(&internalpb.RetrieveResults{
		Ids:                generateIntIDs(2),
		ScannedRemoteBytes: 3,
		ScannedTotalBytes:  5,
		StorageCostValid:   false,
	})
	merged := cache.Flush()
	s.Equal([]int64{0, 1}, merged.GetIds().GetIntId().GetData())
	s.Equal(int64(4), merged.GetScannedRemoteBytes())
	s.Equal(int64(7), merged.GetScannedTotalBytes())
	s.False(merged.GetStorageCostValid())
}

func TestResultCacheServerSuite(t *testing.T) {
	suite.Run(t, new(ResultCacheServerSuite))
}
