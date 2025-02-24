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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

type ResultCacheServerSuite struct {
	suite.Suite
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

func generateIntIds(num int) *schemapb.IDs {
	data := make([]int64, num)
	for i := 0; i < num; i++ {
		data[i] = int64(i)
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data}},
	}
}

func generateStrIds(num int) *schemapb.IDs {
	data := make([]string, num)
	for i := 0; i < num; i++ {
		data[i] = strconv.FormatInt(int64(i), 10)
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: data}},
	}
}

func (s *ResultCacheServerSuite) TestSplit() {
	s.Run("split int64 message", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client := NewLocalQueryClient(ctx)
		srv := client.CreateServer()
		cacheSrv := NewResultCacheServer(srv, 1024, 1024)

		err := cacheSrv.Send(&internalpb.RetrieveResults{
			Ids: generateIntIds(1024),
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
			Ids: generateStrIds(2048),
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
}

func TestResultCacheServerSuite(t *testing.T) {
	suite.Run(t, new(ResultCacheServerSuite))
}
