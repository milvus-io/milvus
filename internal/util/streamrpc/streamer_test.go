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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
)

type ResultCacheServerSuite struct {
	suite.Suite
}

func (s *ResultCacheServerSuite) TestSend() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewLocalQueryClient(ctx)
	srv := client.CreateServer()
	cacheSrv := NewResultCacheServer(srv, 1024)

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

func (s *ResultCacheServerSuite) TestMerge() {
	s.Nil(mergeCostAggregation(nil, nil))

	cost := &internalpb.CostAggregation{}
	s.Equal(cost, mergeCostAggregation(nil, cost))
	s.Equal(cost, mergeCostAggregation(cost, nil))

	a := &internalpb.CostAggregation{ResponseTime: 1, ServiceTime: 1, TotalNQ: 1, TotalRelatedDataSize: 1}
	b := &internalpb.CostAggregation{ResponseTime: 2, ServiceTime: 2, TotalNQ: 2, TotalRelatedDataSize: 2}
	c := mergeCostAggregation(a, b)
	s.Equal(int64(3), c.ResponseTime)
	s.Equal(int64(3), c.ServiceTime)
	s.Equal(int64(3), c.TotalNQ)
	s.Equal(int64(3), c.TotalRelatedDataSize)
}

func TestResultCacheServerSuite(t *testing.T) {
	suite.Run(t, new(ResultCacheServerSuite))
}
