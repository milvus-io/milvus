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

package rootcoord

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mq"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

type tbd struct {
	types.DataCoord
}

func (*tbd) GetInsertBinlogPaths(context.Context, *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return nil, nil
}

func (*tbd) GetSegmentInfo(context.Context, *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return nil, nil
}

func (*tbd) GetSegmentInfoChannel(context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: fmt.Sprintf("tbd-%d", rand.Int()),
	}, nil
}

type tbq struct {
	types.QueryCoord
}

func (*tbq) ReleaseCollection(context.Context, *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

type tbi struct {
	types.IndexCoord
}

func (*tbi) BuildIndex(context.Context, *indexpb.BuildIndexRequest) (*indexpb.BuildIndexResponse, error) {
	return nil, nil
}

func (*tbi) DropIndex(context.Context, *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return nil, nil
}

func BenchmarkAllocTimestamp(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory := mq.NewDefaultFactory(true)
	Params.Init()
	core, err := NewCore(ctx, factory)

	assert.Nil(b, err)

	randVal := rand.Int()

	Params.CommonCfg.RootCoordTimeTick = fmt.Sprintf("master-time-tick-%d", randVal)
	Params.CommonCfg.RootCoordStatistics = fmt.Sprintf("master-statistics-%d", randVal)
	Params.EtcdCfg.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.MetaRootPath)
	Params.EtcdCfg.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.EtcdCfg.KvRootPath)
	Params.CommonCfg.RootCoordSubName = fmt.Sprintf("subname-%d", randVal)

	err = core.SetDataCoord(ctx, &tbd{})
	assert.Nil(b, err)

	err = core.SetIndexCoord(&tbi{})
	assert.Nil(b, err)

	err = core.SetQueryCoord(&tbq{})
	assert.Nil(b, err)

	err = core.Register()
	assert.Nil(b, err)

	pnm := &proxyMock{
		collArray: make([]string, 0, 16),
		mutex:     sync.Mutex{},
	}
	core.NewProxyClient = func(*sessionutil.Session) (types.Proxy, error) {
		return pnm, nil
	}

	err = core.Init()
	assert.Nil(b, err)

	err = core.Start()
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := rootcoordpb.AllocTimestampRequest{
			Base: &commonpb.MsgBase{
				MsgID: int64(i),
			},
			Count: 1,
		}
		_, err := core.AllocTimestamp(ctx, &req)
		assert.Nil(b, err)

	}
	b.StopTimer()
}
