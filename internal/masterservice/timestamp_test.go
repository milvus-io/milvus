// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package masterservice

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/masterpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
)

type tbd struct {
	types.DataService
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
	types.QueryService
}

func (*tbq) ReleaseCollection(context.Context, *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return nil, nil
}

type tbi struct {
	types.IndexService
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

	msFactory := msgstream.NewPmsFactory()
	Params.Init()
	core, err := NewCore(ctx, msFactory)

	assert.Nil(b, err)

	randVal := rand.Int()

	Params.TimeTickChannel = fmt.Sprintf("master-time-tick-%d", randVal)
	Params.StatisticsChannel = fmt.Sprintf("master-statistics-%d", randVal)
	Params.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.MetaRootPath)
	Params.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.KvRootPath)
	Params.MsgChannelSubName = fmt.Sprintf("subname-%d", randVal)

	err = core.SetDataService(ctx, &tbd{})
	assert.Nil(b, err)

	err = core.SetIndexService(&tbi{})
	assert.Nil(b, err)

	err = core.SetQueryService(&tbq{})
	assert.Nil(b, err)

	err = core.Register()
	assert.Nil(b, err)

	pnm := &proxyNodeMock{
		collArray: make([]string, 0, 16),
		mutex:     sync.Mutex{},
	}
	core.NewProxyClient = func(*sessionutil.Session) (types.ProxyNode, error) {
		return pnm, nil
	}

	err = core.Init()
	assert.Nil(b, err)

	err = core.Start()
	assert.Nil(b, err)

	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := masterpb.AllocTimestampRequest{
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
