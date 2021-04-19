package masterservice

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/proxypb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

type tbp struct {
	types.ProxyService
}

func (*tbp) GetTimeTickChannel(context.Context) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: fmt.Sprintf("tbp-%d", rand.Int()),
	}, nil
}

func (*tbp) InvalidateCollectionMetaCache(context.Context, *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return nil, nil
}

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
	Params.DdChannel = fmt.Sprintf("master-dd-%d", randVal)
	Params.StatisticsChannel = fmt.Sprintf("master-statistics-%d", randVal)
	Params.MetaRootPath = fmt.Sprintf("/%d/%s", randVal, Params.MetaRootPath)
	Params.KvRootPath = fmt.Sprintf("/%d/%s", randVal, Params.KvRootPath)
	Params.MsgChannelSubName = fmt.Sprintf("subname-%d", randVal)

	err = core.SetProxyService(ctx, &tbp{})
	assert.Nil(b, err)

	err = core.SetDataService(ctx, &tbd{})
	assert.Nil(b, err)

	err = core.SetIndexService(&tbi{})
	assert.Nil(b, err)

	err = core.SetQueryService(&tbq{})
	assert.Nil(b, err)

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
