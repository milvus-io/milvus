package accesslog

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type TestData struct {
	req, resp interface{}
	err       error
}

func genTestData(clientInfo *commonpb.ClientInfo, identifier int64) []*TestData {
	ret := []*TestData{}

	ret = append(ret, &TestData{
		req: &milvuspb.QueryRequest{
			CollectionName: "test1",
			Expr:           "pk >= 100",
		},
		resp: &milvuspb.QueryResults{
			CollectionName: "test1",
			Status:         merr.Status(merr.WrapErrParameterInvalid("testA", "testB", "stack: 1\n 2\n")),
		},
		err: nil,
	})

	ret = append(ret, &TestData{
		req: &milvuspb.SearchRequest{
			CollectionName: "test2",
			Dsl:            "pk <= 100",
		},
		resp: &milvuspb.SearchResults{
			CollectionName: "test2",
			Status:         merr.Status(nil),
		},
		err: nil,
	})

	ret = append(ret, &TestData{
		req: &milvuspb.ConnectRequest{
			ClientInfo: clientInfo,
		},
		resp: &milvuspb.ConnectResponse{
			Identifier: identifier,
			Status:     merr.Status(nil),
		},
		err: nil,
	})

	return ret
}

func BenchmarkAccesslog(b *testing.B) {
	paramtable.Init()
	Params := paramtable.Get()
	Params.Save(Params.ProxyCfg.AccessLog.Enable.Key, "true")
	Params.Save(Params.ProxyCfg.AccessLog.Filename.Key, "")
	Params.Save(Params.CommonCfg.ClusterPrefix.Key, "in-test")
	InitAccessLogger(Params)
	paramtable.Get().CommonCfg.ClusterPrefix.GetValue()

	clientInfo := &commonpb.ClientInfo{
		SdkType:    "gotest",
		SdkVersion: "testversion",
	}
	identifier := int64(11111)
	md := metadata.MD{util.IdentifierKey: []string{fmt.Sprint(identifier)}}
	ctx := metadata.NewIncomingContext(context.TODO(), md)
	connection.GetManager().Register(ctx, identifier, clientInfo)
	rpcInfo := &grpc.UnaryServerInfo{Server: nil, FullMethod: "testMethod"}
	datas := genTestData(clientInfo, identifier)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data := datas[i%len(datas)]
		accessInfo := info.NewGrpcAccessInfo(ctx, rpcInfo, data.req)
		accessInfo.UpdateCtx(ctx)
		accessInfo.SetResult(data.resp, data.err)
		_globalL.Write(accessInfo)
	}
}
