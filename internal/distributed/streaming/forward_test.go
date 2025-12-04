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

package streaming

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestForwardDMLToLegacyProxy(t *testing.T) {
	etcdCli, _ := kvfactory.GetEtcdAndPath()
	oldProxyPort := paramtable.Get().ProxyGrpcClientCfg.Port.SwapTempValue("19588")
	defer paramtable.Get().ProxyGrpcClientCfg.Port.SwapTempValue(oldProxyPort)

	proxySession := &sessionutil.SessionRaw{ServerID: 10086, Address: "127.0.0.1:19530", Version: "2.5.22"}
	proxySessionJSON, _ := json.Marshal(proxySession)

	key := sessionutil.GetSessionPrefixByRole(typeutil.ProxyRole) + "-10086"
	etcdCli.Put(context.Background(), key, string(proxySessionJSON))
	defer etcdCli.Delete(context.Background(), key)

	sc := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	as.EXPECT().GetLatestStreamingVersion(mock.Anything).Return(nil, nil)
	sc.EXPECT().Assignment().Return(as)

	s := newForwardService(sc)

	Release()
	singleton = &walAccesserImpl{
		forwardService: s,
	}

	reqs := []any{
		&milvuspb.DeleteRequest{},
		&milvuspb.InsertRequest{},
		&milvuspb.UpsertRequest{},
		&milvuspb.SearchRequest{},
		&milvuspb.HybridSearchRequest{},
		&milvuspb.QueryRequest{},
	}
	methods := []string{
		milvuspb.MilvusService_Delete_FullMethodName,
		milvuspb.MilvusService_Insert_FullMethodName,
		milvuspb.MilvusService_Upsert_FullMethodName,
		milvuspb.MilvusService_Search_FullMethodName,
		milvuspb.MilvusService_HybridSearch_FullMethodName,
		milvuspb.MilvusService_Query_FullMethodName,
	}
	interceptor := ForwardLegacyProxyUnaryServerInterceptor()

	for idx, req := range reqs {
		method := methods[idx]
		remoteErr := errors.New("test")
		resp, err := interceptor(context.Background(), req, &grpc.UnaryServerInfo{
			FullMethod: method,
		}, func(ctx context.Context, req any) (any, error) {
			return nil, remoteErr
		})
		// because there's no upstream legacy proxy, the error should be unavailable.
		st := status.Convert(err)
		assert.True(t, st.Code() == codes.Unavailable || st.Code() == codes.Unimplemented)
		assert.Nil(t, resp)
	}

	// Only DML requests will be handled by the forward service.
	resp, err := interceptor(context.Background(), &milvuspb.CreateCollectionRequest{}, &grpc.UnaryServerInfo{
		FullMethod: milvuspb.MilvusService_CreateCollection_FullMethodName,
	}, func(ctx context.Context, req any) (any, error) {
		return merr.Success(), nil
	})
	assert.NoError(t, merr.CheckRPCCall(resp, err))

	// after all proxy is down, the request will be forwarded to the legacy proxy.
	etcdCli.Delete(context.Background(), key)
	for idx, req := range reqs {
		method := methods[idx]
		resp, err := interceptor(context.Background(), req, &grpc.UnaryServerInfo{
			FullMethod: method,
		}, func(ctx context.Context, req any) (any, error) {
			return merr.Success(), nil
		})
		if err != nil {
			st := status.Convert(err)
			assert.True(t, st.Code() == codes.Unavailable || st.Code() == codes.Unimplemented)
		} else {
			assert.NoError(t, merr.CheckRPCCall(resp, err))
		}
	}

	// after streaming service is ready, the request will not be forwarded to the legacy proxy.
	as.EXPECT().GetLatestStreamingVersion(mock.Anything).Unset()
	as.EXPECT().GetLatestStreamingVersion(mock.Anything).Return(&streamingpb.StreamingVersion{
		Version: 1,
	}, nil)
	for idx, req := range reqs {
		method := methods[idx]
		resp, err := interceptor(context.Background(), req, &grpc.UnaryServerInfo{
			FullMethod: method,
		}, func(ctx context.Context, req any) (any, error) {
			return merr.Success(), nil
		})
		assert.NoError(t, merr.CheckRPCCall(resp, err))
	}
}
