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

package info

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/connection"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type GrpcAccessInfoSuite struct {
	suite.Suite

	username string
	traceID  string
	info     *GrpcAccessInfo
}

func (s *GrpcAccessInfoSuite) SetupSuite() {
	paramtable.Init()
}

func (s *GrpcAccessInfoSuite) SetupTest() {
	s.username = "test-user"
	s.traceID = "test-trace"

	ctx := peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})

	md := metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:mockPass"))
	ctx = metadata.NewIncomingContext(ctx, md)
	serverinfo := &grpc.UnaryServerInfo{
		FullMethod: "test",
	}

	s.info = &GrpcAccessInfo{
		ctx:      ctx,
		grpcInfo: serverinfo,
	}
}

func (s *GrpcAccessInfoSuite) TestErrorCode() {
	s.info.resp = &milvuspb.QueryResults{
		Status: merr.Status(nil),
	}
	result := Get(s.info, "$error_code")
	s.Equal(fmt.Sprint(0), result[0])

	s.info.resp = merr.Status(nil)
	result = Get(s.info, "$error_code")
	s.Equal(fmt.Sprint(0), result[0])
}

func (s *GrpcAccessInfoSuite) TestErrorMsg() {
	s.info.resp = &milvuspb.QueryResults{
		Status: merr.Status(merr.ErrChannelLack),
	}
	result := Get(s.info, "$error_msg")
	s.Equal(merr.ErrChannelLack.Error(), result[0])

	s.info.resp = merr.Status(merr.ErrChannelLack)
	result = Get(s.info, "$error_msg")
	s.Equal(merr.ErrChannelLack.Error(), result[0])

	// replace line breaks
	s.info.resp = merr.Status(fmt.Errorf("test error. stack: 1:\n 2:\n 3:\n"))
	result = Get(s.info, "$error_msg")
	s.Equal("test error. stack: 1:\\n 2:\\n 3:\\n", result[0])

	s.info.err = status.Errorf(codes.Unavailable, "mock")
	result = Get(s.info, "$error_msg")
	s.Equal("rpc error: code = Unavailable desc = mock", result[0])
}

func (s *GrpcAccessInfoSuite) TestErrorType() {
	s.info.resp = &milvuspb.QueryResults{
		Status: merr.Status(nil),
	}
	result := Get(s.info, "$error_type")
	s.Equal("", result[0])

	s.info.resp = merr.Status(merr.WrapErrAsInputError(merr.ErrParameterInvalid))
	result = Get(s.info, "$error_type")
	s.Equal(merr.InputError.String(), result[0])

	s.info.err = merr.ErrParameterInvalid
	result = Get(s.info, "$error_type")
	s.Equal(merr.SystemError.String(), result[0])
}

func (s *GrpcAccessInfoSuite) TestDbName() {
	s.info.req = nil
	result := Get(s.info, "$database_name")
	s.Equal(Unknown, result[0])

	s.info.req = &milvuspb.QueryRequest{
		DbName: "test",
	}
	result = Get(s.info, "$database_name")
	s.Equal("test", result[0])
}

func (s *GrpcAccessInfoSuite) TestSdkInfo() {
	ctx := context.Background()
	clientInfo := &commonpb.ClientInfo{
		SdkType:    "test",
		SdkVersion: "1.0",
	}

	s.info.ctx = ctx
	result := Get(s.info, "$sdk_version")
	s.Equal(Unknown, result[0])

	md := metadata.MD{}
	ctx = metadata.NewIncomingContext(ctx, md)
	s.info.ctx = ctx
	result = Get(s.info, "$sdk_version")
	s.Equal(Unknown, result[0])

	md = metadata.MD{util.HeaderUserAgent: []string{"invalid"}}
	ctx = metadata.NewIncomingContext(ctx, md)
	s.info.ctx = ctx
	result = Get(s.info, "$sdk_version")
	s.Equal(Unknown, result[0])

	md = metadata.MD{util.HeaderUserAgent: []string{"grpc-go.test"}}
	ctx = metadata.NewIncomingContext(ctx, md)
	s.info.ctx = ctx
	result = Get(s.info, "$sdk_version")
	s.Equal("Golang"+"-"+Unknown, result[0])

	s.info.req = &milvuspb.ConnectRequest{
		ClientInfo: clientInfo,
	}
	result = Get(s.info, "$sdk_version")
	s.Equal(clientInfo.SdkType+"-"+clientInfo.SdkVersion, result[0])

	identifier := 11111
	md = metadata.MD{util.IdentifierKey: []string{fmt.Sprint(identifier)}}
	ctx = metadata.NewIncomingContext(ctx, md)
	connection.GetManager().Register(ctx, int64(identifier), clientInfo)

	s.info.ctx = ctx
	result = Get(s.info, "$sdk_version")
	s.Equal(clientInfo.SdkType+"-"+clientInfo.SdkVersion, result[0])
}

func (s *GrpcAccessInfoSuite) TestExpression() {
	result := Get(s.info, "$method_expr")
	s.Equal(Unknown, result[0])

	testExpr := "test"
	s.info.req = &milvuspb.QueryRequest{
		Expr: testExpr,
	}
	result = Get(s.info, "$method_expr")
	s.Equal(testExpr, result[0])

	s.info.req = &milvuspb.SearchRequest{
		Dsl: testExpr,
	}
	result = Get(s.info, "$method_expr")
	s.Equal(testExpr, result[0])
}

func (s *GrpcAccessInfoSuite) TestOutputFields() {
	result := Get(s.info, "$output_fields")
	s.Equal(Unknown, result[0])

	fields := []string{"pk"}
	s.info.req = &milvuspb.QueryRequest{
		OutputFields: fields,
	}
	result = Get(s.info, "$output_fields")
	s.Equal(fmt.Sprint(fields), result[0])
}

func (s *GrpcAccessInfoSuite) TestConsistencyLevel() {
	result := Get(s.info, "$consistency_level")
	s.Equal(Unknown, result[0])

	s.info.req = &milvuspb.QueryRequest{
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	}
	result = Get(s.info, "$consistency_level")
	s.Equal(commonpb.ConsistencyLevel_Bounded.String(), result[0])
}

func (s *GrpcAccessInfoSuite) TestClusterPrefix() {
	cluster := "instance-test"
	paramtable.Init()
	ClusterPrefix.Store(cluster)

	result := Get(s.info, "$cluster_prefix")
	s.Equal(cluster, result[0])
}

func TestGrpcAccssInfo(t *testing.T) {
	suite.Run(t, new(GrpcAccessInfoSuite))
}
