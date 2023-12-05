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

package accesslog

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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type GrpcAccessInfoSuite struct {
	suite.Suite

	username string
	traceID  string
	info     *GrpcAccessInfo
}

func (s *GrpcAccessInfoSuite) SetupSuite() {
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
		ctx:  ctx,
		info: serverinfo,
	}
}

func (s *GrpcAccessInfoSuite) TestErrorCode() {
	s.info.resp = &milvuspb.QueryResults{
		Status: merr.Status(nil),
	}
	result := s.info.Get("$error_code")
	s.Equal(fmt.Sprint(0), result[0])

	s.info.resp = merr.Status(nil)
	result = s.info.Get("$error_code")
	s.Equal(fmt.Sprint(0), result[0])
}

func (s *GrpcAccessInfoSuite) TestErrorMsg() {
	s.info.resp = &milvuspb.QueryResults{
		Status: merr.Status(merr.ErrChannelLack),
	}
	result := s.info.Get("$error_msg")
	s.Equal(merr.ErrChannelLack.Error(), result[0])

	s.info.resp = merr.Status(merr.ErrChannelLack)
	result = s.info.Get("$error_msg")
	s.Equal(merr.ErrChannelLack.Error(), result[0])

	s.info.err = status.Errorf(codes.Unavailable, "mock")
	result = s.info.Get("$error_msg")
	s.Equal("rpc error: code = Unavailable desc = mock", result[0])
}

func (s *GrpcAccessInfoSuite) TestDbName() {
	s.info.req = nil
	result := s.info.Get("$database_name")
	s.Equal(unknownString, result[0])

	s.info.req = &milvuspb.QueryRequest{
		DbName: "test",
	}
	result = s.info.Get("$database_name")
	s.Equal("test", result[0])
}

func TestGrpcAccssInfo(t *testing.T) {
	suite.Run(t, new(GrpcAccessInfoSuite))
}
