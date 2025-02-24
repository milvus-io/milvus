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
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v2/tracer"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type LogFormatterSuite struct {
	suite.Suite

	serverinfo *grpc.UnaryServerInfo
	reqs       []interface{}
	resps      []interface{}
	errs       []error
	username   string
	traceID    string
	ctx        context.Context
}

func (s *LogFormatterSuite) SetupSuite() {
	s.username = "test-user"
	s.ctx = peer.NewContext(
		context.Background(),
		&peer.Peer{
			Addr: &net.IPAddr{
				IP:   net.IPv4(0, 0, 0, 0),
				Zone: "test",
			},
		})

	md := metadata.Pairs(util.HeaderAuthorize, crypto.Base64Encode("mockUser:mockPass"))
	s.ctx = metadata.NewIncomingContext(s.ctx, md)
	s.traceID = "test-trace"
	s.serverinfo = &grpc.UnaryServerInfo{
		FullMethod: "test",
	}
	s.reqs = []interface{}{
		&milvuspb.QueryRequest{
			DbName:         "test-db",
			CollectionName: "test-collection",
			PartitionNames: []string{"test-partition-1", "test-partition-2"},
		},
		&milvuspb.SearchRequest{
			DbName:         "test-db",
			CollectionName: "test-collection",
			PartitionNames: []string{"test-partition-1", "test-partition-2"},
		},
		&milvuspb.InsertRequest{
			DbName:         "test-db",
			CollectionName: "test-collection",
			PartitionName:  "test-partition-1",
		},
	}

	s.resps = []interface{}{
		&milvuspb.QueryResults{
			Status: merr.Status(nil),
		},
		&milvuspb.MutationResult{
			Status: merr.Status(merr.ErrCollectionNotFound),
		},
		merr.Status(nil),
	}

	s.errs = []error{nil, nil, status.Errorf(codes.Unavailable, "")}
}

func (s *LogFormatterSuite) TestFormatNames() {
	fmt := "{$database_name}: $collection_name: $partition_name"
	formatter := NewFormatter(fmt)

	for _, req := range s.reqs {
		i := info.NewGrpcAccessInfo(s.ctx, s.serverinfo, req)
		fs := formatter.Format(i)
		s.False(strings.Contains(fs, info.Unknown))
	}

	i := info.NewGrpcAccessInfo(s.ctx, s.serverinfo, nil)
	fs := formatter.Format(i)
	s.True(strings.Contains(fs, info.Unknown))
}

func (s *LogFormatterSuite) TestFormatTime() {
	fmt := "$time_now: $time_start: $time_end: $time_cost: $time_now"
	formatter := NewFormatter(fmt)

	for id, req := range s.reqs {
		i := info.NewGrpcAccessInfo(s.ctx, s.serverinfo, req)
		fs := formatter.Format(i)
		s.True(strings.Contains(fs, info.Unknown))
		i.UpdateCtx(s.ctx)
		i.SetResult(s.resps[id], s.errs[id])
		fs = formatter.Format(i)
		s.False(strings.Contains(fs, info.Unknown))
	}
}

func (s *LogFormatterSuite) TestFormatUserInfo() {
	fmt := "$user_name: $user_addr"
	formatter := NewFormatter(fmt)

	for _, req := range s.reqs {
		i := info.NewGrpcAccessInfo(s.ctx, s.serverinfo, req)
		fs := formatter.Format(i)
		s.False(strings.Contains(fs, info.Unknown))
	}

	// test unknown
	i := info.NewGrpcAccessInfo(context.Background(), &grpc.UnaryServerInfo{}, nil)
	fs := formatter.Format(i)
	s.True(strings.Contains(fs, info.Unknown))
}

func (s *LogFormatterSuite) TestFormatMethodInfo() {
	fmt := "$method_name: $method_status $trace_id"
	formatter := NewFormatter(fmt)

	metaContext := metadata.AppendToOutgoingContext(s.ctx, info.ClientRequestIDKey, s.traceID)
	for _, req := range s.reqs {
		i := info.NewGrpcAccessInfo(metaContext, s.serverinfo, req)
		fs := formatter.Format(i)
		s.True(strings.Contains(fs, s.traceID))
	}

	tracer.Init()
	traceContext, traceSpan := otel.Tracer(typeutil.ProxyRole).Start(s.ctx, "test")
	trueTraceID := traceSpan.SpanContext().TraceID().String()
	for _, req := range s.reqs {
		i := info.NewGrpcAccessInfo(traceContext, s.serverinfo, req)
		fs := formatter.Format(i)
		s.True(strings.Contains(fs, trueTraceID))
	}
}

func (s *LogFormatterSuite) TestFormatMethodResult() {
	fmt := "$method_name: $method_status $response_size $error_code $error_msg"
	formatter := NewFormatter(fmt)

	for id, req := range s.reqs {
		i := info.NewGrpcAccessInfo(s.ctx, s.serverinfo, req)
		fs := formatter.Format(i)
		s.True(strings.Contains(fs, info.Unknown))

		i.SetResult(s.resps[id], s.errs[id])
		fs = formatter.Format(i)
		s.False(strings.Contains(fs, info.Unknown))
	}
}

func (s *LogFormatterSuite) TestParseConfigKeyFailed() {
	configKey := ".testf.invalidSub"
	_, _, err := parseConfigKey(configKey)
	s.Error(err)
}

func TestLogFormatter(t *testing.T) {
	suite.Run(t, new(LogFormatterSuite))
}
