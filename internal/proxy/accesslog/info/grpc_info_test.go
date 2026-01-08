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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
	s.info.resp = merr.Status(errors.New("test error. stack: 1:\n 2:\n 3:\n"))
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

func (s *GrpcAccessInfoSuite) TestNQ() {
	nq := int64(10)
	s.Equal(Unknown, Get(s.info, "$nq")[0])

	s.info.req = &milvuspb.SearchRequest{
		Nq: nq,
	}
	s.Equal(fmt.Sprintf("%d", nq), Get(s.info, "$nq")[0])

	s.info.req = &milvuspb.HybridSearchRequest{
		Requests: []*milvuspb.SearchRequest{{
			Nq: nq,
		}, {
			Nq: nq,
		}},
	}
	s.Equal("[\"10\", \"10\"]", Get(s.info, "$nq")[0])
}

func (s *GrpcAccessInfoSuite) TestSearchParams() {
	params := []*commonpb.KeyValuePair{{Key: "test_key", Value: "test_value"}}

	s.Equal(Unknown, Get(s.info, "$search_params")[0])

	s.info.req = &milvuspb.SearchRequest{
		SearchParams: params,
	}

	s.Equal(kvsToString(params), Get(s.info, "$search_params")[0])

	s.info.req = &milvuspb.HybridSearchRequest{
		Requests: []*milvuspb.SearchRequest{{SearchParams: params}, {SearchParams: params}},
	}

	s.Equal(listToString([]string{kvsToString(params), kvsToString(params)}), Get(s.info, "$search_params")[0])
}

func (s *GrpcAccessInfoSuite) TestQueryParams() {
	params := []*commonpb.KeyValuePair{{Key: "test_key", Value: "test_value"}}

	s.Equal(Unknown, Get(s.info, "$query_params")[0])

	s.info.req = &milvuspb.QueryRequest{
		QueryParams: params,
	}

	s.Equal(kvsToString(params), Get(s.info, "$query_params")[0])
}

func (s *GrpcAccessInfoSuite) TestTemplateValueLength() {
	// params := []*commonpb.KeyValuePair{{Key: "test_key", Value: "test_value"}}
	exprTemplValues := map[string]*schemapb.TemplateValue{
		"store_id": {
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{0, 1},
						},
					},
				},
			},
		},
	}

	s.info.req = &milvuspb.SearchRequest{
		Dsl:                "store_id in {store_id}",
		ExprTemplateValues: exprTemplValues,
	}

	s.Equal(`map[store_id:2]`, Get(s.info, "$template_value_length")[0])
}

func (s *GrpcAccessInfoSuite) TestExprTemplateValues() {
	// Test when request is nil or doesn't have template values
	s.info.req = nil
	s.Equal(NotAny, s.info.ExprTemplateValues())

	// Test with request that has no ExprTemplateValues field set (nil)
	s.info.req = &milvuspb.QueryRequest{
		Expr: "id > 0",
	}
	// When ExprTemplateValues is nil but the request supports it, it returns "map[]"
	s.Equal("map[]", s.info.ExprTemplateValues())

	// Test with LongData array
	exprTemplValuesLong := map[string]*schemapb.TemplateValue{
		"store_id": {
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
			},
		},
	}
	s.info.req = &milvuspb.SearchRequest{
		Dsl:                "store_id in {store_id}",
		ExprTemplateValues: exprTemplValuesLong,
	}
	result := s.info.ExprTemplateValues()
	s.Contains(result, "store_id")
	s.NotEqual(NotAny, result)

	// Test with StringData array
	exprTemplValuesString := map[string]*schemapb.TemplateValue{
		"name": {
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"Alice", "Bob", "Charlie"},
						},
					},
				},
			},
		},
	}
	s.info.req = &milvuspb.QueryRequest{
		Expr:               "name in {name}",
		ExprTemplateValues: exprTemplValuesString,
	}
	result = s.info.ExprTemplateValues()
	s.Contains(result, "name")
	s.NotEqual(NotAny, result)

	// Test with BoolData array
	exprTemplValuesBool := map[string]*schemapb.TemplateValue{
		"is_active": {
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_BoolData{
						BoolData: &schemapb.BoolArray{
							Data: []bool{true, false, true},
						},
					},
				},
			},
		},
	}
	s.info.req = &milvuspb.QueryRequest{
		Expr:               "is_active in {is_active}",
		ExprTemplateValues: exprTemplValuesBool,
	}
	result = s.info.ExprTemplateValues()
	s.Contains(result, "is_active")
	s.NotEqual(NotAny, result)

	// Test with DoubleData array
	exprTemplValuesDouble := map[string]*schemapb.TemplateValue{
		"price": {
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{9.99, 19.99, 29.99},
						},
					},
				},
			},
		},
	}
	s.info.req = &milvuspb.SearchRequest{
		Dsl:                "price in {price}",
		ExprTemplateValues: exprTemplValuesDouble,
	}
	result = s.info.ExprTemplateValues()
	s.Contains(result, "price")
	s.NotEqual(NotAny, result)

	// Test with multiple template values
	exprTemplValuesMultiple := map[string]*schemapb.TemplateValue{
		"store_id": {
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
			},
		},
		"category": {
			Val: &schemapb.TemplateValue_ArrayVal{
				ArrayVal: &schemapb.TemplateArrayValue{
					Data: &schemapb.TemplateArrayValue_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"electronics", "books"},
						},
					},
				},
			},
		},
	}
	s.info.req = &milvuspb.QueryRequest{
		Expr:               "store_id in {store_id} and category in {category}",
		ExprTemplateValues: exprTemplValuesMultiple,
	}
	result = s.info.ExprTemplateValues()
	s.Contains(result, "store_id")
	s.Contains(result, "category")
	s.NotEqual(NotAny, result)

	// Test with single value (non-array)
	exprTemplValuesSingle := map[string]*schemapb.TemplateValue{
		"threshold": {
			Val: &schemapb.TemplateValue_Int64Val{
				Int64Val: 100,
			},
		},
	}
	s.info.req = &milvuspb.SearchRequest{
		Dsl:                "count > {threshold}",
		ExprTemplateValues: exprTemplValuesSingle,
	}
	result = s.info.ExprTemplateValues()
	s.Contains(result, "threshold")
	s.NotEqual(NotAny, result)

	// Test with empty template values map
	s.info.req = &milvuspb.QueryRequest{
		Expr:               "id > 0",
		ExprTemplateValues: map[string]*schemapb.TemplateValue{},
	}
	result = s.info.ExprTemplateValues()
	s.Equal("map[]", result)
}

func TestGrpcAccssInfo(t *testing.T) {
	suite.Run(t, new(GrpcAccessInfoSuite))
}
