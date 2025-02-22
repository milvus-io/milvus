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
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type RestfulAccessInfoSuite struct {
	suite.Suite

	username string
	traceID  string
	info     *RestfulInfo
}

func (s *RestfulAccessInfoSuite) SetupSuite() {
	paramtable.Init()
}

func (s *RestfulAccessInfoSuite) SetupTest() {
	s.username = "test-user"
	s.traceID = "test-trace"
	s.info = &RestfulInfo{}
	s.info.SetParams(
		&gin.LogFormatterParams{
			Keys: make(map[string]any),
		})
}

func (s *RestfulAccessInfoSuite) TestTimeCost() {
	s.info.params.Latency = time.Second
	result := Get(s.info, "$time_cost")
	s.Equal(fmt.Sprint(time.Second), result[0])
}

func (s *RestfulAccessInfoSuite) TestTimeNow() {
	result := Get(s.info, "$time_now")
	s.NotEqual(Unknown, result[0])
}

func (s *RestfulAccessInfoSuite) TestTimeStart() {
	result := Get(s.info, "$time_start")
	s.Equal(Unknown, result[0])

	s.info.start = time.Now()
	result = Get(s.info, "$time_start")
	s.Equal(s.info.start.Format(timeFormat), result[0])
}

func (s *RestfulAccessInfoSuite) TestTimeEnd() {
	s.info.params.TimeStamp = time.Now()
	result := Get(s.info, "$time_end")
	s.Equal(s.info.params.TimeStamp.Format(timeFormat), result[0])
}

func (s *RestfulAccessInfoSuite) TestMethodName() {
	s.info.params.Path = "/restful/test"
	result := Get(s.info, "$method_name")
	s.Equal(s.info.params.Path, result[0])
}

func (s *RestfulAccessInfoSuite) TestAddress() {
	s.info.params.ClientIP = "127.0.0.1"
	result := Get(s.info, "$user_addr")
	s.Equal(s.info.params.ClientIP, result[0])
}

func (s *RestfulAccessInfoSuite) TestTraceID() {
	result := Get(s.info, "$trace_id")
	s.Equal(Unknown, result[0])

	s.info.params.Keys["traceID"] = "testtrace"
	result = Get(s.info, "$trace_id")
	s.Equal(s.info.params.Keys["traceID"], result[0])
}

func (s *RestfulAccessInfoSuite) TestStatus() {
	s.info.params.StatusCode = http.StatusBadRequest
	result := Get(s.info, "$method_status")
	s.Equal("HttpError400", result[0])

	s.info.params.StatusCode = http.StatusOK
	s.info.params.Keys[ContextReturnCode] = merr.Code(merr.ErrChannelLack)
	result = Get(s.info, "$method_status")
	s.Equal("Failed", result[0])

	s.info.params.StatusCode = http.StatusOK
	s.info.params.Keys[ContextReturnCode] = merr.Code(nil)
	result = Get(s.info, "$method_status")
	s.Equal("Successful", result[0])
}

func (s *RestfulAccessInfoSuite) TestErrorCode() {
	result := Get(s.info, "$error_code")
	s.Equal(Unknown, result[0])

	s.info.params.Keys[ContextReturnCode] = 200
	result = Get(s.info, "$error_code")
	s.Equal(fmt.Sprint(200), result[0])
}

func (s *RestfulAccessInfoSuite) TestErrorMsg() {
	s.info.params.Keys[ContextReturnMessage] = merr.ErrChannelLack.Error()
	result := Get(s.info, "$error_msg")
	s.Equal(merr.ErrChannelLack.Error(), result[0])

	s.info.params.Keys[ContextReturnMessage] = "test error. stack: 1:\n 2:\n 3:\n"
	result = Get(s.info, "$error_msg")
	s.Equal("test error. stack: 1:\\n 2:\\n 3:\\n", result[0])
}

func (s *RestfulAccessInfoSuite) TestDbName() {
	result := Get(s.info, "$database_name")
	s.Equal(Unknown, result[0])

	req := &milvuspb.QueryRequest{
		DbName: "test",
	}
	s.info.req = req
	result = Get(s.info, "$database_name")
	s.Equal("test", result[0])
}

func (s *RestfulAccessInfoSuite) TestSdkInfo() {
	result := Get(s.info, "$sdk_version")
	s.Equal("Restful", result[0])
}

func (s *RestfulAccessInfoSuite) TestExpression() {
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

func (s *RestfulAccessInfoSuite) TestOutputFields() {
	result := Get(s.info, "$output_fields")
	s.Equal(Unknown, result[0])

	fields := []string{"pk"}
	s.info.params.Keys[ContextRequest] = &milvuspb.QueryRequest{
		OutputFields: fields,
	}
	s.info.InitReq()
	result = Get(s.info, "$output_fields")
	s.Equal(fmt.Sprint(fields), result[0])
}

func (s *RestfulAccessInfoSuite) TestConsistencyLevel() {
	result := Get(s.info, "$consistency_level")
	s.Equal(Unknown, result[0])

	s.info.params.Keys[ContextRequest] = &milvuspb.QueryRequest{
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	}
	s.info.InitReq()
	result = Get(s.info, "$consistency_level")
	s.Equal(commonpb.ConsistencyLevel_Bounded.String(), result[0])
}

func (s *RestfulAccessInfoSuite) TestClusterPrefix() {
	cluster := "instance-test"
	paramtable.Init()
	ClusterPrefix.Store(cluster)

	result := Get(s.info, "$cluster_prefix")
	s.Equal(cluster, result[0])
}

func TestRestfulAccessInfo(t *testing.T) {
	suite.Run(t, new(RestfulAccessInfoSuite))
}
