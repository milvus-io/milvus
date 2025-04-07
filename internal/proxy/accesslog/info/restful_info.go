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
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/requestutil"
)

const (
	ContextUsername      = "username"
	ContextReturnCode    = "code"
	ContextReturnMessage = "message"
	ContextRequest       = "request"
	ContextToken         = "token"
)

type RestfulInfo struct {
	params *gin.LogFormatterParams
	start  time.Time
	req    interface{}
}

func NewRestfulInfo() *RestfulInfo {
	return &RestfulInfo{start: time.Now(), params: &gin.LogFormatterParams{}}
}

func (i *RestfulInfo) SetParams(p *gin.LogFormatterParams) {
	i.params = p
}

func (i *RestfulInfo) InitReq() {
	req, ok := i.params.Keys[ContextRequest]
	if !ok {
		return
	}
	i.req = req
}

func (i *RestfulInfo) TimeCost() string {
	return fmt.Sprint(i.params.Latency)
}

func (i *RestfulInfo) TimeNow() string {
	return time.Now().Format(timeFormat)
}

func (i *RestfulInfo) TimeStart() string {
	if i.start.IsZero() {
		return Unknown
	}
	return i.start.Format(timeFormat)
}

func (i *RestfulInfo) TimeEnd() string {
	return i.params.TimeStamp.Format(timeFormat)
}

func (i *RestfulInfo) MethodName() string {
	return i.params.Path
}

func (i *RestfulInfo) Address() string {
	return i.params.ClientIP
}

func (i *RestfulInfo) TraceID() string {
	traceID, ok := i.params.Keys["traceID"]
	if !ok {
		return Unknown
	}
	return traceID.(string)
}

func (i *RestfulInfo) MethodStatus() string {
	if i.params.StatusCode != http.StatusOK {
		return fmt.Sprintf("HttpError%d", i.params.StatusCode)
	}

	value, ok := i.params.Keys[ContextReturnCode]
	if !ok {
		return Unknown
	}

	code, ok := value.(int32)
	if ok {
		if code != 0 {
			return "Failed"
		}

		return "Successful"
	}

	return Unknown
}

func (i *RestfulInfo) UserName() string {
	username, ok := i.params.Keys[ContextUsername]
	if !ok || username == "" {
		return Unknown
	}

	return username.(string)
}

func (i *RestfulInfo) ResponseSize() string {
	return fmt.Sprint(i.params.BodySize)
}

func (i *RestfulInfo) ErrorCode() string {
	code, ok := i.params.Keys[ContextReturnCode]
	if !ok {
		return Unknown
	}
	return fmt.Sprint(code)
}

func (i *RestfulInfo) ErrorMsg() string {
	message, ok := i.params.Keys[ContextReturnMessage]
	if !ok {
		return ""
	}
	return strings.ReplaceAll(message.(string), "\n", "\\n")
}

func (i *RestfulInfo) ErrorType() string {
	return Unknown
}

func (i *RestfulInfo) SdkVersion() string {
	return "Restful"
}

func (i *RestfulInfo) DbName() string {
	name, ok := requestutil.GetDbNameFromRequest(i.req)
	if !ok {
		return Unknown
	}
	return name.(string)
}

func (i *RestfulInfo) CollectionName() string {
	name, ok := requestutil.GetCollectionNameFromRequest(i.req)
	if !ok {
		return Unknown
	}
	return name.(string)
}

func (i *RestfulInfo) PartitionName() string {
	name, ok := requestutil.GetPartitionNameFromRequest(i.req)
	if ok {
		return name.(string)
	}

	names, ok := requestutil.GetPartitionNamesFromRequest(i.req)
	if ok {
		return fmt.Sprint(names.([]string))
	}

	return Unknown
}

func (i *RestfulInfo) Expression() string {
	expr, ok := requestutil.GetExprFromRequest(i.req)
	if ok {
		return expr.(string)
	}

	if req, ok := i.req.(*milvuspb.HybridSearchRequest); ok {
		return listToString(lo.Map(req.GetRequests(), func(req *milvuspb.SearchRequest, _ int) string { return req.GetDsl() }))
	}

	dsl, ok := requestutil.GetDSLFromRequest(i.req)
	if ok {
		return dsl.(string)
	}
	return Unknown
}

func (i *RestfulInfo) OutputFields() string {
	fields, ok := requestutil.GetOutputFieldsFromRequest(i.req)
	if ok {
		return fmt.Sprint(fields.([]string))
	}
	return Unknown
}

func (i *RestfulInfo) ConsistencyLevel() string {
	level, ok := requestutil.GetConsistencyLevelFromRequst(i.req)
	if ok {
		return level.String()
	}
	return Unknown
}

func (i *RestfulInfo) AnnsField() string {
	if req, ok := i.req.(*milvuspb.SearchRequest); ok {
		return getAnnsFieldFromKvs(req.GetSearchParams())
	}

	if req, ok := i.req.(*milvuspb.HybridSearchRequest); ok {
		return listToString(lo.Map(req.GetRequests(), func(req *milvuspb.SearchRequest, _ int) string { return getAnnsFieldFromKvs(req.GetSearchParams()) }))
	}
	return Unknown
}
