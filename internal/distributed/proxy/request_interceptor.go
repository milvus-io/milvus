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

package grpcproxy

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/requestutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var retryableCode typeutil.Set[int32]

func init() {
	retryableCode = typeutil.NewSet(
		merr.Code(merr.ErrServiceRateLimit),
		merr.Code(merr.ErrCollectionSchemaMismatch),
	)
}

// UnaryRequestStatsInterceptor implements `grpc.UnaryServerInterceptor`
// it records incoming grpc request metrics in unified interceptor
//
// when some retirable error occurs, it will record it as `RetryLabel` instead of failure one
// when other interceptor rejects the request, it will record it as `RejectedLabel`
func UnaryRequestStatsInterceptor(ctx context.Context, req any, rpcInfo *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	methodTag := ParseShortMethodName(rpcInfo.FullMethod)
	db, _ := requestutil.GetDbNameFromRequest(req)
	collection, _ := requestutil.GetCollectionNameFromRequest(req)

	dbName := db.(string)
	collectionName := collection.(string)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		methodTag,
		metrics.TotalLabel,
		dbName,
		collectionName,
	).Inc()

	resp, err := handler(ctx, req)

	metrics.ProxyFunctionCall.WithLabelValues(
		strconv.FormatInt(paramtable.GetNodeID(), 10),
		methodTag,
		ParseMetricLabel(resp, err),
		dbName,
		collectionName,
	).Inc()

	return resp, err
}

// ParseShortMethodName parse short method name from full method name
// input like: "/milvus.proto.milvus.MilvusService/Search"
// returns "Search"
func ParseShortMethodName(fullMethodName string) string {
	parts := strings.Split(fullMethodName, "/")
	return parts[len(parts)-1]
}

func ParseMetricLabel(resp any, err error) string {
	// err only returned by interceptors
	if err != nil {
		return metrics.RejectedLabel
	}

	// check response status code
	var status *commonpb.Status
	switch resp := resp.(type) {
	case interface{ GetStatus() *commonpb.Status }:
		status = resp.GetStatus()
	case *commonpb.Status:
		status = resp
	}

	// check if retry
	if !merr.Ok(status) {
		// TODO use retriable if all set
		if retryableCode.Contain(status.GetCode()) {
			return metrics.RetryLabel
		}
		return metrics.FailLabel
	}
	return metrics.SuccessLabel
}
