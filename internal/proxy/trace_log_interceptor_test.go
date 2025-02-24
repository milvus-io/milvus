/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proxy

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestTraceLogInterceptor(t *testing.T) {
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	}

	// none
	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TraceLogMode.Key, "0")
	_, _ = TraceLogInterceptor(context.Background(), &milvuspb.ShowCollectionsRequest{}, &grpc.UnaryServerInfo{}, handler)

	// invalid mode
	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TraceLogMode.Key, "10")
	_, _ = TraceLogInterceptor(context.Background(), &milvuspb.ShowCollectionsRequest{}, &grpc.UnaryServerInfo{}, handler)

	// simple mode
	ctx := GetContext(context.Background(), fmt.Sprintf("%s%s%s", "foo", util.CredentialSeperator, "FOO123456"))
	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TraceLogMode.Key, "1")
	{
		_, _ = TraceLogInterceptor(ctx, &milvuspb.CreateCollectionRequest{
			DbName:         "db",
			CollectionName: "col1",
		}, &grpc.UnaryServerInfo{
			FullMethod: "/milvus.proto.milvus.MilvusService/CreateCollection",
		}, handler)
	}

	// detail mode
	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TraceLogMode.Key, "2")
	{
		_, _ = TraceLogInterceptor(ctx, &milvuspb.CreateCollectionRequest{
			DbName:         "db",
			CollectionName: "col1",
		}, &grpc.UnaryServerInfo{
			FullMethod: "/milvus.proto.milvus.MilvusService/CreateCollection",
		}, handler)
	}

	{
		f1 := GetRequestFieldWithoutSensitiveInfo(&milvuspb.CreateCredentialRequest{
			Username: "foo",
			Password: "123456",
		})
		assert.NotContains(t, strings.ToLower(fmt.Sprint(f1.Interface)), "password")

		f2 := GetRequestFieldWithoutSensitiveInfo(&milvuspb.UpdateCredentialRequest{
			Username:    "foo",
			OldPassword: "123456",
			NewPassword: "FOO123456",
		})
		assert.NotContains(t, strings.ToLower(fmt.Sprint(f2.Interface)), "password")
	}

	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TraceLogMode.Key, "3")
	{
		_, _ = TraceLogInterceptor(ctx, &milvuspb.CreateCollectionRequest{
			DbName:         "db",
			CollectionName: "col1",
		}, &grpc.UnaryServerInfo{
			FullMethod: "/milvus.proto.milvus.MilvusService/CreateCollection",
		}, func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, errors.New("internet error")
		})
	}
	{
		_, _ = TraceLogInterceptor(ctx, &milvuspb.CreateCollectionRequest{
			DbName:         "db",
			CollectionName: "col1",
		}, &grpc.UnaryServerInfo{
			FullMethod: "/milvus.proto.milvus.MilvusService/CreateCollection",
		}, func(ctx context.Context, req interface{}) (interface{}, error) {
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Code: 500}, nil
		})
	}
	{
		_, _ = TraceLogInterceptor(ctx, &milvuspb.CreateCollectionRequest{
			DbName:         "db",
			CollectionName: "col1",
		}, &grpc.UnaryServerInfo{
			FullMethod: "/milvus.proto.milvus.MilvusService/CreateCollection",
		}, func(ctx context.Context, req interface{}) (interface{}, error) {
			return "foo", nil
		})
	}
	{
		_, _ = TraceLogInterceptor(ctx, &milvuspb.ShowCollectionsRequest{
			DbName: "db",
		}, &grpc.UnaryServerInfo{
			FullMethod: "/milvus.proto.milvus.MilvusService/ShowCollections",
		}, func(ctx context.Context, req interface{}) (interface{}, error) {
			return &milvuspb.ShowCollectionsResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				CollectionNames: []string{"col1"},
			}, nil
		})
	}
	_ = paramtable.Get().Save(paramtable.Get().CommonCfg.TraceLogMode.Key, "0")
}
