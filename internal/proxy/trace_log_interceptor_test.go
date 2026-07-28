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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	ctx := GetContext(context.Background(), fmt.Sprintf("%s%s%s", "foo", util.CredentialSeparator, "FOO123456"))
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

		externalSpec := `{"extfs":{"cloud_provider":"aws","access_key_id":"AKIAEXAMPLE","access_key_value":"SUPERSECRET","region":"us-west-2"}}`
		f3 := GetRequestFieldWithoutSensitiveInfo(&milvuspb.RestoreExternalSnapshotRequest{
			DbName:               "db",
			TargetCollectionName: "restored",
			SnapshotMetadataUri:  "s3://bucket/export-root/snapshots/100/metadata/1.json",
			ExternalSpec:         externalSpec,
		})
		assert.NotContains(t, fmt.Sprint(f3.Interface), "AKIAEXAMPLE")
		assert.NotContains(t, fmt.Sprint(f3.Interface), "SUPERSECRET")
		assert.Contains(t, fmt.Sprint(f3.Interface), "***")

		f4 := GetRequestFieldWithoutSensitiveInfo(&milvuspb.ExportSnapshotRequest{
			DbName:         "db",
			CollectionName: "source",
			Name:           "snapshot",
			TargetS3Path:   "s3://bucket/export-root",
			ExternalSpec:   externalSpec,
		})
		assert.NotContains(t, fmt.Sprint(f4.Interface), "AKIAEXAMPLE")
		assert.NotContains(t, fmt.Sprint(f4.Interface), "SUPERSECRET")
		assert.Contains(t, fmt.Sprint(f4.Interface), "***")
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

// TestRedactReqForLogAndField covers the two call-site wrappers over
// elideRequestForLog: RedactReqForLog (wraps a template-bearing request, returns
// others unchanged) and the GetRequestFieldWithoutSensitiveInfo template branch.
func TestRedactReqForLogAndField(t *testing.T) {
	blob := []byte("BLOB-SECRET-MUST-NOT-LOG")
	tv := &schemapb.TemplateValue{Val: &schemapb.TemplateValue_BytesVal{BytesVal: blob}}
	withTmpl := &milvuspb.QueryRequest{ExprTemplateValues: map[string]*schemapb.TemplateValue{"bf": tv}}
	without := &milvuspb.QueryRequest{CollectionName: "c"}

	// RedactReqForLog: a template-bearing request is wrapped so String() elides
	// the blob; a request without templates is returned unchanged.
	wrapped := RedactReqForLog(withTmpl)
	assert.NotContains(t, fmt.Sprint(wrapped), string(blob))
	assert.Same(t, without, RedactReqForLog(without))

	// GetRequestFieldWithoutSensitiveInfo takes the wrapped-Stringer branch for a
	// template-bearing request (must not panic, must not leak the blob).
	f := GetRequestFieldWithoutSensitiveInfo(withTmpl)
	assert.NotContains(t, fmt.Sprint(f.Interface), string(blob))
}

// TestElideRequestForLog verifies every expression-template value — of any
// type, including a large bloom_match blob and a small scalar — is elided from
// the request's log string across all carriers (Search + sub_reqs, Query,
// Delete, HybridSearch), that the original request is restored afterwards (no
// clone, swap-restore), and that a request without template values is not
// wrapped.
func TestElideRequestForLog(t *testing.T) {
	blob := []byte("MBF1-a-very-large-binary-blob-that-must-not-be-logged")
	secret := "super-secret-scalar-value"
	newBytesTV := func() *schemapb.TemplateValue {
		return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_BytesVal{BytesVal: blob}}
	}
	newStrTV := func() *schemapb.TemplateValue {
		return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_StringVal{StringVal: secret}}
	}
	logLeaks := func(s fmt.Stringer) bool {
		out := s.String()
		return strings.Contains(out, string(blob)) || strings.Contains(out, secret)
	}
	origLeaks := func(m proto.Message) bool {
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		return strings.Contains(string(b), string(blob)) || strings.Contains(string(b), secret)
	}

	t.Run("search top-level and sub-reqs", func(t *testing.T) {
		req := &milvuspb.SearchRequest{
			ExprTemplateValues: map[string]*schemapb.TemplateValue{"bf": newBytesTV()},
			SubReqs: []*milvuspb.SubSearchRequest{
				{ExprTemplateValues: map[string]*schemapb.TemplateValue{"bf2": newBytesTV(), "s": newStrTV()}},
			},
		}
		wrapped, ok := elideRequestForLog(req)
		require.True(t, ok)
		assert.False(t, logLeaks(wrapped), "all template values must be elided in the log string")
		assert.True(t, origLeaks(req), "original request must be restored after stringify (no mutation)")
	})

	t.Run("query", func(t *testing.T) {
		req := &milvuspb.QueryRequest{ExprTemplateValues: map[string]*schemapb.TemplateValue{"bf": newBytesTV()}}
		wrapped, ok := elideRequestForLog(req)
		require.True(t, ok)
		assert.False(t, logLeaks(wrapped))
		assert.True(t, origLeaks(req))
	})

	t.Run("delete", func(t *testing.T) {
		req := &milvuspb.DeleteRequest{ExprTemplateValues: map[string]*schemapb.TemplateValue{"bf": newBytesTV()}}
		wrapped, ok := elideRequestForLog(req)
		require.True(t, ok)
		assert.False(t, logLeaks(wrapped))
	})

	t.Run("hybrid search sub-requests", func(t *testing.T) {
		req := &milvuspb.HybridSearchRequest{
			Requests: []*milvuspb.SearchRequest{
				{ExprTemplateValues: map[string]*schemapb.TemplateValue{"bf": newBytesTV(), "s": newStrTV()}},
			},
		}
		wrapped, ok := elideRequestForLog(req)
		require.True(t, ok)
		assert.False(t, logLeaks(wrapped))
	})

	t.Run("no template values: not wrapped", func(t *testing.T) {
		req := &milvuspb.SearchRequest{CollectionName: "c"}
		_, ok := elideRequestForLog(req)
		assert.False(t, ok, "requests without template values must not be wrapped")
	})
}
