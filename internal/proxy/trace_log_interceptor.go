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
	"path"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/requestutil"
)

func TraceLogInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	switch Params.CommonCfg.TraceLogMode.GetAsInt() {
	case 0: // none
		return handler(ctx, req)
	case 1: // simple info
		fields := GetRequestBaseInfo(ctx, req, info, false)
		mlog.Info(ctx, "trace info: simple", fields...)
		return handler(ctx, req)
	case 2: // detail info
		fields := GetRequestBaseInfo(ctx, req, info, true)
		fields = append(fields, GetRequestFieldWithoutSensitiveInfo(req))
		mlog.Info(ctx, "trace info: detail", fields...)
		return handler(ctx, req)
	case 3: // detail info with request and response
		fields := GetRequestBaseInfo(ctx, req, info, true)
		fields = append(fields, GetRequestFieldWithoutSensitiveInfo(req))
		mlog.Info(ctx, "trace info: all request", fields...)
		resp, err := handler(ctx, req)
		if err != nil {
			mlog.Info(ctx, "trace info: all, error", mlog.Err(err))
			return resp, err
		}
		if status, ok := requestutil.GetStatusFromResponse(resp); ok {
			if status.Code != 0 {
				mlog.Info(ctx, "trace info: all, fail", mlog.Any("resp", resp))
			}
		} else {
			mlog.Info(ctx, "trace info: all, unknown", mlog.Any("resp", resp))
		}
		return resp, nil
	default:
		return handler(ctx, req)
	}
}

func GetRequestBaseInfo(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, skipBaseRequestInfo bool) []mlog.Field {
	var fields []mlog.Field

	_, requestName := path.Split(info.FullMethod)
	fields = append(fields, mlog.String("request_name", requestName))

	username, err := GetCurUserFromContext(ctx)
	if err == nil && username != "" {
		fields = append(fields, mlog.String("username", username))
	}

	if !skipBaseRequestInfo {
		for baseInfoName, f := range requestutil.TraceLogBaseInfoFuncMap {
			baseInfo, ok := f(req)
			if !ok {
				continue
			}
			fields = append(fields, mlog.Any(baseInfoName, baseInfo))
		}
	}

	return fields
}

func GetRequestFieldWithoutSensitiveInfo(req interface{}) mlog.Field {
	createCredentialReq, ok := req.(*milvuspb.CreateCredentialRequest)
	if ok {
		return mlog.Any("request", &milvuspb.CreateCredentialRequest{
			Base:                  createCredentialReq.Base,
			Username:              createCredentialReq.Username,
			CreatedUtcTimestamps:  createCredentialReq.CreatedUtcTimestamps,
			ModifiedUtcTimestamps: createCredentialReq.ModifiedUtcTimestamps,
		})
	}
	updateCredentialReq, ok := req.(*milvuspb.UpdateCredentialRequest)
	if ok {
		return mlog.Any("request", &milvuspb.UpdateCredentialRequest{
			Base:                  updateCredentialReq.Base,
			Username:              updateCredentialReq.Username,
			CreatedUtcTimestamps:  updateCredentialReq.CreatedUtcTimestamps,
			ModifiedUtcTimestamps: updateCredentialReq.ModifiedUtcTimestamps,
		})
	}
	restoreExternalSnapshotReq, ok := req.(*milvuspb.RestoreExternalSnapshotRequest)
	if ok {
		redactedReq := proto.Clone(restoreExternalSnapshotReq).(*milvuspb.RestoreExternalSnapshotRequest)
		redactedReq.ExternalSpec = externalspec.RedactExternalSpec(redactedReq.GetExternalSpec())
		return mlog.Any("request", redactedReq)
	}
	exportSnapshotReq, ok := req.(*milvuspb.ExportSnapshotRequest)
	if ok {
		redactedReq := proto.Clone(exportSnapshotReq).(*milvuspb.ExportSnapshotRequest)
		redactedReq.ExternalSpec = externalspec.RedactExternalSpec(redactedReq.GetExternalSpec())
		return mlog.Any("request", redactedReq)
	}
	// expression-template values are user-supplied data (and a bloom_match
	// filter blob can be tens of MiB) — never log them verbatim. Wrap the
	// request in a lazy Stringer that elides every template value at log time,
	// with NO clone of the (up-to-tens-of-MiB) request. Covers every carrier /
	// nesting: Search (+ sub_reqs), Query, Delete, HybridSearch. Requests with
	// no template value are logged as-is.
	if wrapped, ok := elideRequestForLog(req); ok {
		return mlog.Stringer("request", wrapped)
	}
	return mlog.Any("request", req)
}

// RedactReqForLog returns req wrapped so its String() elides every
// expression-template value, or req unchanged when it carries none. For call
// sites that log the raw request outside the trace interceptor (e.g. hook
// before/after errors).
func RedactReqForLog(req interface{}) interface{} {
	if wrapped, ok := elideRequestForLog(req); ok {
		return wrapped
	}
	return req
}

// elidedTemplateValue is the shared read-only marker swapped in for a template
// value during logging.
var elidedTemplateValue = &schemapb.TemplateValue{
	Val: &schemapb.TemplateValue_StringVal{StringVal: "<elided>"},
}

// elideRequestForLog wraps req in a lazy Stringer that elides its
// expression-template values, or returns (nil, false) when the request carries
// none (so the caller logs it as-is). No clone is made.
func elideRequestForLog(req interface{}) (fmt.Stringer, bool) {
	// Generated proto request messages implement fmt.Stringer; that String() is
	// what a logged request renders through, so wrap it.
	sr, ok := req.(fmt.Stringer)
	if !ok {
		return nil, false
	}
	maps := requestTemplateMaps(req)
	if len(maps) == 0 {
		return nil, false
	}
	return elidedRequest{req: sr, maps: maps}, true
}

// requestTemplateMaps returns every non-empty expr_template_values map carried
// by req (top level + nested sub-requests). nil if the request type carries
// none.
func requestTemplateMaps(req interface{}) []map[string]*schemapb.TemplateValue {
	switch r := req.(type) {
	case *milvuspb.SearchRequest:
		return searchTemplateMaps(r)
	case *milvuspb.QueryRequest:
		return nonEmptyTemplateMap(r.GetExprTemplateValues())
	case *milvuspb.DeleteRequest:
		return nonEmptyTemplateMap(r.GetExprTemplateValues())
	case *milvuspb.HybridSearchRequest:
		var maps []map[string]*schemapb.TemplateValue
		for _, sr := range r.GetRequests() {
			maps = append(maps, searchTemplateMaps(sr)...)
		}
		return maps
	default:
		return nil
	}
}

func searchTemplateMaps(r *milvuspb.SearchRequest) []map[string]*schemapb.TemplateValue {
	maps := nonEmptyTemplateMap(r.GetExprTemplateValues())
	for _, sub := range r.GetSubReqs() {
		maps = append(maps, nonEmptyTemplateMap(sub.GetExprTemplateValues())...)
	}
	return maps
}

func nonEmptyTemplateMap(m map[string]*schemapb.TemplateValue) []map[string]*schemapb.TemplateValue {
	if len(m) == 0 {
		return nil
	}
	return []map[string]*schemapb.TemplateValue{m}
}

// elidedRequest lazily stringifies req with every expression-template value
// swapped in place for an {elided} marker, restoring the originals afterwards —
// no clone of the request. Safe because zap evaluates a Stringer field
// synchronously in the logging goroutine, before the request reaches the
// handler, so no other reader observes the temporary state.
type elidedRequest struct {
	req  fmt.Stringer
	maps []map[string]*schemapb.TemplateValue
}

func (e elidedRequest) String() string {
	type savedEntry struct {
		m map[string]*schemapb.TemplateValue
		k string
		v *schemapb.TemplateValue
	}
	var back []savedEntry
	for _, m := range e.maps {
		for k, v := range m {
			back = append(back, savedEntry{m, k, v})
			m[k] = elidedTemplateValue
		}
	}
	defer func() {
		for _, s := range back {
			s.m[s.k] = s.v
		}
	}()
	return e.req.String()
}
