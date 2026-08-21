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
	restoreRBACReq, ok := req.(*milvuspb.RestoreRBACMetaRequest)
	if ok {
		return mlog.Any("request", redactRestoreRBACRequestForLog(restoreRBACReq))
	}
	createCollectionReq, ok := req.(*milvuspb.CreateCollectionRequest)
	if ok {
		return mlog.Any("request", redactCreateCollectionRequestForLog(createCollectionReq))
	}
	refreshExternalCollectionReq, ok := req.(*milvuspb.RefreshExternalCollectionRequest)
	if ok {
		if refreshExternalCollectionReq == nil {
			return mlog.Any("request", refreshExternalCollectionReq)
		}
		redactedReq := proto.Clone(refreshExternalCollectionReq).(*milvuspb.RefreshExternalCollectionRequest)
		redactedReq.ExternalSource = externalspec.RedactExternalSource(redactedReq.GetExternalSource())
		redactedReq.ExternalSpec = externalspec.RedactExternalSpecForLog(redactedReq.GetExternalSpec())
		return mlog.Any("request", redactedReq)
	}
	restoreExternalSnapshotReq, ok := req.(*milvuspb.RestoreExternalSnapshotRequest)
	if ok {
		redactedReq := proto.Clone(restoreExternalSnapshotReq).(*milvuspb.RestoreExternalSnapshotRequest)
		redactedReq.ExternalSpec = externalspec.RedactExternalSpecForLog(redactedReq.GetExternalSpec())
		return mlog.Any("request", redactedReq)
	}
	exportSnapshotReq, ok := req.(*milvuspb.ExportSnapshotRequest)
	if ok {
		redactedReq := proto.Clone(exportSnapshotReq).(*milvuspb.ExportSnapshotRequest)
		redactedReq.ExternalSpec = externalspec.RedactExternalSpecForLog(redactedReq.GetExternalSpec())
		return mlog.Any("request", redactedReq)
	}
	return mlog.Any("request", req)
}

func redactRestoreRBACRequestForLog(req *milvuspb.RestoreRBACMetaRequest) *milvuspb.RestoreRBACMetaRequest {
	if req == nil {
		return nil
	}
	redacted := proto.Clone(req).(*milvuspb.RestoreRBACMetaRequest)
	for _, user := range redacted.GetRBACMeta().GetUsers() {
		if user != nil {
			user.Password = sensitiveMark
		}
	}
	return redacted
}

func redactCreateCollectionRequestForLog(req *milvuspb.CreateCollectionRequest) *milvuspb.CreateCollectionRequest {
	if req == nil {
		return nil
	}

	redactedReq := proto.Clone(req).(*milvuspb.CreateCollectionRequest)
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(req.GetSchema(), schema); err != nil {
		redactedReq.Schema = nil
		return redactedReq
	}
	redactedSchema, err := proto.Marshal(externalspec.RedactCollectionSchemaForLog(schema))
	if err != nil {
		redactedReq.Schema = nil
		return redactedReq
	}
	redactedReq.Schema = redactedSchema
	return redactedReq
}
