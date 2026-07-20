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

package requestutil

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CollectionNameGetter interface {
	GetCollectionName() string
}

func GetCollectionNameFromRequest(req any) (any, bool) {
	getter, ok := req.(CollectionNameGetter)
	if !ok {
		return "", false
	}
	return getter.GetCollectionName(), true
}

type CollectionNamesGetter interface {
	GetCollectionNames() []string
}

func GetCollectionNamesFromRequest(req any) (any, bool) {
	getter, ok := req.(CollectionNamesGetter)
	if !ok {
		return nil, false
	}
	return getter.GetCollectionNames(), true
}

type DBNameGetter interface {
	GetDbName() string
}

func GetDbNameFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(DBNameGetter)
	if !ok {
		return "", false
	}
	return getter.GetDbName(), true
}

type PartitionNameGetter interface {
	GetPartitionName() string
}

func GetPartitionNameFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(PartitionNameGetter)
	if !ok {
		return "", false
	}
	return getter.GetPartitionName(), true
}

type PartitionNamesGetter interface {
	GetPartitionNames() []string
}

func GetPartitionNamesFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(PartitionNamesGetter)
	if !ok {
		return nil, false
	}
	return getter.GetPartitionNames(), true
}

type FieldNameGetter interface {
	GetFieldName() string
}

func GetFieldNameFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(FieldNameGetter)
	if !ok {
		return "", false
	}
	return getter.GetFieldName(), true
}

type OutputFieldsGetter interface {
	GetOutputFields() []string
}

func GetOutputFieldsFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(OutputFieldsGetter)
	if !ok {
		return nil, false
	}
	return getter.GetOutputFields(), true
}

type QueryParamsGetter interface {
	GetQueryParams() []*commonpb.KeyValuePair
}

func GetQueryParamsFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(QueryParamsGetter)
	if !ok {
		return nil, false
	}
	return getter.GetQueryParams(), true
}

type ExprGetter interface {
	GetExpr() string
}

func GetExprFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(ExprGetter)
	if !ok {
		return "", false
	}
	return getter.GetExpr(), true
}

type SearchParamsGetter interface {
	GetSearchParams() []*commonpb.KeyValuePair
}

func GetSearchParamsFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(SearchParamsGetter)
	if !ok {
		return nil, false
	}
	return getter.GetSearchParams(), true
}

type DSLGetter interface {
	GetDsl() string
}

func GetDSLFromRequest(req interface{}) (any, bool) {
	getter, ok := req.(DSLGetter)
	if !ok {
		return "", false
	}
	return getter.GetDsl(), true
}

type StatusGetter interface {
	GetStatus() *commonpb.Status
}

type AlterStatusGetter interface {
	GetAlterStatus() *commonpb.Status
}

func GetStatusFromResponse(resp interface{}) (*commonpb.Status, bool) {
	status, ok := resp.(*commonpb.Status)
	if ok {
		return status, status != nil
	}
	getter, ok := resp.(StatusGetter)
	if ok {
		status := getter.GetStatus()
		return status, status != nil
	}
	alterGetter, ok := resp.(AlterStatusGetter)
	if ok {
		status := alterGetter.GetAlterStatus()
		return status, status != nil
	}
	return nil, false
}

type ConsistencyLevelGetter interface {
	GetConsistencyLevel() commonpb.ConsistencyLevel
}

func GetConsistencyLevelFromRequst(req interface{}) (commonpb.ConsistencyLevel, bool) {
	getter, ok := req.(ConsistencyLevelGetter)
	if !ok {
		return 0, false
	}
	return getter.GetConsistencyLevel(), true
}

// TemplateValuesGetter is the common interface for getting expr template values from milvus requests.
type TemplateValuesGetter interface {
	GetExprTemplateValues() map[string]*schemapb.TemplateValue
}

func GetExprTemplateValues(req any) (map[string]*schemapb.TemplateValue, bool) {
	getter, ok := req.(TemplateValuesGetter)
	if !ok {
		return nil, false
	}
	return getter.GetExprTemplateValues(), true
}

var TraceLogBaseInfoFuncMap = map[string]func(interface{}) (any, bool){
	"collection_name": GetCollectionNameFromRequest,
	"db_name":         GetDbNameFromRequest,
	"partition_name":  GetPartitionNameFromRequest,
	"partition_names": GetPartitionNamesFromRequest,
	"field_name":      GetFieldNameFromRequest,
	"output_fields":   GetOutputFieldsFromRequest,
	"query_params":    GetQueryParamsFromRequest,
	"expr":            GetExprFromRequest,
	"search_params":   GetSearchParamsFromRequest,
	"dsl":             GetDSLFromRequest,
}

var retryableCode typeutil.Set[int32] = typeutil.NewSet(
	merr.Code(merr.ErrServiceRateLimit),
	merr.Code(merr.ErrCollectionSchemaMismatch),
)

// func init() {
// 	retryableCode = typeutil.NewSet(
// 		merr.Code(merr.ErrServiceRateLimit),
// 		merr.Code(merr.ErrCollectionSchemaMismatch),
// 	)
// }

// ParseMetricLabel determines the final Prometheus status label based on the
// response and error. It implements the composite-label scheme: the coarse
// "fail"/"rejected" values are split into fine-grained labels (fail_input /
// fail_system / rejected_system) so monitoring can tell the responsible party
// apart. The split is encoded into the existing status label's value domain
// (additive cardinality) rather than a new dimension label (which would be
// multiplicative). Retryability takes priority over classification.
func ParseMetricLabel(resp any, err error) string {
	// A response carrying a non-OK status means the request was PROCESSED and
	// failed (fail_*), and takes priority over a non-nil err: the REST v2
	// wrappers reconstruct err = merr.Error(status) from that same response,
	// which previously routed every processed REST failure into the rejected_*
	// buckets and left the fail_* series blind to the entire REST surface.
	st, _ := GetStatusFromResponse(resp)
	if st != nil && !merr.Ok(st) {
		// Client cancellation is neither party's failure.
		if st.GetCode() == merr.CanceledCode {
			return metrics.CancelLabel
		}
		// Retryability takes priority over input/system classification.
		if retryableCode.Contain(st.GetCode()) {
			return metrics.RetryLabel
		}

		// Hard failure: classify by responsible party. merr.Status already
		// stamps the InputError flag into ExtraInfo, so read it directly instead
		// of reconstructing the whole milvusError (this is the proxy hot path).
		if st.GetExtraInfo()[merr.InputErrorFlagKey] == "true" {
			return metrics.FailInputLabel
		}
		return metrics.FailSystemLabel
	}

	// No usable response status: err is the interceptor-level outcome (context
	// cancellation, flow control, transport issues, auth/privilege rejection)
	// — the request was rejected around processing. Classify merr first: a
	// merr error has no GRPCStatus(), so status.Code(err) degrades to
	// codes.Unknown and would misbucket user input errors as system
	// rejections. The auth/privilege interceptors deliberately return raw gRPC
	// codes (not merr, to keep SDK retry behavior correct); those are the
	// caller's fault, so bucket them as a user-side rejection. Everything else
	// is a system-side rejection.
	if err != nil {
		// Client cancellation is neither party's failure; don't count it as a
		// system rejection.
		if errors.Is(err, context.Canceled) {
			return metrics.CancelLabel
		}
		if merr.GetErrorType(err) == merr.InputError {
			return metrics.RejectedUserLabel
		}
		switch status.Code(err) {
		case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
			return metrics.RejectedUserLabel
		default:
			return metrics.RejectedSystemLabel
		}
	}
	return metrics.SuccessLabel
}
