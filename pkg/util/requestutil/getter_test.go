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
	"reflect"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestGetCollectionNameFromRequest(t *testing.T) {
	type args struct {
		req any
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "true",
			args: args{
				req: &milvuspb.CreateCollectionRequest{
					CollectionName: "foo",
				},
			},
			want:  "foo",
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetCollectionNameFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCollectionNameFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetCollectionNameFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetDbNameFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "true",
			args: args{
				req: &milvuspb.CreateDatabaseRequest{
					DbName: "foo",
				},
			},
			want:  "foo",
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetDbNameFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDbNameFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetDbNameFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetPartitionNameFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "true",
			args: args{
				req: &milvuspb.CreatePartitionRequest{
					PartitionName: "baz",
				},
			},
			want:  "baz",
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetPartitionNameFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPartitionNameFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetPartitionNameFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetPartitionNamesFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "true",
			args: args{
				req: &milvuspb.SearchRequest{
					PartitionNames: []string{"baz", "faz"},
				},
			},
			want:  []string{"baz", "faz"},
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetPartitionNamesFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetPartitionNamesFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetPartitionNamesFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetFieldNameFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "ok",
			args: args{
				req: &milvuspb.CreateIndexRequest{
					FieldName: "foo",
				},
			},
			want:  "foo",
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetFieldNameFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFieldNameFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetFieldNameFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetOutputFieldsFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "ok",
			args: args{
				req: &milvuspb.SearchRequest{
					OutputFields: []string{"foo", "bar"},
				},
			},
			want:  []string{"foo", "bar"},
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetOutputFieldsFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetOutputFieldsFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetOutputFieldsFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetQueryParamsFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "ok",
			args: args{
				req: &milvuspb.QueryRequest{
					QueryParams: []*commonpb.KeyValuePair{
						{
							Key:   "foo",
							Value: "bar",
						},
					},
				},
			},
			want: []*commonpb.KeyValuePair{
				{
					Key:   "foo",
					Value: "bar",
				},
			},
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetQueryParamsFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetQueryParamsFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetQueryParamsFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetExprFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "ok",
			args: args{
				req: &milvuspb.QueryRequest{
					Expr: "foo",
				},
			},
			want:  "foo",
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetExprFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetExprFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetExprFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetSearchParamsFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "ok",
			args: args{
				req: &milvuspb.SearchRequest{
					SearchParams: []*commonpb.KeyValuePair{
						{
							Key:   "foo",
							Value: "bar",
						},
					},
				},
			},
			want: []*commonpb.KeyValuePair{
				{
					Key:   "foo",
					Value: "bar",
				},
			},
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetSearchParamsFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSearchParamsFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetSearchParamsFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetDSLFromRequest(t *testing.T) {
	type args struct {
		req interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want1 bool
	}{
		{
			name: "ok",
			args: args{
				req: &milvuspb.SearchRequest{
					Dsl: "foo",
				},
			},
			want:  "foo",
			want1: true,
		},
		{
			name: "fail",
			args: args{
				req: &commonpb.Status{},
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetDSLFromRequest(tt.args.req)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDSLFromRequest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetDSLFromRequest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestGetStatusFromResponse(t *testing.T) {
	type args struct {
		resp interface{}
	}
	tests := []struct {
		name  string
		args  args
		want  *commonpb.Status
		want1 bool
	}{
		{
			name: "describe collection response",
			args: args{
				resp: &milvuspb.DescribeCollectionResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
					},
				},
			},
			want: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			want1: true,
		},
		{
			name: "common status",
			args: args{
				resp: &commonpb.Status{},
			},
			want:  &commonpb.Status{},
			want1: true,
		},
		{
			name: "invalid response",
			args: args{
				resp: "foo",
			},
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := GetStatusFromResponse(tt.args.resp)
			if got1 && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetStatusFromResponse() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetStatusFromResponse() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseMetricLabel(t *testing.T) {
	assertLabels := func(wantStatus, wantCause string, resp any, err error) {
		t.Helper()
		status, cause := ParseMetricLabel(resp, err)
		assert.Equal(t, wantStatus, status)
		assert.Equal(t, wantCause, cause)
	}

	// transport / interceptor error -> rejected by the system
	assertLabels(metrics.RejectedLabel, metrics.CauseSystem,
		&commonpb.Status{}, errors.New("transport failed"))

	// success
	assertLabels(metrics.SuccessLabel, metrics.CauseNA, &commonpb.Status{}, nil)

	// retryable hard failure -> retry (retryability beats classification)
	assertLabels(metrics.RetryLabel, metrics.CauseNA,
		merr.Status(merr.ErrServiceRateLimit), nil)

	// hard failure caused by Milvus itself
	assertLabels(metrics.FailLabel, metrics.CauseSystem,
		merr.Status(merr.ErrSegcore), nil)

	// hard failure caused by the request
	inputErr := merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("bad param"))
	assertLabels(metrics.FailLabel, metrics.CauseUser, merr.Status(inputErr), nil)

	// response that itself implements GetStatus
	assertLabels(metrics.FailLabel, metrics.CauseUser,
		&milvuspb.BoolResponse{Status: merr.Status(inputErr)}, nil)

	// merr input error through the err path (REST v2 handlers abort with merr
	// directly; no GRPCStatus, so the gRPC switch alone would misbucket it as a
	// system rejection)
	assertLabels(metrics.RejectedLabel, metrics.CauseUser, &commonpb.Status{},
		merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("bad param")))

	// merr system error through the err path
	assertLabels(metrics.RejectedLabel, metrics.CauseSystem,
		&commonpb.Status{}, merr.WrapErrServiceInternalMsg("boom"))

	// client cancellation is neither party's fault, but it stays a rejection so
	// that a pre-existing status="rejected" query keeps counting it
	assertLabels(metrics.RejectedLabel, metrics.CauseCancel, &commonpb.Status{}, context.Canceled)
	assertLabels(metrics.RejectedLabel, metrics.CauseCancel,
		&commonpb.Status{}, errors.Wrap(context.Canceled, "rpc aborted"))

	// REST v2 wrapper shape: the response carries the failed status AND the
	// wrapper reconstructs err from it. Processed failures must classify by
	// the status ("fail"), not be misrouted into the "rejected" bucket.
	restSys := merr.Status(merr.ErrSegcore)
	assertLabels(metrics.FailLabel, metrics.CauseSystem, restSys, merr.Error(restSys))
	restInput := merr.Status(merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("bad param")))
	assertLabels(metrics.FailLabel, metrics.CauseUser, restInput, merr.Error(restInput))
	// Cancellation surfaced through the response status is still a "fail", as it
	// was before the cause dimension existed; cause is what excludes it.
	restCancel := merr.Status(context.Canceled)
	assertLabels(metrics.FailLabel, metrics.CauseCancel, restCancel, merr.Error(restCancel))
}

// The status label is a public monitoring contract: dashboards and alert rules
// written against status="fail" / status="rejected" predate the cause dimension
// and must keep working. Splitting the responsible party into new status values
// (fail_input, rejected_system, ...) silently zeroes those queries, so pin the
// domain here: anything finer belongs on the orthogonal cause label.
func TestParseMetricLabelStatusDomainIsStable(t *testing.T) {
	legacyStatus := typeutil.NewSet(
		metrics.SuccessLabel,
		metrics.FailLabel,
		metrics.RejectedLabel,
		metrics.RetryLabel,
	)
	knownCause := typeutil.NewSet(
		metrics.CauseUser,
		metrics.CauseSystem,
		metrics.CauseCancel,
		metrics.CauseNA,
	)

	inputErr := merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("bad param"))
	cases := []struct {
		name string
		resp any
		err  error
	}{
		{"success", &commonpb.Status{}, nil},
		{"transport error", &commonpb.Status{}, errors.New("transport failed")},
		{"rate limited", merr.Status(merr.ErrServiceRateLimit), nil},
		{"system failure", merr.Status(merr.ErrSegcore), nil},
		{"input failure", merr.Status(inputErr), nil},
		{"input rejection", &commonpb.Status{}, inputErr},
		{"canceled at interceptor", &commonpb.Status{}, context.Canceled},
		{"canceled in response", merr.Status(context.Canceled), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, cause := ParseMetricLabel(tc.resp, tc.err)
			assert.Truef(t, legacyStatus.Contain(status),
				"status %q is outside the pre-2.6.19 domain %v; put the distinction on the cause label instead",
				status, legacyStatus.Collect())
			assert.Truef(t, knownCause.Contain(cause), "unknown cause %q", cause)
		})
	}
}
