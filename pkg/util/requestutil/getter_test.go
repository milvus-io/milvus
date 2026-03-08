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
	"reflect"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
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
