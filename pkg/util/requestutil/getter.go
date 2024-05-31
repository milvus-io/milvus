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

import "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

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

func GetStatusFromResponse(resp interface{}) (*commonpb.Status, bool) {
	status, ok := resp.(*commonpb.Status)
	if ok {
		return status, true
	}
	getter, ok := resp.(StatusGetter)
	if !ok {
		return nil, false
	}
	return getter.GetStatus(), true
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
