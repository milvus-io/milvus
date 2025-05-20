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

const (
	Unknown            = "Unknown"
	timeFormat         = "2006/01/02 15:04:05.000 -07:00"
	ClientRequestIDKey = "client_request_id"
)

type getMetricFunc func(i AccessInfo) string

// supported metrics
var MetricFuncMap = map[string]getMetricFunc{
	"$method_name":       getMethodName,
	"$method_status":     getMethodStatus,
	"$trace_id":          getTraceID,
	"$user_addr":         getAddr,
	"$user_name":         getUserName,
	"$response_size":     getResponseSize,
	"$error_code":        getErrorCode,
	"$error_msg":         getErrorMsg,
	"$error_type":        getErrorType,
	"$database_name":     getDbName,
	"$collection_name":   getCollectionName,
	"$partition_name":    getPartitionName,
	"$time_cost":         getTimeCost,
	"$time_now":          getTimeNow,
	"$time_start":        getTimeStart,
	"$time_end":          getTimeEnd,
	"$method_expr":       getExpr,
	"$output_fields":     getOutputFields,
	"$sdk_version":       getSdkVersion,
	"$cluster_prefix":    getClusterPrefix,
	"$consistency_level": getConsistencyLevel,
	"$anns_field":        getAnnsField,
	"$nq":                getNq,
	"$search_params":     getSearchParams,
	"$query_params":      getQueryParams,
}

type AccessInfo interface {
	TimeCost() string
	TimeNow() string
	TimeStart() string
	TimeEnd() string
	MethodName() string
	Address() string
	TraceID() string
	MethodStatus() string
	UserName() string
	ResponseSize() string
	ErrorCode() string
	ErrorMsg() string
	ErrorType() string
	DbName() string
	AnnsField() string
	CollectionName() string
	PartitionName() string
	Expression() string
	OutputFields() string
	SdkVersion() string
	ConsistencyLevel() string
	NQ() string
	SearchParams() string
	QueryParams() string
}

func Get(i AccessInfo, keys ...string) []any {
	result := []any{}
	metricMap := map[string]string{}
	for _, key := range keys {
		if value, ok := metricMap[key]; ok {
			result = append(result, value)
		} else if getFunc, ok := MetricFuncMap[key]; ok {
			result = append(result, getFunc(i))
		}
	}
	return result
}

func getMethodName(i AccessInfo) string {
	return i.MethodName()
}

func getMethodStatus(i AccessInfo) string {
	return i.MethodStatus()
}

func getTraceID(i AccessInfo) string {
	return i.TraceID()
}

func getAddr(i AccessInfo) string {
	return i.Address()
}

func getUserName(i AccessInfo) string {
	return i.UserName()
}

func getResponseSize(i AccessInfo) string {
	return i.ResponseSize()
}

func getErrorCode(i AccessInfo) string {
	return i.ErrorCode()
}

func getErrorMsg(i AccessInfo) string {
	return i.ErrorMsg()
}

func getErrorType(i AccessInfo) string {
	return i.ErrorType()
}

func getDbName(i AccessInfo) string {
	return i.DbName()
}

func getCollectionName(i AccessInfo) string {
	return i.CollectionName()
}

func getPartitionName(i AccessInfo) string {
	return i.PartitionName()
}

func getTimeCost(i AccessInfo) string {
	return i.TimeCost()
}

func getTimeNow(i AccessInfo) string {
	return i.TimeNow()
}

func getTimeStart(i AccessInfo) string {
	return i.TimeStart()
}

func getTimeEnd(i AccessInfo) string {
	return i.TimeEnd()
}

func getExpr(i AccessInfo) string {
	return i.Expression()
}

func getSdkVersion(i AccessInfo) string {
	return i.SdkVersion()
}

func getOutputFields(i AccessInfo) string {
	return i.OutputFields()
}

func getConsistencyLevel(i AccessInfo) string {
	return i.ConsistencyLevel()
}

func getAnnsField(i AccessInfo) string {
	return i.AnnsField()
}

func getClusterPrefix(i AccessInfo) string {
	return ClusterPrefix.Load()
}

func getNq(i AccessInfo) string {
	return i.NQ()
}

func getSearchParams(i AccessInfo) string {
	return i.SearchParams()
}

func getQueryParams(i AccessInfo) string {
	return i.QueryParams()
}
