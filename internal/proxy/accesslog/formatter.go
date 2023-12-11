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

package accesslog

import (
	"strings"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

const (
	unknownString = "Unknown"
	fomaterkey    = "format"
	methodKey     = "methods"
)

type getMetricFunc func(i *GrpcAccessInfo) string

// supported metrics
var metricFuncMap = map[string]getMetricFunc{
	"$method_name":     getMethodName,
	"$method_status":   getMethodStatus,
	"$trace_id":        getTraceID,
	"$user_addr":       getAddr,
	"$user_name":       getUserName,
	"$response_size":   getResponseSize,
	"$error_code":      getErrorCode,
	"$error_msg":       getErrorMsg,
	"$database_name":   getDbName,
	"$collection_name": getCollectionName,
	"$partition_name":  getPartitionName,
	"$time_cost":       getTimeCost,
	"$time_now":        getTimeNow,
	"$time_start":      getTimeStart,
	"$time_end":        getTimeEnd,
	"$method_expr":     getExpr,
	"$sdk_version":     getSdkVersion,
}

var BaseFormatterKey = "base"

// Formaater manager not concurrent safe
// make sure init with Add and SetMethod before use Get
type FormatterManger struct {
	formatters map[string]*Formatter
	methodMap  map[string]string
}

func NewFormatterManger() *FormatterManger {
	return &FormatterManger{
		formatters: make(map[string]*Formatter),
		methodMap:  make(map[string]string),
	}
}

func (m *FormatterManger) Add(name, fmt string) {
	m.formatters[name] = NewFormatter(fmt)
}

func (m *FormatterManger) SetMethod(name string, methods ...string) {
	for _, method := range methods {
		m.methodMap[method] = name
	}
}

func (m *FormatterManger) GetByMethod(method string) (*Formatter, bool) {
	formatterName, ok := m.methodMap[method]
	if !ok {
		formatterName = BaseFormatterKey
	}

	formatter, ok := m.formatters[formatterName]
	if !ok {
		return nil, false
	}
	return formatter, true
}

type Formatter struct {
	fmt     string
	fields  []string
	prefixs []string
}

func NewFormatter(base string) *Formatter {
	formatter := &Formatter{
		fmt: base,
	}
	formatter.build()
	return formatter
}

func (f *Formatter) buildMetric(metric string) ([]string, []string) {
	newFields := []string{}
	newPrefixs := []string{}
	for id, prefix := range f.prefixs {
		prefixs := strings.Split(prefix, metric)
		newPrefixs = append(newPrefixs, prefixs...)

		for i := 1; i < len(prefixs); i++ {
			newFields = append(newFields, metric)
		}

		if id < len(f.fields) {
			newFields = append(newFields, f.fields[id])
		}
	}
	return newFields, newPrefixs
}

func (f *Formatter) build() {
	f.prefixs = []string{f.fmt}
	f.fields = []string{}
	for mertric := range metricFuncMap {
		if strings.Contains(f.fmt, mertric) {
			f.fields, f.prefixs = f.buildMetric(mertric)
		}
	}
}

func (f *Formatter) Format(info AccessInfo) string {
	fieldValues := info.Get(f.fields...)

	result := ""
	for id, prefix := range f.prefixs {
		result += prefix
		if id < len(fieldValues) {
			result += fieldValues[id]
		}
	}
	result += "\n"
	return result
}

func parseConfigKey(k string) (string, string, error) {
	fields := strings.Split(k, ".")
	if len(fields) != 2 || (fields[1] != fomaterkey && fields[1] != methodKey) {
		return "", "", merr.WrapErrParameterInvalid("<FormatterName>.(format|methods)", k, "parse accsslog formatter config key failed")
	}
	return fields[0], fields[1], nil
}
