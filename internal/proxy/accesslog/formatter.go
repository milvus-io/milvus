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
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	fomaterkey = "format"
	methodKey  = "methods"
)

var BaseFormatterKey = "base"

// lineBreakEscaper keeps one access log record on one line. Field values reach
// the log as is, and a CR or LF in any of them (a multi-line filter in
// $method_expr, a user name, a collection name, an error message) would split
// the record across lines. That breaks line-based tooling and lets a caller
// append a forged record that is attributed to someone else.
var lineBreakEscaper = strings.NewReplacer("\r", "\\r", "\n", "\\n")

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
	base   string
	fmt    string
	fields []string
}

func NewFormatter(base string) *Formatter {
	formatter := &Formatter{
		base: base,
	}
	formatter.build()
	return formatter
}

func (f *Formatter) buildMetric(metric string, prefixs []string) ([]string, []string) {
	newFields := []string{}
	newPrefixs := []string{}
	for id, prefix := range prefixs {
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
	prefixs := []string{f.base}
	f.fields = []string{}
	for metric := range info.MetricFuncMap {
		if strings.Contains(f.base, metric) {
			f.fields, prefixs = f.buildMetric(metric, prefixs)
		}
	}

	f.fmt = ""
	for id, prefix := range prefixs {
		f.fmt += prefix
		if id < len(f.fields) {
			f.fmt += "%s"
		}
	}
	f.fmt += "\n"
}

func (f *Formatter) Format(i info.AccessInfo) string {
	fieldValues := info.Get(i, f.fields...)
	// Every record is built here, so escaping in one place covers all fields and
	// both the gRPC and RESTful protocols. Values without CR/LF are unchanged.
	for idx, v := range fieldValues {
		if s, ok := v.(string); ok {
			fieldValues[idx] = lineBreakEscaper.Replace(s)
		}
	}
	return fmt.Sprintf(f.fmt, fieldValues...)
}

func parseConfigKey(k string) (string, string, error) {
	fields := strings.Split(k, ".")
	if len(fields) != 2 || (fields[1] != fomaterkey && fields[1] != methodKey) {
		// Dynamic names are request payload. This error is also logged by the
		// refresh callback, so describe the required shape without echoing k.
		return "", "", merr.WrapErrParameterInvalidMsg("access log formatter key must match <FormatterName>.(format|methods)")
	}
	return fields[0], fields[1], nil
}
