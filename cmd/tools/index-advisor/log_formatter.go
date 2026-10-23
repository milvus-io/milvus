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

package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/samber/lo"
	"gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	baseFormatterName = "base"

	fieldMethodName = "$method_name"
	fieldStatus     = "$method_status"
	fieldDatabase   = "$database_name"
	fieldCollection = "$collection_name"
	fieldExpr       = "$method_expr"
	fieldTimeCost   = "$time_cost"
)

// formatterConfig is one entry of proxy.accessLog.formatters.
type formatterConfig struct {
	Format  string `yaml:"format"`
	Methods string `yaml:"methods"`
}

// defaultFormatterConfigs mirrors proxy.accessLog.formatters in configs/milvus.yaml.
// Milvus has no code-level defaults for this param group, so the YAML is the only
// source; TestDefaultFormattersMatchMilvusYaml keeps this copy in sync.
var defaultFormatterConfigs = map[string]formatterConfig{
	"base": {
		Format: "[$time_now] [ACCESS] <$user_name: $user_addr> $method_name [status: $method_status] [code: $error_code] [sdk: $sdk_version] [msg: $error_msg] [traceID: $trace_id] [timeCost: $time_cost] [error_type: $error_type]",
	},
	"query": {
		Format:  "[$time_now] [ACCESS] <$user_name: $user_addr> $method_name [status: $method_status] [code: $error_code] [sdk: $sdk_version] [msg: $error_msg] [traceID: $trace_id] [timeCost: $time_cost] [database: $database_name] [collection: $collection_name] [partitions: $partition_name] [expr: $method_expr] [params: $query_params] [error_type: $error_type]",
		Methods: "Query, Delete, /query, /v2/vectordb/entities/query, /v2/vectordb/entities/get, /v2/vectordb/entities/delete",
	},
	"search": {
		Format:  "[$time_now] [ACCESS] <$user_name: $user_addr> $method_name [status: $method_status] [code: $error_code] [sdk: $sdk_version] [msg: $error_msg] [traceID: $trace_id] [timeCost: $time_cost] [database: $database_name] [collection: $collection_name] [partitions: $partition_name] [expr: $method_expr] [nq: $nq] [params: $search_params] [error_type: $error_type]",
		Methods: "HybridSearch, Search, /search, /v2/vectordb/entities/search, /v2/vectordb/entities/advanced_search, /v2/vectordb/entities/hybrid_search",
	},
	"upsert": {
		Format:  "[$time_now] [ACCESS] <$user_name: $user_addr> $method_name [status: $method_status] [code: $error_code] [sdk: $sdk_version] [msg: $error_msg] [traceID: $trace_id] [timeCost: $time_cost] [database: $database_name] [collection: $collection_name] [partitions: $partition_name] [partial_update: $partial_update] [error_type: $error_type]",
		Methods: "Upsert",
	},
}

type milvusConfigFile struct {
	Proxy struct {
		AccessLog struct {
			Formatters map[string]formatterConfig `yaml:"formatters"`
		} `yaml:"accessLog"`
	} `yaml:"proxy"`
}

// loadFormatterConfigs returns the default formatters overlaid, key by key, with
// the formatters in a milvus.yaml/user.yaml file. This matches Milvus config
// layering: a user.yaml that sets only search.format keeps the default
// search.methods. An empty path returns the defaults.
func loadFormatterConfigs(path string) (map[string]formatterConfig, error) {
	cfgs := make(map[string]formatterConfig, len(defaultFormatterConfigs))
	for name, cfg := range defaultFormatterConfigs {
		cfgs[name] = cfg
	}
	if path == "" {
		return cfgs, nil
	}

	data, err := os.ReadFile(path) // #nosec G304 -- path is a CLI argument
	if err != nil {
		return nil, merr.WrapErrIoFailed(path, err)
	}
	var file milvusConfigFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "parse milvus config %s", path)
	}
	for name, override := range file.Proxy.AccessLog.Formatters {
		cfg := cfgs[name]
		if override.Format != "" {
			cfg.Format = override.Format
		}
		if override.Methods != "" {
			cfg.Methods = override.Methods
		}
		cfgs[name] = cfg
	}
	return cfgs, nil
}

// formatter is an access-log format compiled into a regexp that recovers the
// value of every placeholder from a written record.
type formatter struct {
	name   string
	fields []string // placeholder of each capture group, in order, e.g. "$method_expr"
	re     *regexp.Regexp
}

// compileFormatter turns a format such as "[expr: $method_expr] [nq: $nq]" into
// an anchored regexp. Milvus substitutes values with plain fmt.Sprintf and no
// escaping (internal/proxy/accesslog/formatter.go), so a value may contain the
// format's own delimiters. Every group is non-greedy except $method_expr, which
// is greedy: the values written after it (nq, params, error_type) are generated
// by Milvus, while the expression is user text that can contain "] [".
func compileFormatter(name, format string) (*formatter, error) {
	placeholders := make([]string, 0, len(info.MetricFuncMap))
	for key := range info.MetricFuncMap {
		placeholders = append(placeholders, key)
	}
	// Longest first, so a name never matches as a prefix of a longer one.
	sort.Slice(placeholders, func(i, j int) bool {
		if len(placeholders[i]) != len(placeholders[j]) {
			return len(placeholders[i]) > len(placeholders[j])
		}
		return placeholders[i] < placeholders[j]
	})

	f := &formatter{name: name}
	var pattern strings.Builder
	pattern.WriteString(`(?s)^`)
	literalStart := 0
	for i := 0; i < len(format); i++ {
		if format[i] != '$' {
			continue
		}
		for _, p := range placeholders {
			if !strings.HasPrefix(format[i:], p) {
				continue
			}
			pattern.WriteString(regexp.QuoteMeta(format[literalStart:i]))
			if p == fieldExpr {
				pattern.WriteString(`(.*)`)
			} else {
				pattern.WriteString(`(.*?)`)
			}
			f.fields = append(f.fields, p)
			i += len(p) - 1
			literalStart = i + 1
			break
		}
	}
	pattern.WriteString(regexp.QuoteMeta(format[literalStart:]))
	pattern.WriteString(`$`)

	if !lo.Contains(f.fields, fieldMethodName) {
		return nil, merr.WrapErrParameterInvalidMsg("access log formatter %q has no %s, so its records cannot be routed", name, fieldMethodName)
	}
	re, err := regexp.Compile(pattern.String())
	if err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "access log formatter %q", name)
	}
	f.re = re
	return f, nil
}

// formatterSet routes a record to the formatter Milvus used to write it.
type formatterSet struct {
	ordered []*formatter      // named formatters sorted by name, base last
	owner   map[string]string // method name -> formatter name
}

func newFormatterSet(cfgs map[string]formatterConfig) (*formatterSet, error) {
	names := make([]string, 0, len(cfgs))
	for name := range cfgs {
		if name != baseFormatterName {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if _, ok := cfgs[baseFormatterName]; ok {
		names = append(names, baseFormatterName)
	}

	set := &formatterSet{owner: make(map[string]string)}
	for _, name := range names {
		cfg := cfgs[name]
		if cfg.Format == "" {
			continue
		}
		f, err := compileFormatter(name, cfg.Format)
		if err != nil {
			return nil, err
		}
		set.ordered = append(set.ordered, f)
		for _, method := range paramtable.ParseAsStings(cfg.Methods) {
			set.owner[method] = name
		}
	}
	if len(set.ordered) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("no access log formatter configured")
	}
	return set, nil
}

// analysisWarnings names each formatter that writes query or search records
// without $method_expr or $collection_name, since those requests cannot be
// analyzed.
func (s *formatterSet) analysisWarnings() []string {
	methodsBy := map[string][]string{} // formatter name -> analyzed methods it writes
	for method := range analyzedMethods {
		name, ok := s.owner[method]
		if !ok {
			name = baseFormatterName
		}
		methodsBy[name] = append(methodsBy[name], method)
	}
	var warnings []string
	for _, f := range s.ordered {
		methods := methodsBy[f.name]
		sort.Strings(methods)
		for _, field := range []string{fieldExpr, fieldCollection} {
			if len(methods) > 0 && !lo.Contains(f.fields, field) {
				warnings = append(warnings, fmt.Sprintf("access log formatter %q has no %s, so its %s requests are not analyzed", f.name, field, strings.Join(methods, ", ")))
			}
		}
	}
	return warnings
}

// match parses a record with the formatter that owns its method, mirroring
// FormatterManger.GetByMethod: methods not listed under any formatter use base.
// A formatter's regexp may also match records written by another formatter, so
// a match only counts when the captured method routes back to that formatter.
func (s *formatterSet) match(record string) (map[string]string, bool) {
	for _, f := range s.ordered {
		groups := f.re.FindStringSubmatch(record)
		if groups == nil {
			continue
		}
		values := make(map[string]string, len(f.fields))
		for i, field := range f.fields {
			values[field] = groups[i+1]
		}
		owner, ok := s.owner[methodPath(values[fieldMethodName])]
		if !ok {
			owner = baseFormatterName
		}
		if owner == f.name {
			return values, true
		}
	}
	return nil, false
}
