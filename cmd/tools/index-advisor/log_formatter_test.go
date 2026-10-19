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
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDefaultFormattersMatchMilvusYaml(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "configs", "milvus.yaml"))
	require.NoError(t, err)
	var file milvusConfigFile
	require.NoError(t, yaml.Unmarshal(data, &file))
	assert.Equal(t, defaultFormatterConfigs, file.Proxy.AccessLog.Formatters,
		"defaultFormatterConfigs drifted from proxy.accessLog.formatters in configs/milvus.yaml")
}

func TestCompileFormatter(t *testing.T) {
	f, err := compileFormatter("custom", "[$time_now] $method_name ($unknown) [expr: $method_expr] [nq: $nq]")
	require.NoError(t, err)
	assert.Equal(t, []string{"$time_now", "$method_name", "$method_expr", "$nq"}, f.fields)

	// Unknown placeholders stay literal, as in Milvus; the greedy expr group
	// keeps a delimiter that appears inside the expression.
	groups := f.re.FindStringSubmatch("[t] Search ($unknown) [expr: a] [nq: 1] [nq: 2]")
	require.NotNil(t, groups)
	assert.Equal(t, []string{"t", "Search", "a] [nq: 1", "2"}, groups[1:])
	assert.Nil(t, f.re.FindStringSubmatch("[t] Search (x) [expr: a] [nq: 2]"))

	_, err = compileFormatter("nomethod", "[$time_now] [expr: $method_expr]")
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

func TestLoadFormatterConfigs(t *testing.T) {
	cfgs, err := loadFormatterConfigs("")
	require.NoError(t, err)
	assert.Equal(t, defaultFormatterConfigs, cfgs)

	dir := t.TempDir()
	path := filepath.Join(dir, "user.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
proxy:
  accessLog:
    formatters:
      search:
        format: "$method_name [expr: $method_expr]"
      custom:
        format: "$method_name"
        methods: "Flush, Compact"
`), 0o600))
	cfgs, err = loadFormatterConfigs(path)
	require.NoError(t, err)
	assert.Equal(t, "$method_name [expr: $method_expr]", cfgs["search"].Format)
	assert.Equal(t, defaultFormatterConfigs["search"].Methods, cfgs["search"].Methods, "an unset key keeps its default")
	assert.Equal(t, formatterConfig{Format: "$method_name", Methods: "Flush, Compact"}, cfgs["custom"])
	assert.Equal(t, defaultFormatterConfigs["query"], cfgs["query"])

	_, err = loadFormatterConfigs(filepath.Join(dir, "missing.yaml"))
	assert.True(t, errors.Is(err, merr.ErrIoFailed))

	bad := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(bad, []byte("proxy: [unclosed"), 0o600))
	_, err = loadFormatterConfigs(bad)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

func TestNewFormatterSet(t *testing.T) {
	set, err := newFormatterSet(defaultFormatterConfigs)
	require.NoError(t, err)
	names := make([]string, 0, len(set.ordered))
	for _, f := range set.ordered {
		names = append(names, f.name)
	}
	assert.Equal(t, []string{"query", "search", "upsert", "base"}, names)
	assert.Equal(t, "query", set.owner["Delete"])
	assert.Equal(t, "search", set.owner["/v2/vectordb/entities/hybrid_search"])

	_, err = newFormatterSet(map[string]formatterConfig{"base": {}})
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid), "a set with no formats is rejected")

	_, err = newFormatterSet(map[string]formatterConfig{"base": {Format: "[$time_now]"}})
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

func TestFormatterSetMatch(t *testing.T) {
	set, err := newFormatterSet(defaultFormatterConfigs)
	require.NoError(t, err)

	const prefix = "[2026/10/15 02:41:30.674 +00:00] [ACCESS] <Unknown: tcp-127.0.0.1:50000> "
	const tail = " [status: Successful] [code: 0] [sdk: Python-3.0.2] [msg: ] [traceID: 1] [timeCost: 1ms]"

	values, ok := set.match(prefix + "Query" + tail + " [database: default] [collection: c] [partitions: []] [expr: a > 1] [params: {}] [error_type: ]")
	require.True(t, ok)
	assert.Equal(t, "a > 1", values[fieldExpr])

	_, ok = set.match(prefix + "Flush" + tail + " [error_type: ]")
	assert.True(t, ok, "methods with no formatter use base")

	// Query is owned by the query formatter, so a base-shaped Query record is not
	// something Milvus could have written under this config.
	_, ok = set.match(prefix + "Query" + tail + " [error_type: ]")
	assert.False(t, ok)
}

// A formatter that writes query or search records without $method_expr or
// $collection_name is named, with the methods it writes.
func TestFormatterSetAnalysisWarnings(t *testing.T) {
	assert.Empty(t, defaultFormatterSet(t).analysisWarnings())

	cfgs, err := loadFormatterConfigs("")
	require.NoError(t, err)
	cfgs["search"] = formatterConfig{Format: "[$time_now] [ACCESS] $method_name [collection: $collection_name]", Methods: "Search"}
	cfgs["base"] = formatterConfig{Format: "[$time_now] [ACCESS] $method_name"}
	set, err := newFormatterSet(cfgs)
	require.NoError(t, err)
	assert.Equal(t, []string{
		`access log formatter "search" has no $method_expr, so its Search requests are not analyzed`,
		`access log formatter "base" has no $method_expr, so its /search, /v2/vectordb/entities/search requests are not analyzed`,
		`access log formatter "base" has no $collection_name, so its /search, /v2/vectordb/entities/search requests are not analyzed`,
	}, set.analysisWarnings(), "search methods the config leaves out fall back to base")
}
