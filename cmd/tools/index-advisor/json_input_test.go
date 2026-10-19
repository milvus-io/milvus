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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeInput(t *testing.T) {
	type inner struct {
		A int `json:"a"`
	}
	type embedded struct {
		E string `json:"e"`
	}
	type outer struct {
		embedded
		Plain   string
		Skipped string `json:"-"`
		Ptr     *inner `json:"ptr"`
		List    []inner
		Map     map[string]inner `json:"map"`
		hidden  string
	}
	var v outer
	warnings, err := decodeInput([]byte(`{
		"e": "x", "plain": "p", "Skipped": "s", "hidden": "h",
		"ptr": {"a": 1, "b": 2}, "List": [{"a": 1}, {"c": 3}, null], "map": {"k": {"d": 4}}
	}`), &v)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ignoring unknown key List[*].c",
		"ignoring unknown key Skipped",
		"ignoring unknown key hidden",
		"ignoring unknown key map[*].d",
		"ignoring unknown key ptr.b",
	}, warnings)
	assert.Equal(t, "x", v.E)
	assert.Equal(t, "p", v.Plain, "an untagged field is matched by name, case-insensitively")
	assert.Equal(t, 1, v.Ptr.A)
	assert.Empty(t, v.hidden)

	_, err = decodeInput([]byte(`{"ptr": {"a": "one"}}`), &v)
	assert.Error(t, err, "a value of the wrong type is an error")
	_, err = decodeInput([]byte(`{} trailing`), &v)
	assert.Error(t, err)
}
