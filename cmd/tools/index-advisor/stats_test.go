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

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadStatsFixture(t *testing.T) {
	m := loadFixtureSnapshot(t)
	s, warnings, err := loadStatsFile("testdata/field_stats.json", m)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	products, _ := m.lookup("default", "products")

	assert.Equal(t, statsProvenance{
		Source:      "test fixture: written by hand for the generated products data",
		CollectedAt: "2026-10-15T00:00:00Z",
		Scope:       "default.products, all 5000 rows",
	}, s.provenance)

	rows, city := s.lookup("default", "products", mustColumn(t, products, "city"))
	require.NotNil(t, rows)
	assert.Equal(t, int64(5000), *rows)
	require.NotNil(t, city.DistinctCount)
	assert.Equal(t, int64(5), *city.DistinctCount)

	// Keys are normalized: the file says "color", the predicate column is $meta["color"].
	_, color := s.lookup("default", "products", mustColumn(t, products, `$meta["color"]`))
	require.NotNil(t, color.DistinctCount)
	assert.Equal(t, int64(2), *color.DistinctCount)

	_, price := s.lookup("default", "products", mustColumn(t, products, "price"))
	assert.Nil(t, price.DistinctCount, "fields absent from the file are unknown")

	rows, _ = s.lookup("default", "idx_probe", mustColumn(t, products, "city"))
	assert.Nil(t, rows, "collections absent from the file are unknown")

	var none *statsSet
	rows, fs := none.lookup("default", "products", mustColumn(t, products, "city"))
	assert.Nil(t, rows)
	assert.Equal(t, fieldStats{}, fs)
}

func TestParseStatsErrors(t *testing.T) {
	m := loadFixtureSnapshot(t)
	const prov = `"format_version": 1, "source": "s", "collected_at": "2026-10-15T00:00:00Z", "scope": "x"`
	cases := map[string]string{
		"not json":       `[`,
		"no version":     `{"source": "s"}`,
		"version":        `{"format_version": 2}`,
		"no source":      `{"format_version": 1, "collected_at": "2026-10-15T00:00:00Z", "scope": "x"}`,
		"blank scope":    `{"format_version": 1, "source": "s", "collected_at": "2026-10-15T00:00:00Z", "scope": " "}`,
		"bad time":       `{"format_version": 1, "source": "s", "collected_at": "yesterday", "scope": "x"}`,
		"unknown coll":   `{` + prov + `, "collections": [{"name": "nope"}]}`,
		"no name":        `{` + prov + `, "collections": [{"row_count": 1}]}`,
		"duplicate coll": `{` + prov + `, "collections": [{"name": "products"}, {"name": "products"}]}`,
		"negative rows":  `{` + prov + `, "collections": [{"name": "products", "row_count": -1}]}`,
		"negative ndv":   `{` + prov + `, "collections": [{"name": "products", "fields": {"city": {"distinct_count": -1}}}]}`,
		"null fraction":  `{` + prov + `, "collections": [{"name": "products", "fields": {"city": {"null_fraction": 1.5}}}]}`,
		"bad field":      `{` + prov + `, "collections": [{"name": "products", "fields": {"price >": {}}}]}`,
		"same column":    `{` + prov + `, "collections": [{"name": "products", "fields": {"color": {}, "$meta[\"color\"]": {}}}]}`,
	}
	want := map[string]string{
		"no version": "format_version is missing", "version": "format_version 2",
		"no source": "must record source", "blank scope": "must record scope", "bad time": "RFC 3339",
		"unknown coll": "not in the snapshot", "no name": "collection 0 has no name", "duplicate coll": "listed twice", "negative rows": "row_count",
		"negative ndv": "distinct_count", "null fraction": "null_fraction", "same column": "more than once",
	}
	for name, data := range cases {
		_, _, err := parseStats([]byte(data), m)
		if msg, ok := want[name]; ok {
			assert.ErrorContains(t, err, msg, name)
		} else {
			assert.Error(t, err, name)
		}
	}
	_, _, err := parseStats([]byte(`{`+prov+`, "collections": []}`), m)
	assert.NoError(t, err)

	_, _, err = loadStatsFile(filepath.Join(t.TempDir(), "missing.json"), m)
	assert.True(t, errors.Is(err, merr.ErrIoFailed))

	bad := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte(cases["negative rows"]), 0o600))
	_, _, err = loadStatsFile(bad, m)
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

// A misnamed statistic is ignored with a warning and reported as unknown.
func TestParseStatsIgnoresUnknownKeys(t *testing.T) {
	m := loadFixtureSnapshot(t)
	products, _ := m.lookup("default", "products")
	s, warnings, err := parseStats([]byte(`{"format_version": 1, "source": "s", "collected_at": "2026-10-15T00:00:00Z", "scope": "x",
		"collections": [{"name": "products", "fields": {"city": {"cardinality": 5}, "price": {"ndv": 9, "distinct_count": 9}}}]}`), m)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ignoring unknown key collections[*].fields[*].cardinality",
		"ignoring unknown key collections[*].fields[*].ndv",
	}, warnings)
	_, city := s.lookup("default", "products", mustColumn(t, products, "city"))
	assert.Nil(t, city.DistinctCount)
	_, price := s.lookup("default", "products", mustColumn(t, products, "price"))
	require.NotNil(t, price.DistinctCount)
	assert.Equal(t, int64(9), *price.DistinctCount)
}

// Statistics written under an alias are filed under its collection.
func TestParseStatsAlias(t *testing.T) {
	snap, _, err := parseSnapshot([]byte(`{"format_version": 1, "collections": [{"name": "t", "aliases": ["t_alias"],
		"fields": [{"name": "id", "data_type": "Int64", "is_primary_key": true}, {"name": "city", "data_type": "VarChar"}]}]}`))
	require.NoError(t, err)
	s, _, err := parseStats([]byte(`{"format_version": 1, "source": "s", "collected_at": "2026-10-15T00:00:00Z", "scope": "x",
		"collections": [{"name": "t_alias", "row_count": 7, "fields": {"city": {"distinct_count": 2}}}]}`), snap)
	require.NoError(t, err)
	info, _ := snap.lookup("default", "t")
	rows, city := s.lookup("default", "t", mustColumn(t, info, "city"))
	require.NotNil(t, rows)
	assert.Equal(t, int64(7), *rows)
	require.NotNil(t, city.DistinctCount)
	assert.Equal(t, int64(2), *city.DistinctCount)
}
