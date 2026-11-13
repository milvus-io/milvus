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
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func defaultFormatterSet(t *testing.T) *formatterSet {
	set, err := newFormatterSet(defaultFormatterConfigs)
	require.NoError(t, err)
	return set
}

// appendTo returns an emit function that appends each record to records.
func appendTo(records *[]accessRecord) func(accessRecord) {
	return func(r accessRecord) { *records = append(*records, r) }
}

// findRecord returns the first record with the given method and expression.
func findRecord(t *testing.T, records []accessRecord, method, expr string) accessRecord {
	for _, r := range records {
		if e, _ := r.expr(); r.method() == method && e == expr {
			return r
		}
	}
	require.Failf(t, "record not found", "%s %q", method, expr)
	return accessRecord{}
}

func TestReadRecordsFixture(t *testing.T) {
	skipped := skipCounts{}
	var records []accessRecord
	warnings, err := readRecordsFile("testdata/access_log.txt", defaultFormatterSet(t), skipped, appendTo(&records))
	assert.Empty(t, warnings)
	require.NoError(t, err)
	assert.Len(t, records, 73)
	assert.Empty(t, skipped)

	connect := records[0]
	assert.Equal(t, "Connect", connect.method())
	_, hasExpr := connect.expr()
	assert.False(t, hasExpr, "the base formatter logs no expression")

	multiline := findRecord(t, records, "Query", "price > 1\nand price < 10")
	assert.Equal(t, "default", multiline.database())
	assert.Equal(t, "products", multiline.collection())
	cost, ok := multiline.timeCost()
	assert.True(t, ok)
	assert.Equal(t, 285333*time.Nanosecond, cost)

	// Values Milvus writes without escaping: a delimiter inside a string literal,
	// template placeholders, an empty filter and a list-encoded hybrid search.
	findRecord(t, records, "Query", `city == "a]b [params: x"`)
	findRecord(t, records, "Search", "price > {p}")
	findRecord(t, records, "Query", "city in {cs}")
	findRecord(t, records, "Search", "")
	findRecord(t, records, "HybridSearch", `["city == "SF"", "price > 500"]`)
	findRecord(t, records, "Delete", "price < 0")
	status, ok := findRecord(t, records, "Query", "price >").status()
	assert.True(t, ok)
	assert.Equal(t, "Failed", status)

	// RESTful records log the URL path as the method.
	findRecord(t, records, "/v2/vectordb/entities/query", "price > 900")
	findRecord(t, records, "/v2/vectordb/entities/search", `city == "LA"`)
}

func TestReadRecordsSkips(t *testing.T) {
	const base = "[2026/10/15 02:41:14.896 +00:00] [ACCESS] <Unknown: tcp-127.0.0.1:50000> Flush [status: Successful] [code: 0] [sdk: Python-3.0.2] [msg: ] [traceID: 1] [timeCost: 2ms] [error_type: ]"
	input := "leftover from a rotated file\n" +
		"[2026/10/15 02:41:14.896 +00:00] [ACCESS] not a known format\n" +
		base // last record without a trailing newline

	skipped := skipCounts{}
	var records []accessRecord
	warnings, err := readRecords(strings.NewReader(input), "in.log", defaultFormatterSet(t), skipped, appendTo(&records))
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "Flush", records[0].method())
	assert.Equal(t, skipCounts{skipOrphanLine: 1, skipUnmatchedFormat: 1}, skipped)
	assert.Equal(t, []string{
		"access log in.log: no formatter matches 1 record (first at line 2). Check that the formatters in use, the defaults or those passed with --milvus-config, are the deployment's",
	}, warnings, "orphan lines before the first record are not a format problem")
}

// A file in which no line starts a record is reported as an unsupported format.
func TestReadRecordsUnsupportedFormat(t *testing.T) {
	skipped := skipCounts{}
	var records []accessRecord
	warnings, err := readRecords(strings.NewReader("2026-10-15 Query price > 1\n2026-10-15 Search city == 1\n"), "in.log", defaultFormatterSet(t), skipped, appendTo(&records))
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Equal(t, skipCounts{skipOrphanLine: 2}, skipped)
	assert.Equal(t, []string{
		`access log in.log: no line starts a record with "[$time_now] [ACCESS] ", so its format is not supported`,
	}, warnings)

	warnings, err = readRecords(strings.NewReader(""), "empty.log", defaultFormatterSet(t), skipCounts{}, appendTo(&records))
	require.NoError(t, err)
	assert.Empty(t, warnings, "an empty file has no format to report")
}

func TestReadRecordsLongExpr(t *testing.T) {
	values := make([]string, 200000)
	for i := range values {
		values[i] = "1234567"
	}
	expr := "id in [" + strings.Join(values, ", ") + "]"
	line := "[2026/10/15 02:41:30.674 +00:00] [ACCESS] <Unknown: tcp-127.0.0.1:50000> Query [status: Successful] [code: 0] [sdk: Python-3.0.2] [msg: ] [traceID: 1] [timeCost: 1ms] [database: default] [collection: c] [partitions: []] [expr: " + expr + "] [params: {}] [error_type: ]\n"

	var records []accessRecord
	_, err := readRecords(strings.NewReader(line), "in.log", defaultFormatterSet(t), skipCounts{}, appendTo(&records))
	require.NoError(t, err)
	require.Len(t, records, 1)
	got, _ := records[0].expr()
	assert.Equal(t, expr, got)
}

func TestReadRecordsErrors(t *testing.T) {
	_, err := readRecordsFile("testdata/does-not-exist.txt", defaultFormatterSet(t), skipCounts{}, func(accessRecord) {})
	assert.True(t, errors.Is(err, merr.ErrIoFailed))

	_, err = readRecords(iotest.ErrReader(errors.New("disk gone")), "in.log", defaultFormatterSet(t), skipCounts{}, func(accessRecord) {})
	assert.True(t, errors.Is(err, merr.ErrIoFailed))
}

func TestParseTimeCost(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"1.5ms", 1500 * time.Microsecond, true},
		{"850µs", 850 * time.Microsecond, true}, // U+00B5, what time.Duration.String writes
		{"850μs", 850 * time.Microsecond, true}, // U+03BC
		{"1m2.5s", 62500 * time.Millisecond, true},
		{"Unknown", 0, false},
		{"n/a", 0, false},
		{"", 0, false},
		{"fast", 0, false},
	}
	for _, c := range cases {
		got, ok := parseTimeCost(c.in)
		assert.Equal(t, c.ok, ok, c.in)
		assert.Equal(t, c.want, got, c.in)
	}
}

// Milvus logs a RESTful method with its query string and picks the formatter
// by the path.
func TestReadRecordsRESTQueryString(t *testing.T) {
	line := "[2026/10/15 02:41:30.674 +00:00] [ACCESS] <root: 127.0.0.1> /v2/vectordb/entities/query?timeout=5 [status: Successful] [code: 0] [sdk: Restful] [msg: ] [traceID: 1] [timeCost: 1ms] [database: default] [collection: c] [partitions: []] [expr: a > 1] [params: {}] [error_type: ]\n"
	var records []accessRecord
	_, err := readRecords(strings.NewReader(line), "in.log", defaultFormatterSet(t), skipCounts{}, appendTo(&records))
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "/v2/vectordb/entities/query", records[0].method())
	expr, ok := records[0].expr()
	assert.True(t, ok, "routed to the query formatter")
	assert.Equal(t, "a > 1", expr)
}

// Logs with CRLF line endings parse like LF ones.
func TestReadRecordsCRLF(t *testing.T) {
	data, err := os.ReadFile("testdata/access_log.txt")
	require.NoError(t, err)
	var lf, crlf []accessRecord
	_, err = readRecords(strings.NewReader(string(data)), "lf.log", defaultFormatterSet(t), skipCounts{}, appendTo(&lf))
	require.NoError(t, err)
	skipped := skipCounts{}
	_, err = readRecords(strings.NewReader(strings.ReplaceAll(string(data), "\n", "\r\n")), "crlf.log", defaultFormatterSet(t), skipped, appendTo(&crlf))
	require.NoError(t, err)
	assert.Empty(t, skipped)
	assert.Equal(t, lf, crlf)
}
