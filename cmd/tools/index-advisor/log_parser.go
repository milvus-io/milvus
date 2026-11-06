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
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Reasons a line or record is left out of the analysis. Every dropped input is
// counted under one of these, so the report accounts for the whole log.
const (
	skipOrphanLine      = "orphan_line"      // text before the first record header
	skipUnmatchedFormat = "unmatched_format" // record matches no configured formatter
)

// recordStart matches the "[$time_now] [ACCESS] " prefix every default formatter
// begins with ($time_now is written in the "2006/01/02 15:04:05.000 -07:00"
// layout of internal/proxy/accesslog/info). An expression may contain
// newlines, which Milvus writes unescaped, so any other line continues the
// current record.
var recordStart = regexp.MustCompile(`^\[\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [+-]\d{2}:\d{2}\] \[ACCESS\] `)

// accessRecord is one parsed access-log record.
type accessRecord struct {
	values map[string]string // placeholder -> written value
}

func (r accessRecord) method() string     { return methodPath(r.values[fieldMethodName]) }
func (r accessRecord) database() string   { return r.values[fieldDatabase] }
func (r accessRecord) collection() string { return r.values[fieldCollection] }

// methodPath returns the method a logged $method_name names. Milvus logs a
// RESTful method as the URL path followed by any query string, and picks the
// formatter by the path alone (RestfulInfo.MethodNameForFormatter).
func methodPath(logged string) string {
	path, _, _ := strings.Cut(logged, "?")
	return path
}

// status returns the logged request status and whether the record's formatter
// logs one at all.
func (r accessRecord) status() (string, bool) {
	v, ok := r.values[fieldStatus]
	return v, ok
}

// expr returns the logged filter expression and whether the record's formatter
// logs one at all. An empty expression with ok=true means "no filter".
func (r accessRecord) expr() (string, bool) {
	v, ok := r.values[fieldExpr]
	return v, ok
}

// timeCost returns the request latency, or false when it was not logged or
// Milvus wrote a sentinel because the request never finished.
func (r accessRecord) timeCost() (time.Duration, bool) {
	return parseTimeCost(r.values[fieldTimeCost])
}

// parseTimeCost parses a $time_cost value, which Milvus writes with
// fmt.Sprint(time.Duration) (e.g. "1.2ms", "850µs") or info.Unknown.
func parseTimeCost(v string) (time.Duration, bool) {
	if v == "" || v == info.Unknown || v == info.NotAny {
		return 0, false
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, false
	}
	return d, true
}

// skipCounts counts dropped inputs by reason.
type skipCounts map[string]int

// readRecordsFile parses one access-log file. See readRecords.
func readRecordsFile(path string, formatters *formatterSet, skipped skipCounts, emit func(accessRecord)) ([]string, error) {
	f, err := os.Open(path) // #nosec G304 -- path is a CLI argument
	if err != nil {
		return nil, merr.WrapErrIoFailed(path, err)
	}
	defer f.Close()
	return readRecords(f, path, formatters, skipped, emit)
}

// readRecords splits r into records, parses each with its formatter and
// passes it to emit. Unparseable input is counted in skipped rather than
// returned as an error, so one odd line does not hide the rest of the log. It
// returns warnings when the file holds no record at all, or records that match
// no formatter.
func readRecords(r io.Reader, file string, formatters *formatterSet, skipped skipCounts, emit func(accessRecord)) ([]string, error) {
	var (
		current        strings.Builder
		start          int // header line of current; 0 when no record is open
		headers        int
		orphans        int
		unmatched      int
		firstUnmatched int
	)
	flush := func() {
		if start == 0 {
			return
		}
		if values, ok := formatters.match(current.String()); ok {
			emit(accessRecord{values: values})
		} else {
			skipped[skipUnmatchedFormat]++
			if unmatched == 0 {
				firstUnmatched = start
			}
			unmatched++
		}
		current.Reset()
		start = 0
	}

	// bufio.Reader rather than bufio.Scanner: an IN list can make one line
	// longer than any fixed token limit.
	reader := bufio.NewReader(r)
	for lineNo := 1; ; lineNo++ {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, merr.WrapErrIoFailed(file, err)
		}
		if line == "" && err == io.EOF {
			break
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r") // LF or CRLF line endings

		switch {
		case recordStart.MatchString(line):
			flush()
			start = lineNo
			headers++
			current.WriteString(line)
		case start == 0:
			skipped[skipOrphanLine]++
			orphans++
		default:
			current.WriteByte('\n')
			current.WriteString(line)
		}
		if err == io.EOF {
			break
		}
	}
	flush()

	var warnings []string
	if headers == 0 && orphans > 0 {
		warnings = append(warnings, fmt.Sprintf("access log %s: no line starts a record with \"[$time_now] [ACCESS] \", so its format is not supported", file))
	}
	if unmatched > 0 {
		warnings = append(warnings, fmt.Sprintf("access log %s: no formatter matches %s (first at line %d). Check that the formatters in use, the defaults or those passed with --milvus-config, are the deployment's",
			file, plural(unmatched, "record"), firstUnmatched))
	}
	return warnings, nil
}
