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

// Validation plans: the before/after measurements that decide whether a
// candidate, or an existing index, is worth keeping.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"
)

// validationPlan is a before/after check for a column with no index: measure
// without an index, then with each candidate on its own.
func validationPlan(rec recommendation) []string {
	plan := []string{
		fmt.Sprintf("Before: on a non-production copy of %s.%s with production-like data, replay %s with representative values and record p50/p95 latency without an index on %s (%s).",
			rec.DB, rec.Collection, quoteList(rec.Observed.TopFingerprints), rec.Column, baselineText(rec.Observed.TimeCostMs)),
	}
	for _, c := range rec.Candidates {
		plan = append(plan, fmt.Sprintf("After (%s): on a copy with no other index on %s, create %s; this tool does not create it. Wait until the index is built, replay the same requests, and record p50/p95 latency, build time, memory and disk use, and insert latency.",
			c.Role, rec.Column, describeCandidate(c)))
	}
	return append(plan, "Keep the candidate with the clearest measured improvement over Before; if none improves, keep no index.")
}

// reviewPlan checks an indexed column: whether the existing index helps, and
// whether a candidate does better.
func reviewPlan(rec recommendation) []string {
	if rec.Observed.Predicates == 0 {
		return []string{
			fmt.Sprintf("No analyzed request in these logs filters on %s. These logs may be a sample: access logs that cover the whole workload (every client and period) show whether requests filter on it.", rec.Column),
		}
	}
	plan := []string{
		fmt.Sprintf("On non-production copies of %s.%s, one with the existing index and one without, replay %s with representative values and compare p50/p95 latency (%s); its presence alone does not show it is used.",
			rec.DB, rec.Collection, quoteList(rec.Observed.TopFingerprints), baselineText(rec.Observed.TimeCostMs)),
	}
	for _, c := range rec.Candidates {
		plan = append(plan, fmt.Sprintf("On a copy whose only index on %s is %s, replay the same requests and record latency, build time, memory and disk use, and insert latency.", rec.Column, describeCandidate(c)))
	}
	return append(plan, "Compare these measurements with those of the existing index; this tool does not alter indexes.")
}

func describeCandidate(c candidate) string {
	s := fmt.Sprintf("index_type %s on field %q", c.IndexType, c.FieldName)
	if len(c.Params) > 0 {
		keys := lo.Keys(c.Params)
		sort.Strings(keys)
		s += " with params " + strings.Join(lo.Map(keys, func(k string, _ int) string { return k + "=" + c.Params[k] }), ", ")
	}
	return s
}

func baselineText(l *latency) string {
	if l == nil {
		return "the access log has no latency for these requests"
	}
	return fmt.Sprintf("for reference, the access log shows p50 %.3f ms, p95 %.3f ms over %s", l.P50, l.P95, plural(l.Samples, "request"))
}

// plural renders n with noun, adding "s" unless n is 1.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

func quoteList(items []string) string {
	return strings.Join(lo.Map(items, func(s string, _ int) string { return "`" + s + "`" }), ", ")
}
