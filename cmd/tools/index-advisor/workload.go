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
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
)

// Reasons a parsed record is not analyzed, in addition to the parse-level
// reasons in log_parser.go.
const (
	skipNoFilterField        = "not_a_filter_request"      // formatter logs no expression (Insert, Connect, ...)
	skipHybridSearch         = "hybrid_search_unsupported" // list-encoded sub-request filters cannot be split reliably
	skipMethodOutOfScope     = "method_out_of_scope"       // logs an expression but is not a query or search (Delete, ...)
	skipRequestFailed        = "request_failed"            // a logged status other than Successful
	skipNoFilter             = "no_filter"                 // a query or search without a filter
	skipUnknownCollection    = "unknown_collection"
	skipExpressionParseError = "expression_parse_error"
)

// analyzedMethods are the query and search methods whose filter is analyzed,
// named as the access log writes $method_name: the gRPC method (Query) or the
// RESTful URL path (/v2/vectordb/entities/query). The log records one filter
// expression for each of these requests.
var analyzedMethods = map[string]bool{
	"Query": true, "Search": true,
	"/query": true, "/search": true,
	"/v2/vectordb/entities/query": true, "/v2/vectordb/entities/search": true,
}

// hybridMethods log $method_expr as a list of their sub-requests' filters
// (GrpcAccessInfo.Expression in internal/proxy/accesslog/info), without
// escaping the quotes inside them, so the list cannot be split reliably.
var hybridMethods = map[string]bool{
	"HybridSearch": true, "/v2/vectordb/entities/advanced_search": true, "/v2/vectordb/entities/hybrid_search": true,
}

// workload is the analyzed part of the access logs: every record is either
// analyzed or counted under one skip reason. Records are added as they are
// read and not kept. What remains is one aggregate per fingerprint and per
// column, each with the logged latency of its requests.
type workload struct {
	snap         *snapshotSet
	records      int // records parsed from the logs
	analyzed     int
	skipped      skipCounts
	fingerprints map[string]*fingerprintAgg // collection|fingerprint -> requests
	fields       map[string]*filteredField  // collection|column -> requests
}

type fingerprintAgg struct {
	db, collection, fingerprint string
	count                       int
	methods                     map[string]int
	costs                       []time.Duration
}

// filteredField is a column that at least one analyzed request filters on.
type filteredField struct {
	info     *collectionInfo
	col      column
	workload *fieldWorkload
}

// fieldWorkload accumulates the analyzed requests that filter on one column.
// Each request counts once, however many predicates it has on the column.
type fieldWorkload struct {
	requests       int
	opRequests     map[opClass]int // requests per op class
	opSets         map[string]int  // sorted op classes of one request -> requests
	valueKinds     map[string]bool
	minLiteral     int  // shortest known fixed part of a LIKE filter; -1 when none seen
	unknownLiteral bool // a LIKE operand was a template variable, or a regex was used
	fingerprints   map[string]int
	costs          []time.Duration
}

func newFieldWorkload() *fieldWorkload {
	return &fieldWorkload{
		opRequests: map[opClass]int{}, opSets: map[string]int{}, valueKinds: map[string]bool{},
		minLiteral: -1, fingerprints: map[string]int{},
	}
}

func newWorkload(snap *snapshotSet) *workload {
	return &workload{snap: snap, skipped: skipCounts{}, fingerprints: map[string]*fingerprintAgg{}, fields: map[string]*filteredField{}}
}

// add classifies r and aggregates it, when analyzed, by fingerprint and by
// column.
func (w *workload) add(r accessRecord) {
	w.records++
	info, parsed, reason := classify(r, w.snap)
	if reason != "" {
		w.skipped[reason]++
		return
	}
	w.analyzed++
	cost, hasCost := r.timeCost()
	collKey := collectionKey(info.db, info.name)

	fp := w.fingerprints[collKey+"|"+parsed.fingerprint]
	if fp == nil {
		fp = &fingerprintAgg{db: info.db, collection: info.name, fingerprint: parsed.fingerprint, methods: map[string]int{}}
		w.fingerprints[collKey+"|"+parsed.fingerprint] = fp
	}
	fp.count++
	fp.methods[r.method()]++
	if hasCost {
		fp.costs = append(fp.costs, cost)
	}

	// Group this request's predicates by column, so each column counts the
	// request once however many predicates it has on that column.
	byColumn := lo.GroupBy(parsed.predicates, func(p predicate) string { return p.column.String() })
	for colKey, preds := range byColumn {
		f := w.fields[collKey+"|"+colKey]
		if f == nil {
			f = &filteredField{info: info, col: preds[0].column, workload: newFieldWorkload()}
			w.fields[collKey+"|"+colKey] = f
		}
		f.workload.add(preds, parsed.fingerprint, cost, hasCost)
	}
}

// classify decides whether a record is analyzed. It returns the record's
// collection and parsed filter, or the reason it is skipped. A record whose
// formatter logs no status has an unknown status and is analyzed.
func classify(r accessRecord, snap *snapshotSet) (*collectionInfo, parsedExpr, string) {
	expr, hasExpr := r.expr()
	status, hasStatus := r.status()
	switch {
	case !hasExpr:
		return nil, parsedExpr{}, skipNoFilterField
	case hybridMethods[r.method()]:
		return nil, parsedExpr{}, skipHybridSearch
	case !analyzedMethods[r.method()]:
		return nil, parsedExpr{}, skipMethodOutOfScope
	case hasStatus && status != "Successful":
		return nil, parsedExpr{}, skipRequestFailed
	case strings.TrimSpace(expr) == "":
		return nil, parsedExpr{}, skipNoFilter
	}
	info, ok := snap.lookup(r.database(), r.collection())
	if !ok {
		return nil, parsedExpr{}, skipUnknownCollection
	}
	parsed, err := analyzeExpr(info.helper, expr)
	if err != nil {
		return nil, parsedExpr{}, skipExpressionParseError
	}
	return info, parsed, ""
}

// add records one request's predicates on the column.
func (w *fieldWorkload) add(preds []predicate, fingerprint string, cost time.Duration, hasCost bool) {
	w.requests++
	w.fingerprints[fingerprint]++
	if hasCost {
		w.costs = append(w.costs, cost)
	}
	ops := map[opClass]bool{}
	for _, p := range preds {
		ops[p.op] = true
		if p.valueKind != "" {
			w.valueKinds[p.valueKind] = true
		}
		switch p.op {
		case opLikePrefix, opLikeSuffix, opLikeInfix, opLikePattern:
			if p.literalLen == literalLenUnknown {
				w.unknownLiteral = true
			} else if w.minLiteral < 0 || p.literalLen < w.minLiteral {
				w.minLiteral = p.literalLen
			}
		case opRegex:
			w.unknownLiteral = true // literal substrings of a regex are not extracted
		}
	}
	names := make([]string, 0, len(ops))
	for op := range ops {
		w.opRequests[op]++
		names = append(names, string(op))
	}
	sort.Strings(names)
	w.opSets[strings.Join(names, ",")]++
}

// requestsServing counts requests with at least one predicate in ops.
func (w *fieldWorkload) requestsServing(ops []opClass) int {
	n := 0
	for set, count := range w.opSets {
		if lo.ContainsBy(strings.Split(set, ","), func(op string) bool { return lo.Contains(ops, opClass(op)) }) {
			n += count
		}
	}
	return n
}

// servedOps returns the observed op classes a scalar index can serve, sorted.
func (w *fieldWorkload) servedOps() []opClass {
	ops := lo.Filter(lo.Keys(w.opRequests), func(op opClass, _ int) bool {
		_, unserved := unservedOps[op]
		return !unserved
	})
	sort.Slice(ops, func(i, j int) bool { return ops[i] < ops[j] })
	return ops
}
