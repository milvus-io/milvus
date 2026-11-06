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
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/samber/lo"
)

const reportFormatVersion = 1

var fixedNotes = []string{
	"Advisory only: this tool never creates, alters or drops an index.",
	"Candidates are for validation, not changes to apply: no speedup is promised. Keep an index only if its validation plan shows a measured improvement.",
	"AUTOINDEX is the baseline candidate wherever Milvus accepts it; a specific index type is listed only with evidence from the access log or --field-stats.",
	"Existing indexes are listed from the snapshot; this report does not show whether any query used them.",
	`Statistics shown as "unknown" were not provided with --field-stats; alternatives that need them are not evaluated.`,
	"Fingerprints replace every literal with ?; raw expressions, user names, client addresses and trace IDs are not reported.",
}

// report is the JSON output.
type report struct {
	FormatVersion   int                  `json:"format_version"`
	Inputs          inputsSummary        `json:"inputs"`
	Fingerprints    []fingerprintSummary `json:"fingerprints"`
	Recommendations []recommendation     `json:"recommendations"`
	Notes           []string             `json:"notes"`
}

type inputsSummary struct {
	LogFiles        []string    `json:"log_files"` // base names only
	RecordsTotal    int         `json:"records_total"`
	RecordsAnalyzed int         `json:"records_analyzed"`
	Skipped         []skipEntry `json:"skipped"`
	// FieldStats is where the --field-stats numbers came from; nil without it.
	FieldStats *statsProvenance `json:"field_stats"`
}

type skipEntry struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

type fingerprintSummary struct {
	DB          string         `json:"db"`
	Collection  string         `json:"collection"`
	Fingerprint string         `json:"fingerprint"`
	Count       int            `json:"count"`
	Methods     map[string]int `json:"methods"`
	TimeCostMs  *latency       `json:"time_cost_ms"`
}

// latency summarizes logged $time_cost values; nil when none was logged.
type latency struct {
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	Max     float64 `json:"max"`
	Samples int     `json:"samples"`
}

type recommendation struct {
	Rank            int             `json:"rank"`
	DB              string          `json:"db"`
	Collection      string          `json:"collection"`
	Column          string          `json:"column"`
	DataType        string          `json:"data_type"`
	Action          string          `json:"action"`
	Observed        observed        `json:"observed"`
	ExistingIndexes []existingIndex `json:"existing_indexes"`
	Candidates      []candidate     `json:"candidates"`
	Statistics      statistics      `json:"statistics"`
	Notes           []string        `json:"notes,omitempty"`
	ValidationPlan  []string        `json:"validation_plan,omitempty"`
}

type observed struct {
	// Predicates is the number of analyzed requests that filter on the column.
	Predicates      int            `json:"predicates"`
	OpClasses       map[string]int `json:"op_classes"` // requests per op class
	TopFingerprints []string       `json:"top_fingerprints"`
	TimeCostMs      *latency       `json:"time_cost_ms"` // of those requests: the baseline to validate against
}

type existingIndex struct {
	IndexName string `json:"index_name"`
	IndexType string `json:"index_type"`
}

// statistics values are numbers, or "unknown" when not provided.
type statistics struct {
	RowCount      statValue `json:"row_count"`
	DistinctCount statValue `json:"distinct_count"`
	NullFraction  statValue `json:"null_fraction"`
}

type statValue struct{ v *float64 }

func (s statValue) MarshalJSON() ([]byte, error) {
	if s.v == nil {
		return []byte(`"unknown"`), nil
	}
	return json.Marshal(*s.v)
}

func intStat(v *int64) statValue {
	if v == nil {
		return statValue{}
	}
	f := float64(*v)
	return statValue{&f}
}

func fingerprintSummaries(w *workload) []fingerprintSummary {
	out := []fingerprintSummary{}
	for _, fp := range w.fingerprints {
		out = append(out, fingerprintSummary{
			DB: fp.db, Collection: fp.collection, Fingerprint: fp.fingerprint,
			Count: fp.count, Methods: fp.methods, TimeCostMs: summarize(fp.costs),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}
		return a.DB+"."+a.Collection+"|"+a.Fingerprint < b.DB+"."+b.Collection+"|"+b.Fingerprint
	})
	return out
}

// newRecommendation reports fc and the advice the engine gave for it, with the
// validation plan for its action.
func newRecommendation(fc *fieldContext, adv advice) recommendation {
	w := fc.workload
	rec := recommendation{
		DB: fc.db, Collection: fc.collection, Column: fc.col.String(), DataType: fc.col.typeName(),
		Action: adv.action,
		Observed: observed{
			Predicates:      w.requests,
			OpClasses:       lo.MapKeys(w.opRequests, func(_ int, op opClass) string { return string(op) }),
			TopFingerprints: topKeys(w.fingerprints, 3),
			TimeCostMs:      summarize(w.costs),
		},
		ExistingIndexes: lo.Map(fc.existing, func(idx indexSnapshot, _ int) existingIndex {
			return existingIndex{IndexName: idx.IndexName, IndexType: idx.IndexType}
		}),
		Candidates: append([]candidate{}, adv.candidates...),
		Statistics: statistics{
			RowCount:      intStat(fc.stats.rowCount),
			DistinctCount: intStat(fc.stats.distinctCount),
			NullFraction:  statValue{fc.stats.nullFraction},
		},
		Notes: adv.notes,
	}
	switch rec.Action {
	case actionValidate:
		rec.ValidationPlan = validationPlan(rec)
	case actionReviewExisting:
		rec.ValidationPlan = reviewPlan(rec)
	}
	return rec
}

// rank orders recommendations by action, then by how many requests filter on
// the column, then by name, and numbers them.
func rank(recs []recommendation) {
	actionOrder := map[string]int{actionValidate: 0, actionReviewExisting: 1, actionNoAction: 2}
	sort.Slice(recs, func(i, j int) bool {
		a, b := recs[i], recs[j]
		if actionOrder[a.Action] != actionOrder[b.Action] {
			return actionOrder[a.Action] < actionOrder[b.Action]
		}
		if a.Observed.Predicates != b.Observed.Predicates {
			return a.Observed.Predicates > b.Observed.Predicates
		}
		return a.DB+"."+a.Collection+"|"+a.Column < b.DB+"."+b.Collection+"|"+b.Column
	})
	for i := range recs {
		recs[i].Rank = i + 1
	}
}

// summarize returns nearest-rank percentiles in milliseconds, rounded to µs.
func summarize(costs []time.Duration) *latency {
	if len(costs) == 0 {
		return nil
	}
	sorted := append([]time.Duration(nil), costs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(sorted)))) - 1
		return ms(sorted[max(i, 0)])
	}
	return &latency{P50: pct(0.5), P95: pct(0.95), Max: ms(sorted[len(sorted)-1]), Samples: len(sorted)}
}

func ms(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Microsecond)) / 1000
}

// topKeys returns up to n keys with the highest counts, ties broken by key.
func topKeys(counts map[string]int, n int) []string {
	keys := lo.Keys(counts)
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	if len(keys) > n {
		keys = keys[:n]
	}
	return keys
}
