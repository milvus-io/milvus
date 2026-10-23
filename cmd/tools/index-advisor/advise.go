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
	"path/filepath"
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// advise turns the workload (workload.go) into a report: it builds a field
// context for every column, runs engine on each context (engine.go), and
// assembles and ranks the recommendations (report.go). The report is
// deterministic: every list in it is sorted, and it carries no wall-clock time.
func advise(logFiles []string, w *workload, stats *statsSet, engine *ruleEngine) report {
	rep := report{
		FormatVersion: reportFormatVersion,
		Inputs: inputsSummary{
			LogFiles:        lo.Map(logFiles, func(f string, _ int) string { return filepath.Base(f) }),
			RecordsTotal:    w.records + w.skipped[skipUnmatchedFormat],
			RecordsAnalyzed: w.analyzed,
			Skipped:         []skipEntry{},
		},
		Fingerprints:    fingerprintSummaries(w),
		Recommendations: []recommendation{},
		Notes:           fixedNotes,
	}
	if stats != nil {
		rep.Inputs.FieldStats = &stats.provenance
	}
	for _, reason := range lo.Keys(w.skipped) {
		rep.Inputs.Skipped = append(rep.Inputs.Skipped, skipEntry{Reason: reason, Count: w.skipped[reason]})
	}
	sort.Slice(rep.Inputs.Skipped, func(i, j int) bool { return rep.Inputs.Skipped[i].Reason < rep.Inputs.Skipped[j].Reason })

	for _, fc := range fieldContexts(w, stats) {
		rep.Recommendations = append(rep.Recommendations, newRecommendation(fc, engine.evaluate(fc)))
	}
	rank(rep.Recommendations)
	return rep
}

// fieldContexts returns a context for every column the rules evaluate: each
// filtered column, indexed or not, and each indexed scalar column that no
// analyzed request filters on, so an existing index is never taken as used. An
// index with json_cast_type JSON that serves a filtered path beneath it is
// reviewed with that path instead.
func fieldContexts(w *workload, stats *statsSet) []*fieldContext {
	var out []*fieldContext
	for _, f := range w.fields {
		out = append(out, newFieldContext(f.info, f.col, f.workload, stats))
	}
	for _, info := range w.snap.collections {
		for key, col := range info.indexedColumns {
			_, filtered := w.fields[collectionKey(info.db, info.name)+"|"+key]
			if filtered || typeutil.IsVectorType(col.dataType) || servesFilteredPath(w, info, col) {
				continue
			}
			out = append(out, newFieldContext(info, col, newFieldWorkload(), stats))
		}
	}
	return out
}

// servesFilteredPath reports whether an index on col with json_cast_type JSON
// serves a filtered path beneath col.
func servesFilteredPath(w *workload, info *collectionInfo, col column) bool {
	return lo.SomeBy(info.indexes[col.String()], isJSONCast) &&
		lo.SomeBy(lo.Values(w.fields), func(f *filteredField) bool { return f.info == info && coversPath(col, f.col) })
}

func newFieldContext(info *collectionInfo, col column, wl *fieldWorkload, stats *statsSet) *fieldContext {
	rowCount, fs := stats.lookup(info.db, info.name, col)
	fc := &fieldContext{
		db: info.db, collection: info.name, col: col, kind: kindOf(col), workload: wl,
		stats:    fieldStatistics{rowCount: rowCount, distinctCount: fs.DistinctCount, nullFraction: fs.NullFraction},
		existing: info.indexesOn(col),
	}
	if fc.kind == kindJSONPath {
		fc.castType = inferCastType(wl.valueKinds)
	}
	return fc
}

// inferCastType returns the json_cast_type every literal kind compared with a
// JSON path maps to (int and float both map to DOUBLE); "" when there is no
// literal, or the literals need different cast types or none.
func inferCastType(valueKinds map[string]bool) string {
	casts := lo.Uniq(lo.Map(lo.Keys(valueKinds), func(k string, _ int) string { return jsonCastType(k) }))
	if len(casts) != 1 {
		return ""
	}
	return casts[0]
}
