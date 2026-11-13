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
	"bytes"
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite the golden reports in testdata/")

// TestAdviseGolden runs the CLI on the fixture log, with and without
// --field-stats, and compares the output byte for byte. The golden files double
// as the README's example output. Regenerate with -update after an intended
// change, then review the diff.
func TestAdviseGolden(t *testing.T) {
	for golden, extra := range map[string][]string{
		"testdata/advice.json":            nil,
		"testdata/advice_with_stats.json": {"--field-stats", "testdata/field_stats.json"},
	} {
		args := append([]string{"--access-log", "testdata/access_log.txt", "--snapshot", "testdata/snapshot.json"}, extra...)
		stdout, stderr, err := runCLI(args...)
		require.NoError(t, err, stderr)

		again, _, err := runCLI(args...)
		require.NoError(t, err)
		require.Equal(t, stdout, again, "two runs on the same input must be byte-identical")

		if *update {
			require.NoError(t, os.WriteFile(golden, []byte(stdout), 0o600))
		}
		want, err := os.ReadFile(golden)
		require.NoError(t, err)
		assert.Equal(t, string(want), stdout, "%s is stale; rerun with -update and review the diff", golden)
	}
}

// TestAdviseGoldenInvariants pins the acceptance criteria of issue #53828 on the
// golden output, so a regenerated golden cannot silently drop them.
func TestAdviseGoldenInvariants(t *testing.T) {
	load := func(extra ...string) (string, map[string]any, map[string]map[string]any) {
		stdout, stderr, err := runCLI(append([]string{"--access-log", "testdata/access_log.txt", "--snapshot", "testdata/snapshot.json"}, extra...)...)
		require.NoError(t, err, stderr)
		var rep map[string]any
		require.NoError(t, jsonUnmarshal(stdout, &rep))
		byColumn := map[string]map[string]any{}
		for _, r := range rep["recommendations"].([]any) {
			rec := r.(map[string]any)
			byColumn[rec["collection"].(string)+"."+rec["column"].(string)] = rec
		}
		return stdout, rep, byColumn
	}
	stdout, rep, byColumn := load()

	for col, rec := range byColumn {
		for i, c := range rec["candidates"].([]any) {
			cand := c.(map[string]any)
			assert.NotEmpty(t, cand["evidence"], "%s candidate %d has no evidence", col, i)
			if cand["index_type"] == "AUTOINDEX" {
				assert.Equal(t, 0, i, "%s: AUTOINDEX, the baseline, comes first", col)
				assert.Equal(t, "baseline", cand["role"])
			} else {
				assert.Equal(t, "alternative", cand["role"], "%s candidate %d", col, i)
			}
		}
		if rec["action"] != "no_action" {
			assert.NotEmpty(t, rec["validation_plan"], "%s needs a validation plan", col)
		}
	}

	price := byColumn["products.price"]
	assert.EqualValues(t, 1, price["rank"], "the most filtered column ranks first")
	assert.Equal(t, "validate_candidates", price["action"])
	assert.Len(t, price["candidates"], 1, "a range filter alone is no evidence for a specific type")

	city := byColumn["products.city"]
	assert.Equal(t, "review_existing", city["action"])
	assert.Contains(t, rep["notes"], "Existing indexes are listed from the snapshot; this report does not show whether any query used them.")
	assert.Equal(t, "unknown", city["statistics"].(map[string]any)["distinct_count"], "missing statistics are reported as unknown")
	assert.Equal(t, []string{"AUTOINDEX"}, candidateIndexTypes(city), "an indexed column is still compared with the baseline; BITMAP needs distinct_count, which is unknown")
	assert.Contains(t, strings.Join(toStrings(city["notes"]), " "), "distinct_count is unknown")
	assert.Nil(t, rep["inputs"].(map[string]any)["field_stats"])

	assert.Equal(t, "no_action", byColumn["products.id"]["action"], "the primary key is never recommended")

	// An index no analyzed request filters on is still reviewed.
	unused := byColumn["idx_probe.price"]
	assert.Equal(t, "review_existing", unused["action"])
	assert.EqualValues(t, 0, unused["observed"].(map[string]any)["predicates"])

	// Redaction: no literal from the fixture's filters, and no client address
	// or trace ID, reaches the report.
	for _, leaked := range []string{`"SF"`, `"LA"`, `"red"`, "SKU-1", "%123%", "[1, 2, 3]", "4.5", "127.0.0.1", "tcp-", "Python-3.0.2", "00000000000000000000000000000001"} {
		assert.NotContains(t, stdout, leaked)
	}

	// With statistics, low cardinality is evidence for BITMAP, and the
	// statistics' provenance is reported.
	_, rep, byColumn = load("--field-stats", "testdata/field_stats.json")
	city = byColumn["products.city"]
	assert.EqualValues(t, 5, city["statistics"].(map[string]any)["distinct_count"])
	assert.Equal(t, []string{"AUTOINDEX", "BITMAP"}, candidateIndexTypes(city))
	assert.Equal(t, "default.products, all 5000 rows", rep["inputs"].(map[string]any)["field_stats"].(map[string]any)["scope"])
}

func candidateIndexTypes(rec map[string]any) []string {
	out := []string{}
	for _, c := range rec["candidates"].([]any) {
		out = append(out, c.(map[string]any)["index_type"].(string))
	}
	return out
}

func toStrings(v any) []string {
	var out []string
	for _, s := range v.([]any) {
		out = append(out, s.(string))
	}
	return out
}

// adviseTestMeta is a snapshot with one collection, t, over testSchemaHelper,
// with the given indexes keyed by the column they cover.
func adviseTestMeta(t *testing.T, indexes map[string][]indexSnapshot) *snapshotSet {
	info := newCollectionInfo("default", "t", testSchemaHelper(t))
	for identifier, list := range indexes {
		for _, idx := range list {
			info.addIndex(mustColumn(t, info, identifier), idx)
		}
	}
	return &snapshotSet{collections: []*collectionInfo{info}, byName: map[string]*collectionInfo{"default.t": info}}
}

// adviseRecords runs advise on a workload built from records and parseSkips.
func adviseRecords(logFiles []string, records []accessRecord, parseSkips skipCounts, snap *snapshotSet, stats *statsSet, engine *ruleEngine) report {
	w := newWorkload(snap)
	for reason, n := range parseSkips {
		w.skipped[reason] += n
	}
	for _, r := range records {
		w.add(r)
	}
	return advise(logFiles, w, stats, engine)
}

func queryRecord(method, status, collection, expr string, cost time.Duration) accessRecord {
	values := map[string]string{
		fieldMethodName: method, fieldStatus: status, fieldDatabase: "default",
		fieldCollection: collection, fieldExpr: expr, fieldTimeCost: cost.String(),
	}
	if cost == 0 {
		values[fieldTimeCost] = "Unknown"
	}
	return accessRecord{values: values}
}

func recFor(t *testing.T, rep report, col string) recommendation {
	for _, r := range rep.Recommendations {
		if r.Column == col {
			return r
		}
	}
	require.Failf(t, "no recommendation", "column %s", col)
	return recommendation{}
}

func candidateTypes(r recommendation) []string {
	var out []string
	for _, c := range r.Candidates {
		out = append(out, c.IndexType)
	}
	return out
}

func TestAdviseSkipReasons(t *testing.T) {
	records := []accessRecord{
		queryRecord("Query", "Successful", "missing", "price > 1", time.Millisecond),
		queryRecord("Query", "Successful", "t", "price >", time.Millisecond),
		queryRecord("Query", "Successful", "t", "  ", time.Millisecond),
		queryRecord("Query", "Failed", "t", "price > 1", time.Millisecond),
		queryRecord("Delete", "Successful", "t", "price > 1", time.Millisecond),
		queryRecord("HybridSearch", "Successful", "t", `["price > 1"]`, time.Millisecond),
		{values: map[string]string{fieldMethodName: "Insert"}},
		queryRecord("Query", "Successful", "t", "price > 1", 0),
	}
	rep := adviseRecords([]string{"/home/someone/logs/access.log"}, records, skipCounts{skipOrphanLine: 2, skipUnmatchedFormat: 1}, adviseTestMeta(t, nil), nil, defaultRuleEngine())

	assert.Equal(t, []string{"access.log"}, rep.Inputs.LogFiles, "only base names, never local paths")
	assert.Equal(t, 9, rep.Inputs.RecordsTotal)
	assert.Equal(t, 1, rep.Inputs.RecordsAnalyzed)
	assert.Equal(t, []skipEntry{
		{skipExpressionParseError, 1},
		{skipHybridSearch, 1},
		{skipMethodOutOfScope, 1},
		{skipNoFilter, 1},
		{skipNoFilterField, 1},
		{skipOrphanLine, 2},
		{skipRequestFailed, 1},
		{skipUnknownCollection, 1},
		{skipUnmatchedFormat, 1},
	}, rep.Inputs.Skipped)

	require.Len(t, rep.Fingerprints, 1)
	assert.Nil(t, rep.Fingerprints[0].TimeCostMs, "an Unknown time_cost is missing, not zero")
	assert.Contains(t, recFor(t, rep, "price").ValidationPlan[0], "the access log has no latency")
}

// A formatter without $method_status gives records an unknown status, which is
// not a failure, so they are analyzed.
func TestAdviseRecordsWithoutStatus(t *testing.T) {
	cfgs := map[string]formatterConfig{
		"base":  defaultFormatterConfigs["base"],
		"query": {Format: "[$time_now] [ACCESS] $method_name [collection: $collection_name] [expr: $method_expr]", Methods: "Query"},
	}
	formatters, err := newFormatterSet(cfgs)
	require.NoError(t, err)
	w := newWorkload(adviseTestMeta(t, nil))
	_, err = readRecords(strings.NewReader("[2026/10/15 10:00:00.000 +00:00] [ACCESS] Query [collection: t] [expr: price > 1]\n"), "access.log", formatters, w.skipped, w.add)
	require.NoError(t, err)

	rep := advise([]string{"access.log"}, w, nil, defaultRuleEngine())
	assert.Equal(t, 1, rep.Inputs.RecordsAnalyzed)
	assert.Empty(t, rep.Inputs.Skipped)
}

// A text_match filter parses against a schema built from a snapshot, so the
// other conditions of its expression are analyzed. Its column gets no candidate
// and ranks after every column that has one.
func TestAdviseTextMatch(t *testing.T) {
	snap, _, err := parseSnapshot([]byte(`{"format_version": 1, "collections": [{"name": "t", "fields": [
		{"name": "id", "data_type": "Int64", "is_primary_key": true},
		{"name": "review", "data_type": "VarChar"},
		{"name": "price", "data_type": "Int64"}]}]}`))
	require.NoError(t, err)
	records := []accessRecord{queryRecord("Query", "Successful", "t", `text_match(review, "great") and price > 1`, time.Millisecond)}

	rep := adviseRecords(nil, records, skipCounts{}, snap, nil, defaultRuleEngine())
	assert.Equal(t, 1, rep.Inputs.RecordsAnalyzed)
	assert.Empty(t, rep.Inputs.Skipped)
	require.Len(t, rep.Recommendations, 2)
	price, review := rep.Recommendations[0], rep.Recommendations[1]
	assert.Equal(t, "price", price.Column)
	assert.Equal(t, actionValidate, price.Action)
	assert.Equal(t, "review", review.Column)
	assert.Equal(t, actionNoAction, review.Action)
	assert.Empty(t, review.Candidates)
	require.Len(t, review.Notes, 1)
	assert.Contains(t, review.Notes[0], "text_match")
}

// A request through an alias counts for its collection, and the collection's
// indexed columns are reported once.
func TestAdviseAlias(t *testing.T) {
	snap, _, err := parseSnapshot([]byte(`{"format_version": 1, "collections": [{"name": "t", "aliases": ["t_alias"], "fields": [
		{"name": "id", "data_type": "Int64", "is_primary_key": true}, {"name": "price", "data_type": "Int64"}, {"name": "city", "data_type": "VarChar"}],
		"indexes": [{"index_name": "city", "field_name": "city", "index_type": "INVERTED"}]}]}`))
	require.NoError(t, err)
	rep := adviseRecords(nil, []accessRecord{queryRecord("Query", "Successful", "t_alias", "price > 1", time.Millisecond)}, skipCounts{}, snap, nil, defaultRuleEngine())
	assert.Equal(t, 1, rep.Inputs.RecordsAnalyzed)
	assert.Equal(t, []string{"t.price", "t.city"}, lo.Map(rep.Recommendations, func(r recommendation, _ int) string { return r.Collection + "." + r.Column }))
}

// A RESTful method is reported by its path, without the query string.
func TestAdviseRESTQueryString(t *testing.T) {
	rep := adviseRecords(nil, []accessRecord{queryRecord("/v2/vectordb/entities/query?timeout=5", "Successful", "t", "price > 1", time.Millisecond)},
		skipCounts{}, adviseTestMeta(t, nil), nil, defaultRuleEngine())
	assert.Equal(t, 1, rep.Inputs.RecordsAnalyzed)
	require.Len(t, rep.Fingerprints, 1)
	assert.Equal(t, map[string]int{"/v2/vectordb/entities/query": 1}, rep.Fingerprints[0].Methods)
}

// An index with json_cast_type JSON serves the paths beneath it: a filtered
// path is reviewed against it, and the index is not reported as unused.
func TestAdviseJSONCastIndex(t *testing.T) {
	jsonIndex := indexSnapshot{IndexName: "meta_all", FieldName: "meta", IndexType: indexInverted, Params: map[string]any{"json_cast_type": "json"}}
	rep := adviseRecords(nil, []accessRecord{queryRecord("Query", "Successful", "t", `meta["brand"] == "x"`, time.Millisecond)},
		skipCounts{}, adviseTestMeta(t, map[string][]indexSnapshot{"meta": {jsonIndex}}), nil, defaultRuleEngine())
	require.Len(t, rep.Recommendations, 1, "the indexed field is not reported separately")
	brand := rep.Recommendations[0]
	assert.Equal(t, `meta["brand"]`, brand.Column)
	assert.Equal(t, actionReviewExisting, brand.Action)
	assert.Equal(t, []existingIndex{{IndexName: "meta_all", IndexType: indexInverted}}, brand.ExistingIndexes)
	assert.Equal(t, []string{indexAuto}, candidateTypes(brand), "a VARCHAR-cast candidate is compared with the JSON-cast index")

	rep = adviseRecords(nil, nil, skipCounts{}, adviseTestMeta(t, map[string][]indexSnapshot{"meta": {jsonIndex}}), nil, defaultRuleEngine())
	assert.Equal(t, "meta", recFor(t, rep, "meta").Column, "with no filtered path beneath it, the index is reviewed on its own")
}

// advise groups each request's predicates by column and gives every column its
// statistics. The rules themselves are tested one by one in rules_test.go.
func TestAdviseCandidates(t *testing.T) {
	ndv := func(n int64) *int64 { return &n }
	stats := &statsSet{
		rowCount: map[string]*int64{"default.t": ndv(1000)},
		fields:   map[string]map[string]fieldStats{"default.t": {"city": {DistinctCount: ndv(4)}}},
	}
	var records []accessRecord
	for _, e := range []string{`city == "a" and price > 1`, `city == "b" or city == "c"`} {
		records = append(records, queryRecord("Query", "Successful", "t", e, time.Millisecond))
	}
	rep := adviseRecords(nil, records, skipCounts{}, adviseTestMeta(t, nil), stats, defaultRuleEngine())

	city := recFor(t, rep, "city")
	assert.Equal(t, 2, city.Observed.Predicates, "a request counts once per column, however many predicates it has on it")
	assert.Equal(t, map[string]int{"eq": 2}, city.Observed.OpClasses)
	assert.Equal(t, []string{"AUTOINDEX", "BITMAP"}, candidateTypes(city), "the column's distinct_count reaches the rules")
	assert.EqualValues(t, 1000, *city.Statistics.RowCount.v)
	assert.Equal(t, "city", rep.Recommendations[0].Column, "ranked by number of filtering requests")
	assert.Equal(t, 1, recFor(t, rep, "price").Observed.Predicates)
}

func TestAdviseReviewExisting(t *testing.T) {
	ndv := func(n int64) *int64 { return &n }
	stats := &statsSet{fields: map[string]map[string]fieldStats{"default.t": {"city": {DistinctCount: ndv(4)}}}}
	meta := adviseTestMeta(t, map[string][]indexSnapshot{
		"price":  {{IndexName: "p", IndexType: "STL_SORT"}},
		"city":   {{IndexName: "c", IndexType: "INVERTED"}},
		"rating": {{IndexName: "r", IndexType: "STL_SORT"}},
		"tags":   {{IndexName: "t", IndexType: "INVERTED"}},
		"vec":    {{IndexName: "v", IndexType: "AUTOINDEX"}},
	})
	rep := adviseRecords(nil, []accessRecord{
		queryRecord("Query", "Successful", "t", "price > 1", time.Millisecond),
		queryRecord("Query", "Successful", "t", `city == "a"`, time.Millisecond),
		queryRecord("Query", "Successful", "t", "price > rating", time.Millisecond),
	}, skipCounts{}, meta, stats, defaultRuleEngine())

	// Requests on an indexed column are analyzed like any other: the column is
	// compared with the baseline and every alternative with evidence.
	price := recFor(t, rep, "price")
	assert.Equal(t, "review_existing", price.Action)
	assert.Equal(t, []existingIndex{{IndexName: "p", IndexType: "STL_SORT"}}, price.ExistingIndexes)
	assert.Equal(t, 2, price.Observed.Predicates)
	assert.Equal(t, []string{"AUTOINDEX"}, candidateTypes(price), "AUTOINDEX builds HYBRID, not the existing STL_SORT")
	assert.Contains(t, price.ValidationPlan[0], "does not show it is used")

	city := recFor(t, rep, "city")
	assert.Equal(t, []string{"AUTOINDEX", "BITMAP"}, candidateTypes(city))
	assert.Contains(t, city.ValidationPlan[2], "whose only index on city is index_type BITMAP")

	// An indexed column filtered only in ways no scalar index serves is still
	// reviewed, not dropped.
	rating := recFor(t, rep, "rating")
	assert.Equal(t, "review_existing", rating.Action)
	assert.Equal(t, 1, rating.Observed.Predicates)
	assert.Contains(t, rating.Notes[0], "No scalar index serves these predicates")
	assert.Contains(t, rating.ValidationPlan[0], "one with the existing index and one without")

	// An index no analyzed request filters on is reported, with no claim that
	// it is used or unused beyond these logs.
	tags := recFor(t, rep, "tags")
	assert.Equal(t, "review_existing", tags.Action)
	assert.Zero(t, tags.Observed.Predicates)
	assert.Empty(t, tags.Candidates)
	assert.Contains(t, tags.Notes[0], "No analyzed request filters on this column")
	assert.Contains(t, tags.ValidationPlan[0], "These logs may be a sample")

	_, listed := lo.Find(rep.Recommendations, func(r recommendation) bool { return r.Column == "vec" })
	assert.False(t, listed, "vector indexes are out of scope")
}

// The report must not depend on record order.
func TestAdviseOrderIndependent(t *testing.T) {
	var records []accessRecord
	_, err := readRecordsFile("testdata/access_log.txt", defaultFormatterSet(t), skipCounts{}, appendTo(&records))
	require.NoError(t, err)
	meta := loadFixtureSnapshot(t)

	var want bytes.Buffer
	require.NoError(t, writeJSON(&want, adviseRecords(nil, records, skipCounts{}, meta, nil, defaultRuleEngine())))
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5; i++ {
		rng.Shuffle(len(records), func(a, b int) { records[a], records[b] = records[b], records[a] })
		var got bytes.Buffer
		require.NoError(t, writeJSON(&got, adviseRecords(nil, records, skipCounts{}, meta, nil, defaultRuleEngine())))
		require.Equal(t, want.String(), got.String())
	}
}

func jsonUnmarshal(s string, v any) error { return json.Unmarshal([]byte(s), v) }
