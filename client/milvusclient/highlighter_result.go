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

package milvusclient

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// Highlight is the per-row highlight data for one field.
//
// Fragments carries the highlighted snippets, with pre/post tags interleaved
// around matched terms. Scores carries per-fragment relevance from the
// semantic highlighter; for lexical highlighting it is nil.
//
// One Highlight corresponds to one row of the search result. The outer
// ResultSet.Highlights map is keyed by field name; the value is one Highlight
// per row, in the same order as the rest of the result set.
type Highlight struct {
	Fragments []string
	Scores    []float32
}

// parseHighlights extracts per-field, per-row Highlights from the server's
// SearchResultData. offset and end are the half-open row range [offset, end)
// for the current query (nq). Each HighlightResult.Datas slice is row-aligned,
// so we slice it directly to the current query's row window.
//
// Returns a nil map when the server response carries no HighlightResults, so
// callers can use a simple `if rs.Highlights["text"] != nil` guard.
func parseHighlights(results *schemapb.SearchResultData, offset, end int) (map[string][]Highlight, error) {
	if offset > end {
		return nil, fmt.Errorf("parseHighlights: offset %d must be <= end %d", offset, end)
	}
	if offset < 0 || end < 0 {
		return nil, fmt.Errorf("parseHighlights: offset and end must be non-negative, got offset=%d end=%d", offset, end)
	}
	raw := results.GetHighlightResults()
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string][]Highlight, len(raw))
	for _, hr := range raw {
		if hr == nil {
			continue
		}
		datas := hr.GetDatas()
		// The proto's datas slice is row-aligned across all nq. We slice to
		// [offset, end). Validate bounds once per field, then convert.
		if end > len(datas) {
			return nil, errors.Newf("parseHighlights: end %d exceeds HighlightResult %q length %d", end, hr.GetFieldName(), len(datas))
		}
		window := datas[offset:end]
		rows := make([]Highlight, len(window))
		for i, d := range window {
			if d == nil {
				continue
			}
			rows[i] = Highlight{
				Fragments: append([]string(nil), d.GetFragments()...),
				Scores:    append([]float32(nil), d.GetScores()...),
			}
		}
		out[hr.GetFieldName()] = rows
	}
	return out, nil
}
