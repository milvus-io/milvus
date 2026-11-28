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
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const statsFormatVersion = 1

// statsFile is the optional --field-stats input: field statistics supplied by
// an operator, since Milvus does not expose per-field distinct counts
// (GetCollectionStatistics gives only the row count). Anything absent is
// reported as "unknown" rather than guessed. Source, CollectedAt and Scope say
// where the numbers came from; they are required and copied into the report.
type statsFile struct {
	FormatVersion int `json:"format_version"`
	statsProvenance
	Collections []collectionStats `json:"collections"`
}

type statsProvenance struct {
	Source      string `json:"source"`       // how the numbers were obtained
	CollectedAt string `json:"collected_at"` // RFC 3339 time they were collected
	Scope       string `json:"scope"`        // which data they cover, e.g. a full collection or a sample
}

type collectionStats struct {
	DB       string                `json:"db"`
	Name     string                `json:"name"`
	RowCount *int64                `json:"row_count,omitempty"`
	Fields   map[string]fieldStats `json:"fields"` // field name or JSON path -> stats
}

type fieldStats struct {
	DistinctCount *int64   `json:"distinct_count,omitempty"`
	NullFraction  *float64 `json:"null_fraction,omitempty"`
}

// statsSet holds statistics keyed by collection and normalized column, so a
// dynamic key written as color or $meta["color"] matches the same predicate.
type statsSet struct {
	provenance statsProvenance
	rowCount   map[string]*int64
	fields     map[string]map[string]fieldStats
}

// lookup returns the row count and field statistics for col; nil pointers mean
// unknown. A nil set has no statistics at all.
func (s *statsSet) lookup(db, name string, col column) (*int64, fieldStats) {
	if s == nil {
		return nil, fieldStats{}
	}
	key := collectionKey(db, name)
	return s.rowCount[key], s.fields[key][col.String()]
}

// loadStatsFile loads the field statistics at path. It also returns warnings
// about the keys it ignored.
func loadStatsFile(path string, meta *snapshotSet) (*statsSet, []string, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is a CLI argument
	if err != nil {
		return nil, nil, merr.WrapErrIoFailed(path, err)
	}
	s, warnings, err := parseStats(data, meta)
	if err != nil {
		return nil, nil, merr.Wrapf(err, "stats file %s", path)
	}
	return s, prefixAll("stats file "+path+": ", warnings), nil
}

func parseStats(data []byte, meta *snapshotSet) (*statsSet, []string, error) {
	var file statsFile
	warnings, err := decodeInput(data, &file)
	if err != nil {
		return nil, nil, merr.WrapErrParameterInvalidErr(err, "invalid stats JSON")
	}
	if err := checkFormatVersion(file.FormatVersion, statsFormatVersion); err != nil {
		return nil, nil, err
	}

	for _, f := range []struct{ name, value string }{{"source", file.Source}, {"collected_at", file.CollectedAt}, {"scope", file.Scope}} {
		if strings.TrimSpace(f.value) == "" {
			return nil, nil, merr.WrapErrParameterMissingMsg("stats file must record %s", f.name)
		}
	}
	if _, err := time.Parse(time.RFC3339, file.CollectedAt); err != nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg("collected_at %q is not an RFC 3339 time", file.CollectedAt)
	}

	set := &statsSet{provenance: file.statsProvenance, rowCount: map[string]*int64{}, fields: map[string]map[string]fieldStats{}}
	for i, c := range file.Collections {
		if c.Name == "" {
			return nil, nil, merr.WrapErrParameterMissingMsg("collection %d has no name", i)
		}
		info, ok := meta.lookup(c.DB, c.Name)
		if !ok {
			return nil, nil, merr.WrapErrParameterInvalidMsg("stats for collection %s, which is not in the snapshot", collectionKey(c.DB, c.Name))
		}
		key := collectionKey(info.db, info.name) // an alias names its collection
		if _, dup := set.fields[key]; dup {
			return nil, nil, merr.WrapErrParameterInvalidMsg("stats for collection %s listed twice, by name or alias", key)
		}
		if c.RowCount != nil && *c.RowCount < 0 {
			return nil, nil, merr.WrapErrParameterInvalidMsg("collection %s: row_count must not be negative", key)
		}
		set.rowCount[key] = c.RowCount
		set.fields[key] = map[string]fieldStats{}
		for name, fs := range c.Fields {
			if fs.DistinctCount != nil && *fs.DistinctCount < 0 {
				return nil, nil, merr.WrapErrParameterInvalidMsg("collection %s field %s: distinct_count must not be negative", key, name)
			}
			if fs.NullFraction != nil && (*fs.NullFraction < 0 || *fs.NullFraction > 1) {
				return nil, nil, merr.WrapErrParameterInvalidMsg("collection %s field %s: null_fraction must be within [0, 1]", key, name)
			}
			col, err := resolveIdentifier(info.helper, name)
			if err != nil {
				return nil, nil, merr.Wrapf(err, "collection %s field %s", key, name)
			}
			if _, dup := set.fields[key][col.String()]; dup {
				return nil, nil, merr.WrapErrParameterInvalidMsg("collection %s: field %s is listed more than once", key, col)
			}
			set.fields[key][col.String()] = fs
		}
	}
	return set, warnings, nil
}
