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

package datacoord

import (
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// BackfillResult is the decoded form of the JSON produced by the Spark
// backfill job. Only fields required for commit are modeled; other diagnostic
// fields (executionTimeMs, usedSourceByField, etc.) are ignored.
//
// Reference: spark-milvus/docs/backfill-result-json-format.md
type BackfillResult struct {
	Success       bool                       `json:"success"`
	CollectionID  int64                      `json:"collectionId"`
	PartitionID   int64                      `json:"partitionId"`
	NewFieldNames []string                   `json:"newFieldNames"`
	Segments      map[string]BackfillSegment `json:"segments"` // key = segmentID as decimal string
}

// BackfillSegment is one entry in BackfillResult.Segments.
//
// V3 entries carry {version, manifestPaths} and omit storage_version/column_groups.
// V2 entries carry {storage_version: 2, column_groups: [...]} and have version == -1.
type BackfillSegment struct {
	Version        int64                   `json:"version"` // V3: committedVersion (>0); V2: -1
	RowCount       int64                   `json:"rowCount"`
	OutputPath     string                  `json:"outputPath"`
	ManifestPaths  []string                `json:"manifestPaths"`
	StorageVersion *int64                  `json:"storage_version,omitempty"` // ptr to distinguish absent vs 0
	ColumnGroups   []BackfillV2ColumnGroup `json:"column_groups,omitempty"`
}

// BackfillV2ColumnGroup describes one V2 column group produced by backfill.
// Invariant from backfill: field_ids has exactly one element (single-field groups).
type BackfillV2ColumnGroup struct {
	FieldIDs    []int64  `json:"field_ids"`
	BinlogFiles []string `json:"binlog_files"` // ascending by log_id
	RowCount    int64    `json:"row_count"`    // group-level; trusted as segment NumOfRows
}

// IsV2 reports whether this entry represents a StorageV2 segment.
func (s *BackfillSegment) IsV2() bool {
	return s.StorageVersion != nil && *s.StorageVersion == storage.StorageV2 && len(s.ColumnGroups) > 0
}

// knownObjectSchemes lists URI schemes recognized by normalizeObjectKey.
// All map to "treat host as bucket, path as object key".
var knownObjectSchemes = map[string]struct{}{
	"s3":    {},
	"s3a":   {},
	"s3n":   {},
	"gs":    {},
	"oss":   {},
	"minio": {},
}

// normalizeObjectKey converts a spark-style URI into a chunk-manager object key.
//
// Rules:
//  1. No scheme -> trim leading '/', return as-is.
//  2. Known scheme -> parse "<scheme>://<host>/<path>"; if expectedBucket != "" and
//     host != expectedBucket, return ErrBucketMismatch. Otherwise return <path>.
//  3. Unknown scheme -> return ErrUnsupportedScheme.
func normalizeObjectKey(raw, expectedBucket string) (string, error) {
	if raw == "" {
		return "", errors.New("empty object path")
	}
	if !strings.Contains(raw, "://") {
		key := strings.TrimPrefix(raw, "/")
		if key == "" {
			return "", errors.Newf("empty object key in %q", raw)
		}
		return key, nil
	}
	idx := strings.Index(raw, "://")
	scheme := strings.ToLower(raw[:idx])
	if _, ok := knownObjectSchemes[scheme]; !ok {
		return "", errors.Newf("unsupported object URI scheme %q in %q", scheme, raw)
	}
	rest := raw[idx+3:]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return "", errors.Newf("malformed object URI %q: missing object key", raw)
	}
	bucket := rest[:slash]
	key := rest[slash+1:]
	if expectedBucket != "" && bucket != expectedBucket {
		return "", errors.Newf("object URI bucket %q differs from datacoord bucket %q (path=%s)", bucket, expectedBucket, raw)
	}
	// Reject inputs like "s3a://bucket/" that parse to an empty key -- passing
	// an empty key to chunkManager.Read has undefined behavior across SDKs.
	if key == "" {
		return "", errors.Newf("empty object key in %q", raw)
	}
	return key, nil
}

// parseLogIDFromKey extracts the trailing path segment as int64. Returns 0 with
// ok=false if the trailing segment is not a decimal integer (e.g. when it has a
// file extension). Callers should proceed even on ok=false -- LogID is
// informational for this particular flow.
func parseLogIDFromKey(key string) (int64, bool) {
	base := path.Base(key)
	if base == "" || base == "." || base == "/" {
		return 0, false
	}
	id, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// buildV2Groups constructs the datapb.FieldBinlog map used by the V2 update
// operator. EntriesNum is distributed across BinlogFiles by integer division
// (remainder added to the last file) -- the sum equals g.RowCount, which the
// backfill contract guarantees to equal segment.NumOfRows.
//
// This function does NOT read parquet footers. It trusts the row counts in the
// result JSON per the backfill contract (see
// spark-milvus/docs/backfill-result-json-format.md).
func buildV2Groups(bucket string, entry *BackfillSegment) (map[int64]*datapb.FieldBinlog, error) {
	out := make(map[int64]*datapb.FieldBinlog, len(entry.ColumnGroups))
	for i := range entry.ColumnGroups {
		g := &entry.ColumnGroups[i]
		if len(g.FieldIDs) != 1 {
			return nil, errors.Newf("backfill invariant violated: column group has %d field_ids (expected 1)", len(g.FieldIDs))
		}
		n := int64(len(g.BinlogFiles))
		if n == 0 {
			return nil, errors.Newf("column group for field %d has no binlog files", g.FieldIDs[0])
		}
		fid := g.FieldIDs[0]
		if _, dup := out[fid]; dup {
			return nil, errors.Newf("duplicate column group for field %d", fid)
		}
		// row_count flows into EntriesNum; non-positive values are undefined
		// (zero collapses presence markers, negatives break accounting).
		if g.RowCount <= 0 {
			return nil, errors.Newf("column group for field %d has non-positive row_count %d", fid, g.RowCount)
		}
		avg := g.RowCount / n
		rem := g.RowCount - avg*n
		binlogs := make([]*datapb.Binlog, 0, n)
		for idx, p := range g.BinlogFiles {
			key, err := normalizeObjectKey(p, bucket)
			if err != nil {
				return nil, err
			}
			rows := avg
			if int64(idx) == n-1 {
				rows += rem
			}
			logID, ok := parseLogIDFromKey(key)
			if !ok {
				return nil, errors.Newf("column group for field %d has binlog file %q with non-numeric trailing segment", fid, p)
			}
			// LogPath must be empty at persistence time -- catalog.checkLogID
			// rejects any Binlog with LogPath != "" (see
			// internal/metastore/kv/datacoord/util.go). Canonical on-disk
			// form is {LogID, LogPath:""}; DecompressBinLog reconstructs the
			// path from LogID + (collection, partition, segment, field) at
			// load time.
			binlogs = append(binlogs, &datapb.Binlog{
				EntriesNum: rows,
				LogID:      logID,
			})
		}
		out[fid] = &datapb.FieldBinlog{
			FieldID: fid,
			// ChildFields carries the real field IDs that index creation
			// (getSegmentBinlogFields) and backfill-compaction detection
			// (getMissingFunctions) rely on. Without it the new group is
			// invisible to both paths; it also lets the operator's strip
			// logic reference-count the field out of the old groups.
			// The backfill invariant guarantees len(g.FieldIDs) == 1.
			ChildFields: []int64{fid},
			Binlogs:     binlogs,
		}
	}
	return out, nil
}

// bucketFromChunkManager returns the bucket name of the given chunk manager if
// it exposes BucketName(); otherwise returns an empty string (bucket check will
// be skipped). Used to avoid a hard dependency on *RemoteChunkManager.
func bucketFromChunkManager(cm storage.ChunkManager) string {
	type bucketNameProvider interface {
		BucketName() string
	}
	if p, ok := cm.(bucketNameProvider); ok {
		return p.BucketName()
	}
	return ""
}
