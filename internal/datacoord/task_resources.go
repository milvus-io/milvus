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
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file is the ONE place DataCoord prices a task.
//
// It lives on the coordinator rather than on the worker because this is the
// side that holds what the estimate is derived from. SegmentInfo.Stats is
// populated on every storage version; the per-FieldBinlog arrays a worker
// would have to reconstruct from are deliberately not persisted for V3
// segments (internal/metastore/kv/datacoord/kv_catalog.go skips writing them),
// so a worker that recomputes locally prices a multi-GiB V3 compaction at the
// estimator's floor. The worker is handed the answer instead.

// compactionRequirement prices a compaction from its resolved input segments.
//
// The delete term is read through EnsureStats().GetDeltaBinlogSize() rather
// than by walking GetDeltalogs(): the array is empty on a V3 segment loaded
// after a restart, and walking it there silently drops the whole delete
// payload out of the estimate.
//
// L0 sums the delete payload across segments while everything else takes the
// largest single segment, mirroring the two load shapes on the worker --
// ComposeDeleteDataFromSegments holds every segment's deletes at once, while
// ComposeDeleteFromDeltalogs holds one segment's at a time.
func compactionRequirement(compactionType datapb.CompactionType, segments []*SegmentInfo, schema *schemapb.CollectionSchema) taskresource.Requirement {
	var totalMemory, totalRows, deleteBytes, storageVersion int64
	isL0 := compactionType == datapb.CompactionType_Level0DeleteCompaction

	for _, segment := range segments {
		if segment == nil {
			continue
		}
		totalMemory += segmentMemorySize(segment, schema)
		totalRows += segment.GetNumOfRows()

		segDelete := segment.EnsureStats().GetDeltaBinlogSize()
		if isL0 {
			deleteBytes += segDelete
		} else if segDelete > deleteBytes {
			deleteBytes = segDelete
		}

		// Storage version is per segment, not per task. Take the max: a V3
		// segment in a mixed plan dominates the memory profile.
		if v := segment.GetStorageVersion(); v > storageVersion {
			storageVersion = v
		}
	}

	return taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:                  compactionType,
		StorageVersion:        storageVersion,
		TotalMemorySize:       totalMemory,
		TotalRows:             totalRows,
		MaxSegmentDeleteBytes: deleteBytes,
	})
}

// indexBuildRequirement prices one index build.
//
// The field size comes from the closed form when the type has one -- that is
// what the build path itself allocates, and it is the only route that survives
// V3, where no per-field byte figure exists in the metadata at all. Everything
// else falls back to the per-field binlog bytes and then to
// fieldBytesFromSchema.
func indexBuildRequirement(
	segment *SegmentInfo,
	schema *schemapb.CollectionSchema,
	field *schemapb.FieldSchema,
	indexType string,
) taskresource.Requirement {
	return taskresource.EstimateIndexBuild(taskresource.IndexInput{
		IndexType:       indexType,
		FieldMemorySize: fieldMemorySize(segment, schema, field),
		StorageVersion:  segment.GetStorageVersion(),
	})
}

// fieldMemorySize is the uncompressed bytes one field occupies in a segment.
//
// Three routes, in descending order of how much they actually know:
//
//  1. The closed form for a fixed-width type: dim x rows x element width. Exact,
//     and independent of what the metadata persisted, so it is the only one that
//     works unchanged on V3.
//  2. The field's own binlog bytes. Exact on V1/V2 and on a V3 segment whose
//     arrays are still in memory; returns the whole column group on the packed
//     layouts, which over-states, which is the safe direction.
//  3. fieldBytesFromSchema, which apportions the segment total. Coarse, but it
//     is the only thing left once V3 has dropped the arrays.
func fieldMemorySize(segment *SegmentInfo, schema *schemapb.CollectionSchema, field *schemapb.FieldSchema) int64 {
	if size := taskresource.FixedWidthFieldBytes(field, segment.GetNumOfRows()); size > 0 {
		return size
	}
	if size := taskresource.SumFieldBinlogMemoryForField(segment.GetBinlogs(), field.GetFieldID()); size > 0 {
		return size
	}
	return fieldBytesFromSchema(segment, schema, map[int64]bool{field.GetFieldID(): true})
}

// segmentMemorySize is a segment's uncompressed bytes, with a fallback for the
// segments whose byte figure does not survive a DataCoord restart.
//
// getSegmentSize reads EnsureStats(), which is right for every segment whose
// producer wrote Statistics. EXTERNAL-COLLECTION segments historically did
// not: the refresh task put the sampled size only into fake FieldBinlogs, and
// a manifest-bearing segment is a V3 segment to the catalog, which strips
// those arrays on persist. Such a segment reloads with no Stats and no
// binlogs, so it reports zero bytes and would be priced at the estimator
// floor -- the same collapse this whole change exists to remove, arriving by
// a different route.
//
// internal/datanode/external now persists Stats, so newly refreshed segments
// do not reach this fallback. It stays for the ones already written that way,
// and it is deliberately not silent: a segment with rows but no bytes is a
// metadata defect worth seeing rather than papering over.
func segmentMemorySize(segment *SegmentInfo, schema *schemapb.CollectionSchema) int64 {
	if size := segment.getSegmentSize(); size > 0 {
		return size
	}
	rows := segment.GetNumOfRows()
	if rows <= 0 || schema == nil {
		return 0
	}
	perRow, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil || perRow <= 0 {
		return 0
	}
	size := int64(perRow) * rows
	mlog.Warn(context.TODO(), "segment reports no bytes; pricing it from the schema instead",
		mlog.Int64("segmentID", segment.GetID()),
		mlog.Int64("numRows", rows),
		mlog.String("manifestPath", segment.GetManifestPath()),
		mlog.Int64("schemaDerivedBytes", size))
	return size
}

// fieldBytesFromSchema apportions a segment's insert bytes across a set of
// fields when no per-field figure survives in the metadata.
//
// It exists for storage V3. V3 persists SegmentInfo.Stats but not the
// per-FieldBinlog arrays, so a V3 segment loaded after a DataCoord restart
// carries a whole-segment byte total and nothing finer. Charging the whole
// segment for a job that touches one field of twenty is the alternative, and
// it over-charges by the field count.
//
// The apportionment is: every fixed-width field in the schema has an exactly
// computable size (rows x width), so subtract all of them from the segment
// total; whatever remains belongs to the variable-width fields, and is split
// evenly among them because nothing in the metadata says how it divides. Even
// splitting is a guess, but it is a bounded one -- the sum over all fields is
// still exactly the segment total, so a job touching every field is charged
// the segment and one touching a single field can never be charged more.
func fieldBytesFromSchema(segment *SegmentInfo, schema *schemapb.CollectionSchema, ids map[int64]bool) int64 {
	if len(ids) == 0 {
		return 0
	}
	total := segmentMemorySize(segment, schema)
	rows := segment.GetNumOfRows()
	if schema == nil || rows <= 0 || total <= 0 {
		return total
	}

	var (
		fixedTotal    int64
		wantedFixed   int64
		variableCount int64
		wantedVar     int64
	)
	for _, f := range typeutil.GetAllFieldSchemas(schema) {
		size := taskresource.FixedWidthFieldBytes(f, rows)
		if size > 0 {
			fixedTotal += size
			if ids[f.GetFieldID()] {
				wantedFixed += size
			}
			continue
		}
		variableCount++
		if ids[f.GetFieldID()] {
			wantedVar++
		}
	}

	if wantedVar == 0 {
		// Every wanted field was exactly computable; no need to guess at all.
		return wantedFixed
	}
	remainder := total - fixedTotal
	if remainder < 0 || variableCount == 0 {
		// The fixed-width fields already account for more than the segment
		// reports -- a compressed-but-uncounted layout, or a stale Stats.
		// Falling back to the whole segment over-charges, which is the
		// direction that does not OOM the node.
		return total
	}
	return wantedFixed + remainder/variableCount*wantedVar
}

// statsRequirement prices one stats sub-job from the fields it actually
// touches, rather than from the whole segment the way calculateStatsTaskSlot
// did. A text-index build over one matched field of twenty was charged twenty
// times what it reads.
func statsRequirement(segment *SegmentInfo, schema *schemapb.CollectionSchema, subJob indexpb.StatsSubJob) taskresource.Requirement {
	ids, recognized := taskresource.StatsTouchedFieldIDs(subJob, schema)
	var touched int64
	switch {
	case !recognized || len(ids) == 0:
		// An unrecognized sub-job has no known field subset, and a recognized
		// one that matched nothing has no isolable field. Both charge the whole
		// segment rather than guessing: under-provisioning is the direction
		// that causes OOM.
		touched = segmentMemorySize(segment, schema)
	default:
		touched = statsTouchedBytes(segment, schema, ids)
	}
	return taskresource.EstimateStats(taskresource.StatsInput{SubJobType: subJob, FieldMemorySize: touched})
}

// statsTouchedBytes sizes a field set, preferring the real per-field binlogs
// and falling back to the schema apportionment on V3.
func statsTouchedBytes(segment *SegmentInfo, schema *schemapb.CollectionSchema, ids map[int64]bool) int64 {
	if size := taskresource.SumFieldBinlogMemoryForFieldSet(segment.GetBinlogs(), ids); size > 0 {
		return size
	}
	return fieldBytesFromSchema(segment, schema, ids)
}
