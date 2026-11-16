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

package metacache

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// normalizeSegment shrinks seg before it enters the store. It rewrites only
// seg's top-level fields: nested messages may be shared with other segments,
// so a change to one is made on a copy.
func (s *metaStore) normalizeSegment(seg *datapb.SegmentInfo) {
	// An importing segment keeps its null counts: import acceptance compares
	// the stored stats with a replayed worker result, which carries them all.
	// The commit that clears IsImporting puts the segment again and prunes.
	if seg == nil || seg.GetIsImporting() || len(seg.GetStats().GetNullCounts()) == 0 {
		return
	}
	if p := pruneNullCounts(seg.Stats, s.prunableNullCountFields(seg.GetCollectionID())); p != seg.Stats {
		seg.Stats = p
	}
}

// pruneNullCounts drops the NullCounts entries of the fields in prunable
// (see prunableFields); a nil prunable drops nothing. It returns stats itself
// when there is nothing to drop; otherwise a clone without those keys, so a
// stats shared by other segments is never written. An emptied map becomes
// nil.
func pruneNullCounts(stats *datapb.Statistics, prunable map[int64]struct{}) *datapb.Statistics {
	if len(stats.GetNullCounts()) == 0 || len(prunable) == 0 {
		return stats
	}
	kept, changed := keptNullCounts(stats.NullCounts, prunable)
	if !changed {
		return stats
	}
	pruned := proto.Clone(stats).(*datapb.Statistics)
	pruned.NullCounts = kept
	return pruned
}

// prunableEntry caches prunableFields(schema) for one collection.
type prunableEntry struct {
	schema *schemapb.CollectionSchema
	fields map[int64]struct{}
}

// prunableNullCountFields returns prunableFields of the collection's schema,
// or nil when the collection or its schema is unknown. The result is cached
// per collection and shared, so callers must not modify it. An entry is valid
// only for the schema it was built from: a schema change replaces the
// CollectionInfo's Schema pointer (it is never modified in place), so a
// lookup that finds another pointer rebuilds the entry. Entries go away with
// their collection: the removal hook deletes the entry, and an insert that
// races with the removal sees the collection gone on its recheck and deletes
// its own entry (sources notify only after a removal is visible).
func (s *metaStore) prunableNullCountFields(collectionID int64) map[int64]struct{} {
	coll, ok := s.GetCollection(collectionID)
	if !ok || coll.Schema == nil {
		return nil
	}
	if e, ok := s.prunable.Get(collectionID); ok && e.schema == coll.Schema {
		return e.fields
	}
	fields := prunableFields(coll.Schema)
	s.prunable.Insert(collectionID, &prunableEntry{schema: coll.Schema, fields: fields})
	if _, ok := s.GetCollection(collectionID); !ok {
		s.prunable.Remove(collectionID)
	}
	return fields
}

// prunableFields returns the IDs of the fields of schema (struct sub-fields
// included) whose NullCounts entries the coord need not keep: not-nullable
// fields other than VectorArray ones. Such a field never has nulls, so its
// entry is always zero; a VectorArray field keeps its entry because the
// DataNode's valid-element accounting reads the key's presence once it
// reseeds from these stats. Keys of fields schema does not list are kept. A
// nil schema gives nil, which prunes nothing.
func prunableFields(schema *schemapb.CollectionSchema) map[int64]struct{} {
	if schema == nil {
		return nil
	}
	prunable := make(map[int64]struct{})
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if !field.GetNullable() && !typeutil.IsVectorArrayType(field.GetDataType()) {
			prunable[field.GetFieldID()] = struct{}{}
		}
	}
	return prunable
}

// keptNullCounts returns nullCounts without the keys in prunable, and
// whether any key was dropped. When none is, it returns nullCounts itself.
// Otherwise it returns a new map sized to the kept entries, or nil when none
// remain: Go maps never shrink on delete, so deleting from nullCounts would
// keep its full footprint whenever one key survives. nullCounts is never
// written.
func keptNullCounts(nullCounts map[int64]int64, prunable map[int64]struct{}) (map[int64]int64, bool) {
	dropped := 0
	for fieldID := range nullCounts {
		if _, ok := prunable[fieldID]; ok {
			dropped++
		}
	}
	if dropped == 0 {
		return nullCounts, false
	}
	if dropped == len(nullCounts) {
		return nil, true
	}
	out := make(map[int64]int64, len(nullCounts)-dropped)
	for fieldID, count := range nullCounts {
		if _, ok := prunable[fieldID]; !ok {
			out[fieldID] = count
		}
	}
	return out, true
}
