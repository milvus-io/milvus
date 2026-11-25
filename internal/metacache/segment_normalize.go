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
	"unique"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec/specutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// normalizeSegment shrinks seg before it enters the store. It rewrites only
// seg's top-level fields: nested messages may be shared with other segments,
// so a change to one is made on a copy. A segment normalized before is left
// as is, without writes.
//
// Repeated strings are replaced by copies that have a long-lived owner, so
// that one copy serves every segment: channel names by the matching entry of
// the collection's VChannelNames, stats formats by the constants of
// knownFormats. Strings without an owner are left as is, and null counts are
// pruned only when the collection's schema is known.
func (s *metaStore) normalizeSegment(seg *datapb.SegmentInfo) {
	if seg == nil {
		return
	}
	coll, _ := s.GetCollection(seg.GetCollectionID())
	var prunable map[int64]struct{}
	if len(seg.GetStats().GetNullCounts()) > 0 {
		prunable = s.prunableFieldsOf(seg.GetCollectionID(), coll)
	}
	normalizeWith(seg, vchannelsOf(coll), prunable, true)
}

// vchannelsOf returns coll's VChannelNames, or nil for a nil coll.
func vchannelsOf(coll *CollectionInfo) []string {
	if coll == nil {
		return nil
	}
	return coll.VChannelNames
}

// prunableEntry caches prunableFields(schema) for one collection.
type prunableEntry struct {
	schema *schemapb.CollectionSchema
	fields map[int64]struct{}
}

// prunableNullCountFields returns prunableFieldsOf the collection.
func (s *metaStore) prunableNullCountFields(collectionID int64) map[int64]struct{} {
	coll, _ := s.GetCollection(collectionID)
	return s.prunableFieldsOf(collectionID, coll)
}

// prunableFieldsOf returns prunableFields of coll's schema, or nil when coll
// (the collection collectionID as just looked up) or its schema is nil. The
// result is cached per collection and shared, so callers must not modify it.
// An entry is valid only for the schema it was built from: a schema change
// replaces the CollectionInfo's Schema pointer (it is never modified in
// place), so a lookup that finds another pointer rebuilds the entry. Entries
// go away with their collection: the removal hook deletes the entry, and an
// insert that races with the removal sees the collection gone on its recheck
// and deletes its own entry (sources notify only after a removal is visible).
func (s *metaStore) prunableFieldsOf(collectionID int64, coll *CollectionInfo) map[int64]struct{} {
	if coll == nil || coll.Schema == nil {
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

// normalizeWith makes the changes normalizeSegment describes, with vchannels
// the owners of channel names and dropping the NullCounts keys in prunable.
//
// With copyOnWrite unset it writes seg's nested messages in place instead
// of copying them; seg and all its nested messages must then be owned by the
// caller and visible to no one else, as the protos a catalog decodes are.
// Only then are MsgGroups interned too, with unique.Make: they have no owner,
// and unique keeps its copy only while some holder references it, so after a
// GC a runtime put could find a new copy and clone the position for nothing.
// A catalog load makes the one copy that every segment then keeps.
func normalizeWith(seg *datapb.SegmentInfo, vchannels []string, prunable map[int64]struct{}, copyOnWrite bool) {
	if seg == nil {
		return
	}
	if ch, changed := ownedString(vchannels, seg.InsertChannel); changed {
		seg.InsertChannel = ch
	}
	if p := normalizePosition(seg.StartPosition, vchannels, copyOnWrite); p != seg.StartPosition {
		seg.StartPosition = p
	}
	if p := normalizePosition(seg.DmlPosition, vchannels, copyOnWrite); p != seg.DmlPosition {
		seg.DmlPosition = p
	}
	// An importing segment keeps its null counts: import acceptance compares
	// the stored stats with a replayed worker result, which carries them all.
	// The commit that clears IsImporting puts the segment again and prunes.
	if seg.GetIsImporting() {
		prunable = nil
	}
	if st := normalizeStats(seg.Stats, prunable, copyOnWrite); st != seg.Stats {
		seg.Stats = st
	}
}

// knownFormats owns the stats format names. Statistics.Formats collects
// the FieldBinlog.Format values of a segment (see the stats collector and
// storage.BuildStatsFromFieldBinlogs): dataNode.storage.format ("parquet"
// or "vortex") for the column groups the writers fill, and the external
// source formats of specutil, which mirror the C++ LOON_FORMAT_* names, for
// external collections. Other names are kept as is.
var knownFormats = []string{
	specutil.FormatParquet,
	specutil.FormatVortex,
	specutil.FormatLanceTable,
	specutil.FormatIcebergTable,
	specutil.FormatPaimonTable,
	specutil.FormatMilvusTable,
}

// sameStringData reports whether a and b share backing data. Two empty
// strings count as shared.
func sameStringData(a, b string) bool {
	return len(a) == len(b) && (len(a) == 0 || unsafe.StringData(a) == unsafe.StringData(b))
}

// ownedString returns the entry of owners equal to s, and whether it is a
// different copy than s, so that s needs to be replaced. When no entry is
// equal to s it returns s itself and false.
func ownedString(owners []string, s string) (string, bool) {
	for _, owner := range owners {
		if owner == s {
			return owner, !sameStringData(owner, s)
		}
	}
	return s, false
}

// internString returns the copy unique.Make currently holds for s, and
// whether it differs from s itself, so that s needs to be replaced.
func internString(s string) (string, bool) {
	canonical := unique.Make(s).Value()
	return canonical, !sameStringData(canonical, s)
}

// normalizePosition returns pos with its ChannelName replaced by the owned
// copy in vchannels, and with copyOnWrite unset its MsgGroup interned (see
// normalizeWith). It returns pos itself when nothing changes. Otherwise, with
// copyOnWrite set, it returns a clone, so a position shared by other
// segments is never written; with copyOnWrite unset, it writes pos in place.
func normalizePosition(pos *msgpb.MsgPosition, vchannels []string, copyOnWrite bool) *msgpb.MsgPosition {
	if pos == nil {
		return pos
	}
	ch, chChanged := ownedString(vchannels, pos.ChannelName)
	group, groupChanged := pos.MsgGroup, false
	if !copyOnWrite {
		group, groupChanged = internString(pos.MsgGroup)
	}
	if !chChanged && !groupChanged {
		return pos
	}
	if copyOnWrite {
		pos = proto.Clone(pos).(*msgpb.MsgPosition)
	}
	pos.ChannelName = ch
	pos.MsgGroup = group
	return pos
}

// normalizeStats replaces stats' Formats by the owned copies in knownFormats
// and drops the NullCounts entries of the fields in prunable (see
// prunableFields); a nil prunable drops nothing. It returns stats itself when
// there is nothing to change. Otherwise, with copyOnWrite set, it returns one
// clone with both changes, so a stats shared by other segments is never
// written; with copyOnWrite unset, it makes the changes on stats in place. An
// emptied map becomes nil.
func normalizeStats(stats *datapb.Statistics, prunable map[int64]struct{}, copyOnWrite bool) *datapb.Statistics {
	if stats == nil {
		return nil
	}
	var kept map[int64]int64
	pruned := false
	if len(prunable) > 0 {
		kept, pruned = keptNullCounts(stats.NullCounts, prunable)
	}
	// Formats before firstFormat are owned already.
	firstFormat := len(stats.Formats)
	var firstOwned string
	for i, format := range stats.Formats {
		if owned, changed := ownedString(knownFormats, format); changed {
			firstFormat, firstOwned = i, owned
			break
		}
	}
	if !pruned && firstFormat == len(stats.Formats) {
		return stats
	}

	if copyOnWrite {
		stats = proto.Clone(stats).(*datapb.Statistics)
	}
	if pruned {
		stats.NullCounts = kept
	}
	if firstFormat < len(stats.Formats) {
		stats.Formats[firstFormat] = firstOwned
		for i := firstFormat + 1; i < len(stats.Formats); i++ {
			if owned, changed := ownedString(knownFormats, stats.Formats[i]); changed {
				stats.Formats[i] = owned
			}
		}
	}
	return stats
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
