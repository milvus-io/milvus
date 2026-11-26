// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package compactor

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A source is read to EOF. Its TEXT payload can be inherited only when all
// those rows actually reached this output, and the field was read from storage
// (rather than filled by the reader or materializer).
type textSizeSource struct {
	sizes   *datapb.FieldLogicalSizes
	present map[int64]struct{}
	read    int64
}

func (s *textSizeSource) wholePayload(fieldID, routed int64) (uint64, bool) {
	_, present := s.present[fieldID]
	if !present || routed != s.read || !storage.FieldLogicalSizesCover(s.sizes, s.read) {
		return 0, false
	}
	value := s.sizes.GetFields()[fieldID]
	return value.GetPayloadBytes(), value != nil
}

// textSizeLineage only tracks REUSE fields with a potentially inheritable
// source snapshot. REWRITE payloads always come from the writer itself.
type textSizeLineage struct {
	fields  []*schemapb.FieldSchema
	sources []textSizeSource // indexed by the input reader, not the segment ID
	outputs map[int64]map[int]*textSizePortion
}

type textSizePortion struct {
	rows int64
	// Per field: bytes of this source's rows in this output, or unknown if
	// any of those rows needs an external LOB. Never add a partial subtotal
	// to a whole-source total that already includes it.
	measured []textPayload
}

type textPayload struct {
	bytes   uint64
	unknown bool
}

func newTextSizeLineage(schema *schemapb.CollectionSchema, lob *compaction.LOBCompactionContext, inputs []*datapb.CompactionSegmentBinlogs) *textSizeLineage {
	var fields []*schemapb.FieldSchema
	for _, id := range lob.GetReuseAllFieldIDs() {
		field := typeutil.GetField(schema, id)
		// A builder may replace a NULL with a declared default. Do not
		// claim the source values survived such a projection unchanged.
		if field.GetDataType() != schemapb.DataType_Text || field.GetDefaultValue() != nil {
			continue
		}
		for _, input := range inputs {
			s := input.GetFieldLogicalSizes()
			if s.GetVersion() == storage.FieldLogicalSizeVersion && s.GetFields()[id] != nil {
				fields = append(fields, field)
				break
			}
		}
	}
	if len(fields) == 0 {
		return nil
	}
	l := &textSizeLineage{fields: fields, sources: make([]textSizeSource, len(inputs)), outputs: make(map[int64]map[int]*textSizePortion)}
	for i, input := range inputs {
		l.sources[i].sizes = input.GetFieldLogicalSizes()
	}
	return l
}

// recordRange is called only after a successful write, using the ID assigned
// by rotation for that batch. Rows within each range have the same source.
func (l *textSizeLineage) recordRange(outputID int64, source int, r storage.Record, start, end int) {
	if end == start {
		return
	}
	parts := l.outputs[outputID]
	if parts == nil {
		parts = make(map[int]*textSizePortion)
		l.outputs[outputID] = parts
	}
	part := parts[source]
	if part == nil {
		part = &textSizePortion{}
		if len(l.sources) > 1 {
			part.measured = make([]textPayload, len(l.fields))
		}
		parts[source] = part
	}
	part.rows += int64(end - start)
	// With a single source the final writer has already measured every
	// inline-only field. No second inspection of those rows is needed.
	for i := range part.measured {
		m := &part.measured[i]
		if !m.unknown {
			n, ok := storage.TextLogicalPayloadBytes(r.Column(l.fields[i].GetFieldID()), start, end)
			m.unknown = !ok || n > math.MaxUint64-m.bytes
			if !m.unknown {
				m.bytes += n
			}
		}
	}
}

func (l *textSizeLineage) finish(segments []*datapb.CompactionSegment) {
	if l != nil {
		for _, segment := range segments {
			l.fill(segment, l.outputs[segment.GetSegmentID()])
		}
	}
}

func (l *textSizeLineage) fill(output *datapb.CompactionSegment, parts map[int]*textSizePortion) {
	sizes := output.GetStats().GetFieldLogicalSizes()
	var routed int64
	for _, part := range parts {
		routed += part.rows
	}
	if routed == 0 || routed != output.GetNumOfRows() || !storage.FieldLogicalSizesCover(sizes, routed) {
		return
	}
fields:
	for i, field := range l.fields {
		id := field.GetFieldID()
		if sizes.GetFields()[id] != nil { // the writer measured the entire column directly
			continue
		}
		var total uint64
		for index, part := range parts {
			var n uint64
			if len(part.measured) != 0 && !part.measured[i].unknown {
				n = part.measured[i].bytes
			} else {
				var ok bool
				n, ok = l.sources[index].wholePayload(id, part.rows)
				if !ok {
					continue fields
				}
			}
			if n > math.MaxUint64-total {
				continue fields
			}
			total += n
		}
		if sizes.Fields == nil {
			sizes.Fields = make(map[int64]*datapb.FieldLogicalSize)
		}
		size := &datapb.FieldLogicalSize{PayloadBytes: total}
		if field.GetNullable() {
			size.ValidityBits = uint64(routed)
		}
		sizes.Fields[id] = size
	}
}

// Sort and full schema rewrite have exactly one source and one real output.
// read is the number of rows visited by the scan (including filtered rows),
// not the old segment's advertised row count.
func inheritSingleSourceText(output *datapb.CompactionSegment, input *datapb.CompactionSegmentBinlogs, read int64, present map[int64]struct{}, schema *schemapb.CollectionSchema, lob *compaction.LOBCompactionContext) {
	if read != output.GetNumOfRows() {
		return
	}
	l := newTextSizeLineage(schema, lob, []*datapb.CompactionSegmentBinlogs{input})
	if l != nil {
		l.sources[0].read, l.sources[0].present = read, present
		l.fill(output, map[int]*textSizePortion{0: {rows: read}})
	}
}

// Only passed to MergeSort when a REUSE source snapshot is available. Keep
// source indices for the pending batch only; neither the schema nor the TEXT
// references are changed to carry lineage through the merge.
type mergeTextSizeWriter struct {
	*MultiSegmentWriter
	lineage *textSizeLineage
	batch   []int32
}

func (w *mergeTextSizeWriter) RecordSourceRow(readerIndex int) {
	w.batch = append(w.batch, int32(readerIndex))
}

func (w *mergeTextSizeWriter) Write(r storage.Record) error {
	if err := w.MultiSegmentWriter.Write(r); err != nil {
		return err
	}
	if len(w.batch) == r.Len() {
		for start := 0; start < len(w.batch); {
			end := start + 1
			for end < len(w.batch) && w.batch[end] == w.batch[start] {
				end++
			}
			w.lineage.recordRange(w.currentSegmentID, int(w.batch[start]), r, start, end)
			start = end
		}
	}
	w.batch = w.batch[:0]
	return nil
}
