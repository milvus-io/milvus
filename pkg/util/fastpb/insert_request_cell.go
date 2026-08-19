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

package fastpb

import (
	"math"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// An ArrayArray element is a whole schemapb.ScalarField rather than a row
// slice, so the row-selection encoders in insert_request_view.go do not apply
// to it. Before this file it was the only insert payload still measured and
// written by the protobuf reflection encoder. Here it is measured and written
// arithmetically instead.
//
// The split is deliberate:
//
//	classifyScalarCell -- O(1), looks at the oneof only
//	scalarCellPayload  -- O(elements), pure arithmetic, no reflection
//	appendScalarCell   -- the bytes, byte-identical to proto.Marshal
//
// Sizing runs classify+payload and stores the payload in cursor scratch;
// MarshalTo runs classify again and replays the stored payload, so the
// O(elements) pass happens exactly once per cell.
//
// Note this is exact, not an estimate. A cheap over-approximation was
// considered and rejected: bounding a packed int64 array at 10 bytes per
// element is O(1) but roughly 10x the real size for small values, which would
// cut the rows per message by the same factor. SizeVarint is a bit-length
// operation -- computing the true size costs little more than bounding it, and
// keeps the splitter's limit exact so no headroom is needed to absorb sizing
// error.
//
// Cells the fast path does not cover -- an unknown oneof, a nested ArrayData,
// or any cell carrying unknown fields -- report ok=false from classify and go
// back through the protobuf runtime for both sizing and writing. Dropping
// unknown fields on only one of those sides would make the length prefix disagree
// with the bytes written, which corrupts the message rather than merely losing
// data.

type scalarCellKind uint8

const (
	// scalarCellUnsupported marks a cell the arithmetic path must not encode.
	scalarCellUnsupported scalarCellKind = iota
	// scalarCellEmpty is a ScalarField with no oneof set.
	scalarCellEmpty
	// scalarCellPacked covers repeated numeric arrays, which proto3 encodes as
	// one packed length-delimited payload under field 1 of the array message.
	// An empty array omits field 1 entirely.
	scalarCellPacked
	// scalarCellRepeated covers repeated string/bytes arrays, where every
	// element carries its own tag and length prefix under field 1.
	scalarCellRepeated
)

// scalarCellPlan is a cell's classification plus the one size that cannot be
// derived from its type alone.
type scalarCellPlan struct {
	kind scalarCellKind
	// oneofNumber is the ScalarField field number holding the array message.
	oneofNumber protowire.Number
	// payload is the packed byte count for scalarCellPacked, or the summed
	// element wire size for scalarCellRepeated.
	payload int
}

// classifyScalarCell inspects only the oneof and reports whether the
// arithmetic path can reproduce this cell. Both the sizing pass and MarshalTo
// call it and must agree on every cell, so it derives its verdict from the
// cell alone -- the borrowed source is immutable for the cursor's lifetime.
func classifyScalarCell(cell *schemapb.ScalarField) (scalarCellPlan, bool) {
	if cell == nil {
		return scalarCellPlan{kind: scalarCellEmpty}, true
	}
	// ValidData (field 17) has no producer anywhere in this repo yet, but proto
	// reflection recognizes it now, so GetUnknown() no longer catches it: a cell
	// carrying it would silently lose that data through the arithmetic path,
	// which only writes the oneof value. Fall back to the protobuf path, which
	// writes every recognized field, until this one earns explicit handling.
	if len(cell.GetValidData()) != 0 {
		return scalarCellPlan{}, false
	}
	if len(cell.ProtoReflect().GetUnknown()) != 0 {
		return scalarCellPlan{}, false
	}

	switch value := cell.Data.(type) {
	case nil:
		return scalarCellPlan{kind: scalarCellEmpty}, true
	case *schemapb.ScalarField_BoolData:
		return packedCellPlan(1, value.BoolData)
	case *schemapb.ScalarField_IntData:
		return packedCellPlan(2, value.IntData)
	case *schemapb.ScalarField_LongData:
		return packedCellPlan(3, value.LongData)
	case *schemapb.ScalarField_FloatData:
		return packedCellPlan(4, value.FloatData)
	case *schemapb.ScalarField_DoubleData:
		return packedCellPlan(5, value.DoubleData)
	case *schemapb.ScalarField_StringData:
		return repeatedCellPlan(6, value.StringData)
	case *schemapb.ScalarField_BytesData:
		return repeatedCellPlan(7, value.BytesData)
	case *schemapb.ScalarField_JsonData:
		return repeatedCellPlan(9, value.JsonData)
	case *schemapb.ScalarField_GeometryData:
		return repeatedCellPlan(10, value.GeometryData)
	case *schemapb.ScalarField_TimestamptzData:
		return packedCellPlan(11, value.TimestamptzData)
	case *schemapb.ScalarField_GeometryWktData:
		return repeatedCellPlan(12, value.GeometryWktData)
	case *schemapb.ScalarField_MolData:
		return repeatedCellPlan(13, value.MolData)
	case *schemapb.ScalarField_MolSmilesData:
		return repeatedCellPlan(14, value.MolSmilesData)
	case *schemapb.ScalarField_DateData:
		return packedCellPlan(15, value.DateData)
	case *schemapb.ScalarField_TimeData:
		return packedCellPlan(16, value.TimeData)
	default:
		// A nested ArrayData, or an oneof added after this code was written.
		return scalarCellPlan{}, false
	}
}

// packedCellPlan rejects a set-but-nil oneof wrapper. proto.Marshal still emits
// the empty array message for one, and the arithmetic path agrees, but the
// row-selection encoders treat it as a malformed source, so keep the two paths
// consistent by refusing it here too.
func packedCellPlan(oneofNumber protowire.Number, array proto.Message) (scalarCellPlan, bool) {
	if !scalarCellNestedArraySupported(array) {
		return scalarCellPlan{}, false
	}
	return scalarCellPlan{kind: scalarCellPacked, oneofNumber: oneofNumber}, true
}

func repeatedCellPlan(oneofNumber protowire.Number, array proto.Message) (scalarCellPlan, bool) {
	if !scalarCellNestedArraySupported(array) {
		return scalarCellPlan{}, false
	}
	return scalarCellPlan{kind: scalarCellRepeated, oneofNumber: oneofNumber}, true
}

// scalarCellNestedArraySupported rejects both malformed set-but-nil oneofs and
// nested messages carrying fields this arithmetic encoder does not know about.
// The protobuf fallback preserves those unknown bytes verbatim.
func scalarCellNestedArraySupported(array proto.Message) bool {
	return !isNilProto(array) && len(array.ProtoReflect().GetUnknown()) == 0
}

// scalarCellPayload computes the cell's inner payload size arithmetically. The
// caller must have classified the cell first; an unsupported cell returns 0.
func scalarCellPayload(cell *schemapb.ScalarField, plan scalarCellPlan) int {
	if plan.kind != scalarCellPacked && plan.kind != scalarCellRepeated {
		return 0
	}
	switch value := cell.Data.(type) {
	case *schemapb.ScalarField_BoolData:
		// Every bool is a single-byte varint.
		return len(value.BoolData.GetData())
	case *schemapb.ScalarField_IntData:
		return int32VarintPayload(value.IntData.GetData())
	case *schemapb.ScalarField_LongData:
		return int64VarintPayload(value.LongData.GetData())
	case *schemapb.ScalarField_FloatData:
		return 4 * len(value.FloatData.GetData())
	case *schemapb.ScalarField_DoubleData:
		return 8 * len(value.DoubleData.GetData())
	case *schemapb.ScalarField_StringData:
		return stringElementsPayload(value.StringData.GetData())
	case *schemapb.ScalarField_BytesData:
		return bytesElementsPayload(value.BytesData.GetData())
	case *schemapb.ScalarField_JsonData:
		return bytesElementsPayload(value.JsonData.GetData())
	case *schemapb.ScalarField_GeometryData:
		return bytesElementsPayload(value.GeometryData.GetData())
	case *schemapb.ScalarField_TimestamptzData:
		return int64VarintPayload(value.TimestamptzData.GetData())
	case *schemapb.ScalarField_GeometryWktData:
		return stringElementsPayload(value.GeometryWktData.GetData())
	case *schemapb.ScalarField_MolData:
		return bytesElementsPayload(value.MolData.GetData())
	case *schemapb.ScalarField_MolSmilesData:
		return stringElementsPayload(value.MolSmilesData.GetData())
	case *schemapb.ScalarField_DateData:
		return int32VarintPayload(value.DateData.GetData())
	case *schemapb.ScalarField_TimeData:
		return int64VarintPayload(value.TimeData.GetData())
	default:
		return 0
	}
}

// arrayMessageSize is the wire size of the array message itself (LongArray,
// StringArray, ...) sitting inside the ScalarField oneof.
func (p scalarCellPlan) arrayMessageSize() int {
	switch p.kind {
	case scalarCellPacked:
		if p.payload == 0 {
			// proto3 omits an empty packed field, leaving an empty message.
			return 0
		}
		return protowire.SizeTag(1) + protowire.SizeBytes(p.payload)
	case scalarCellRepeated:
		return p.payload
	default:
		return 0
	}
}

// scalarCellSize is the cell's full wire size, i.e. what proto.Size returns for
// the same ScalarField.
func (p scalarCellPlan) scalarCellSize() int {
	if p.kind == scalarCellEmpty || p.kind == scalarCellUnsupported {
		return 0
	}
	return protowire.SizeTag(p.oneofNumber) + protowire.SizeBytes(p.arrayMessageSize())
}

// appendScalarCell writes the ScalarField body -- without the caller's own tag
// and length prefix -- reproducing proto.Marshal byte for byte. plan must carry
// the payload measured from this same, unmodified cell.
func appendScalarCell(w *insertViewWriter, cell *schemapb.ScalarField, plan scalarCellPlan) error {
	switch plan.kind {
	case scalarCellEmpty:
		return nil
	case scalarCellPacked, scalarCellRepeated:
	default:
		return insertViewInternal("array cell is not encodable by the arithmetic path")
	}

	return w.message(plan.oneofNumber, plan.arrayMessageSize(), func() error {
		switch value := cell.Data.(type) {
		case *schemapb.ScalarField_BoolData:
			return appendPackedCell(w, plan, func(out []byte) []byte {
				for _, item := range value.BoolData.GetData() {
					if item {
						out = append(out, 1)
					} else {
						out = append(out, 0)
					}
				}
				return out
			})
		case *schemapb.ScalarField_IntData:
			return appendPackedInt32Cell(w, plan, value.IntData.GetData())
		case *schemapb.ScalarField_LongData:
			return appendPackedInt64Cell(w, plan, value.LongData.GetData())
		case *schemapb.ScalarField_FloatData:
			return appendPackedCell(w, plan, func(out []byte) []byte {
				for _, item := range value.FloatData.GetData() {
					out = protowire.AppendFixed32(out, math.Float32bits(item))
				}
				return out
			})
		case *schemapb.ScalarField_DoubleData:
			return appendPackedCell(w, plan, func(out []byte) []byte {
				for _, item := range value.DoubleData.GetData() {
					out = protowire.AppendFixed64(out, math.Float64bits(item))
				}
				return out
			})
		case *schemapb.ScalarField_StringData:
			return appendStringCell(w, value.StringData.GetData())
		case *schemapb.ScalarField_BytesData:
			return appendBytesCell(w, value.BytesData.GetData())
		case *schemapb.ScalarField_JsonData:
			return appendBytesCell(w, value.JsonData.GetData())
		case *schemapb.ScalarField_GeometryData:
			return appendBytesCell(w, value.GeometryData.GetData())
		case *schemapb.ScalarField_TimestamptzData:
			return appendPackedInt64Cell(w, plan, value.TimestamptzData.GetData())
		case *schemapb.ScalarField_GeometryWktData:
			return appendStringCell(w, value.GeometryWktData.GetData())
		case *schemapb.ScalarField_MolData:
			return appendBytesCell(w, value.MolData.GetData())
		case *schemapb.ScalarField_MolSmilesData:
			return appendStringCell(w, value.MolSmilesData.GetData())
		case *schemapb.ScalarField_DateData:
			return appendPackedInt32Cell(w, plan, value.DateData.GetData())
		case *schemapb.ScalarField_TimeData:
			return appendPackedInt64Cell(w, plan, value.TimeData.GetData())
		default:
			return insertViewInternal("array cell oneof %T changed after planning", value)
		}
	})
}

// appendPackedCell writes the packed payload under field 1. appendValues is
// charged once against the payload size measured during sizing, so it never
// recomputes a per-value size the way w.varint would.
func appendPackedCell(w *insertViewWriter, plan scalarCellPlan, appendValues func([]byte) []byte) error {
	if plan.payload == 0 {
		return nil
	}
	return w.message(1, plan.payload, func() error {
		w.appendBulk(plan.payload, appendValues)
		return w.err
	})
}

func appendPackedInt32Cell(w *insertViewWriter, plan scalarCellPlan, values []int32) error {
	return appendPackedCell(w, plan, func(out []byte) []byte {
		for _, item := range values {
			out = protowire.AppendVarint(out, uint64(item))
		}
		return out
	})
}

func appendPackedInt64Cell(w *insertViewWriter, plan scalarCellPlan, values []int64) error {
	return appendPackedCell(w, plan, func(out []byte) []byte {
		for _, item := range values {
			out = protowire.AppendVarint(out, uint64(item))
		}
		return out
	})
}

// appendStringCell does not rescan UTF-8, matching how the row-selection
// encoders treat top-level proto3 strings: trusted internal input. proto.Marshal,
// which used to write these cells, did validate and fail on invalid UTF-8, so an
// array cell is now accepted where it previously errored -- the same treatment a
// top-level varchar already got. Validation belongs at the ingest boundary, not
// in the encoder.
func appendStringCell(w *insertViewWriter, values []string) error {
	for _, item := range values {
		w.stringBytes(1, item)
	}
	return w.err
}

func appendBytesCell(w *insertViewWriter, values [][]byte) error {
	for _, item := range values {
		w.bytes(1, item)
	}
	return w.err
}

const scalarCellProtoFallbackPayload = -1

// scalarCellWirePlan is the sizing side's entry point. It returns the exact
// wire size of one ArrayArray element plus the payload token MarshalTo replays.
// Non-negative tokens are arithmetic payload sizes; -1 selects protobuf's
// cached-size fallback so unknown fields remain byte-preserving.
func scalarCellWirePlan(cell *schemapb.ScalarField) (int, int) {
	plan, ok := classifyScalarCell(cell)
	if !ok {
		return nullableProtoSize(cell, false), scalarCellProtoFallbackPayload
	}
	plan.payload = scalarCellPayload(cell, plan)
	return plan.scalarCellSize(), plan.payload
}

func int32VarintPayload(values []int32) int {
	payload := 0
	for _, item := range values {
		payload += protowire.SizeVarint(uint64(item))
	}
	return payload
}

func int64VarintPayload(values []int64) int {
	payload := 0
	for _, item := range values {
		payload += protowire.SizeVarint(uint64(item))
	}
	return payload
}

func stringElementsPayload(values []string) int {
	payload := 0
	for _, item := range values {
		payload += protowire.SizeTag(1) + protowire.SizeBytes(len(item))
	}
	return payload
}

func bytesElementsPayload(values [][]byte) int {
	payload := 0
	for _, item := range values {
		payload += protowire.SizeTag(1) + protowire.SizeBytes(len(item))
	}
	return payload
}
