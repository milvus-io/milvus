package fastpb

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	msgpb "github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// fieldNumbers returns the sorted set of field numbers declared on m's
// descriptor, INCLUDING oneof members and proto3-optional fields (the
// descriptor enumerates them, unlike struct-tag scraping).
func fieldNumbers(m proto.Message) []int {
	fds := m.ProtoReflect().Descriptor().Fields()
	out := make([]int, 0, fds.Len())
	for i := 0; i < fds.Len(); i++ {
		out = append(out, int(fds.Get(i).Number()))
	}
	sort.Ints(out)
	return out
}

// TestProtoContract_FieldSetsPinned is a tripwire. fastpb hand-writes wire
// codecs for the message types below; each codec switches on hard-coded
// field numbers. The golden field set for every such type is pinned here, so
// ANY edit to these protos (a new field, a removed field, a renumber) flips
// this test red and blocks CI.
//
// When it fails, do NOT just bump the golden set. First go read the matching
// hand-written codec in pkg/util/fastpb and decide what the proto change requires:
//
//   - A new plain (non-oneof) scalar/message field may be correct-by-construction
//     for a decoder that folds unknown fields through `rest -> protoMerge`.
//     Encoders do not have that fallback: decide whether the field belongs to
//     output metadata, selected row data, or must be rejected explicitly.
//
//   - A NEW oneof VARIANT is the dangerous case. It also falls through to the
//     deferred protoMerge, but that breaks oneof last-wins ordering relative to
//     the variants decoded in-pass (see the StructArrays/case-8 comment in
//     fielddata.go). New oneof variants on FieldData / ScalarField / VectorField
//     / IDs MUST be handled in-pass, not left to the fallback.
//
//   - Changing the wire type or meaning of an EXISTING hard-coded number
//     silently corrupts the fast path — the hard-coded case still fires.
//
// Only after every affected codec is correct should you update the golden set.
func TestProtoContract_FieldSetsPinned(t *testing.T) {
	cases := []struct {
		name string
		msg  proto.Message
		want []int
	}{
		// top-level fast-pathed types (TryUnmarshal dispatch)
		{"internalpb.RetrieveResults", &internalpb.RetrieveResults{}, []int{1, 2, 3, 4, 5, 6, 7, 8, 13, 14, 15, 16, 17, 18, 19}},
		{"milvuspb.InsertRequest", &milvuspb.InsertRequest{}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		{"milvuspb.UpsertRequest", &milvuspb.UpsertRequest{}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}},
		{"msgpb.InsertRequest", &msgpb.InsertRequest{}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
		{"schemapb.SearchResultData", &schemapb.SearchResultData{}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19}},
		// nested hand-decoded types (oneof-bearing — highest divergence risk)
		{"schemapb.FieldData", &schemapb.FieldData{}, []int{1, 2, 3, 4, 5, 6, 7, 8}},
		{"schemapb.ScalarField", &schemapb.ScalarField{}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}},
		{"schemapb.VectorField", &schemapb.VectorField{}, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"schemapb.IDs", &schemapb.IDs{}, []int{1, 2, 3}},
		// leaf arrays with their own hand-written field switches (no unknown-field
		// fallback in some — a new field here would mis-parse)
		{"schemapb.SparseFloatArray", &schemapb.SparseFloatArray{}, []int{1, 2}},
		{"schemapb.FloatArray", &schemapb.FloatArray{}, []int{1}},
		{"schemapb.LongArray", &schemapb.LongArray{}, []int{1}},
		{"schemapb.IntArray", &schemapb.IntArray{}, []int{1}},
		{"schemapb.BoolArray", &schemapb.BoolArray{}, []int{1}},
		{"schemapb.DoubleArray", &schemapb.DoubleArray{}, []int{1}},
		{"schemapb.BytesArray", &schemapb.BytesArray{}, []int{1}},
		{"schemapb.ArrayArray", &schemapb.ArrayArray{}, []int{1, 2}},
		{"schemapb.JSONArray", &schemapb.JSONArray{}, []int{1}},
		{"schemapb.UUIDArray", &schemapb.UUIDArray{}, []int{1}},
		{"schemapb.GeometryArray", &schemapb.GeometryArray{}, []int{1}},
		{"schemapb.TimestamptzArray", &schemapb.TimestamptzArray{}, []int{1}},
		{"schemapb.GeometryWktArray", &schemapb.GeometryWktArray{}, []int{1}},
		{"schemapb.MolArray", &schemapb.MolArray{}, []int{1}},
		{"schemapb.MolSmilesArray", &schemapb.MolSmilesArray{}, []int{1}},
		{"schemapb.DateArray", &schemapb.DateArray{}, []int{1}},
		{"schemapb.TimeArray", &schemapb.TimeArray{}, []int{1}},
		{"schemapb.StringArray", &schemapb.StringArray{}, []int{1}},
		{"schemapb.VectorArray", &schemapb.VectorArray{}, []int{1, 2, 3}},
	}
	for _, c := range cases {
		got := fieldNumbers(c.msg)
		assert.Equalf(t, c.want, got,
			"%s proto field set changed (want %v, got %v).\n"+
				"This type has a hand-written codec in pkg/util/fastpb. Read the codec and the\n"+
				"guidance on TestProtoContract_FieldSetsPinned BEFORE updating this golden set —\n"+
				"a new oneof variant in particular requires explicit handling.",
			c.name, c.want, got)
	}
}
