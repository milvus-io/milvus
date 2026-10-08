package fastpb

import (
	"testing"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// withUnknownField appends an unknown field (number 15, varint 42) to a wire blob.
func withUnknownField(b []byte) []byte {
	return append(append([]byte{}, b...), 0x78, 0x2A) // tag (15<<3|0)=0x78, value 42
}

// TestLeafUnknownFieldPreserved guards the fix: leaf *Array messages carrying an
// unknown/future field must decode identically to the official codec (the field
// is preserved, not dropped). Previously these helpers silently skipped it.
func TestLeafUnknownFieldPreserved(t *testing.T) {
	cases := []struct {
		name   string
		base   proto.Message
		fresh  func() proto.Message
		decode func(b []byte, m proto.Message) error
	}{
		{
			"StringArray", &schemapb.StringArray{Data: []string{"a", "b"}},
			func() proto.Message { return &schemapb.StringArray{} },
			func(b []byte, m proto.Message) error { return dec{}.stringArray(b, m.(*schemapb.StringArray)) },
		},
		{
			"LongArray", &schemapb.LongArray{Data: []int64{1, 2, 3}},
			func() proto.Message { return &schemapb.LongArray{} },
			func(b []byte, m proto.Message) error {
				a := m.(*schemapb.LongArray)
				return decodePackedI64(b, &a.Data, a)
			},
		},
		{
			"IntArray", &schemapb.IntArray{Data: []int32{1, 2, 3}},
			func() proto.Message { return &schemapb.IntArray{} },
			func(b []byte, m proto.Message) error {
				a := m.(*schemapb.IntArray)
				return decodePackedI32(b, &a.Data, a)
			},
		},
		{
			"BoolArray", &schemapb.BoolArray{Data: []bool{true, false}},
			func() proto.Message { return &schemapb.BoolArray{} },
			func(b []byte, m proto.Message) error {
				a := m.(*schemapb.BoolArray)
				return decodePackedBool(b, &a.Data, a)
			},
		},
		{
			"FloatArray", &schemapb.FloatArray{Data: []float32{1, 2, 3}},
			func() proto.Message { return &schemapb.FloatArray{} },
			func(b []byte, m proto.Message) error {
				a := m.(*schemapb.FloatArray)
				return decodePackedF32(b, &a.Data, a)
			},
		},
		{
			"DoubleArray", &schemapb.DoubleArray{Data: []float64{1, 2, 3}},
			func() proto.Message { return &schemapb.DoubleArray{} },
			func(b []byte, m proto.Message) error {
				a := m.(*schemapb.DoubleArray)
				return decodePackedF64(b, &a.Data, a)
			},
		},
		{
			"BytesArray", &schemapb.BytesArray{Data: [][]byte{{1, 2}, {3}}},
			func() proto.Message { return &schemapb.BytesArray{} },
			func(b []byte, m proto.Message) error {
				a := m.(*schemapb.BytesArray)
				return decodeRepeatedBytes(b, &a.Data, a)
			},
		},
		{
			"SparseFloatArray", &schemapb.SparseFloatArray{Dim: 9, Contents: [][]byte{{1}, {2}}},
			func() proto.Message { return &schemapb.SparseFloatArray{} },
			func(b []byte, m proto.Message) error {
				return unmarshalSparseFloatArray(b, m.(*schemapb.SparseFloatArray))
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			base, err := proto.Marshal(c.base)
			if err != nil {
				t.Fatal(err)
			}
			wire := withUnknownField(base)

			official := c.fresh()
			if err := proto.Unmarshal(wire, official); err != nil {
				t.Fatalf("official: %v", err)
			}
			got := c.fresh()
			if err := c.decode(wire, got); err != nil {
				t.Fatalf("fastpb: %v", err)
			}
			if !proto.Equal(official, got) {
				t.Fatalf("unknown field NOT preserved:\n official=%v\n got=%v", official, got)
			}
		})
	}
}

// TestUnmarshalResetsTarget guards the fix: the public entry points must clear a
// pre-populated target (matching official proto.Unmarshal semantics) so stale
// fields cannot leak when the codec reuses a message.
func TestUnmarshalResetsTarget(t *testing.T) {
	src := &schemapb.SearchResultData{TopK: 5, NumQueries: 2}
	wire, err := proto.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	// target pre-populated with stale data that is NOT present in src
	got := &schemapb.SearchResultData{
		PrimaryFieldName: "stale",
		Scores:           []float32{9, 9, 9},
		AllSearchCount:   123,
	}
	if err := UnmarshalSearchResultData(wire, got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(src, got) {
		t.Fatalf("stale fields not cleared before decode:\n want=%v\n got=%v", src, got)
	}
}
