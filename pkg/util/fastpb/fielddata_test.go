// Package fastpb hand-writes unmarshal for the hot schemapb types
// (FieldData / SearchResultData). Correctness is pinned by differential
// testing against the official proto.Unmarshal: for any input, our decode
// must produce a result that proto.Equal-s the official one.
package fastpb

import (
	"math"
	"testing"

	"google.golang.org/protobuf/proto"

	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// roundTrip marshals src with the official codec, decodes with our fast path,
// and fails unless the result is proto.Equal to src.
func roundTripFieldData(t *testing.T, src *schemapb.FieldData) {
	t.Helper()
	wire, err := proto.Marshal(src)
	if err != nil {
		t.Fatalf("official marshal: %v", err)
	}
	var got schemapb.FieldData
	if err := UnmarshalFieldData(wire, &got); err != nil {
		t.Fatalf("UnmarshalFieldData: %v", err)
	}
	if !proto.Equal(src, &got) {
		t.Fatalf("mismatch:\n src = %v\n got = %v", src, &got)
	}
}

func TestUnmarshalFieldData_ScalarHeaderFields(t *testing.T) {
	roundTripFieldData(t, &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "pk",
		FieldId:   100,
		IsDynamic: true,
	})
}

func TestUnmarshalFieldData_FloatVector(t *testing.T) {
	roundTripFieldData(t, &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: "embedding",
		FieldId:   102,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 4,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{
				Data: []float32{1.5, -2.25, 0, 3.0e9, 0.001, float32(math.Copysign(0, -1)), 42, 7},
			}},
		}},
	})
}

func TestUnmarshalFieldData_StringData(t *testing.T) {
	roundTripFieldData(t, &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "title",
		FieldId:   101,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{
				Data: []string{"hello", "", "世界", "a longer varchar value to copy"},
			}},
		}},
	})
}
