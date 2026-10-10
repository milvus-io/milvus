package fastpb

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// repValue returns a representative non-default value for a scalar field kind.
func repValue(t *testing.T, fd protoreflect.FieldDescriptor, m protoreflect.Message) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(7)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(7)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(7)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(7)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("x") // valid UTF-8
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{1, 2, 3})
	case protoreflect.EnumKind:
		vals := fd.Enum().Values()
		if vals.Len() > 1 {
			return protoreflect.ValueOfEnum(vals.Get(1).Number())
		}
		return protoreflect.ValueOfEnum(0)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return protoreflect.ValueOfMessage(m.NewField(fd).Message())
	default:
		t.Fatalf("unhandled kind %v for %s", fd.Kind(), fd.FullName())
		return protoreflect.Value{}
	}
}

func setOneField(t *testing.T, m protoreflect.Message, fd protoreflect.FieldDescriptor) {
	switch {
	case fd.IsList():
		list := m.Mutable(fd).List()
		switch fd.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			list.Append(protoreflect.ValueOfMessage(list.NewElement().Message()))
		default:
			list.Append(repValue(t, fd, m))
		}
	case fd.IsMap():
		t.Logf("skipping map field %s (none expected in decoded types)", fd.FullName())
	default:
		m.Set(fd, repValue(t, fd, m))
	}
}

// assertFieldCoverage sets each field of fresh's descriptor individually,
// marshals with the official codec, decodes with decode, and asserts proto.Equal.
// A dropped or mis-decoded field — including a newly-added proto field — fails here.
func assertFieldCoverage(t *testing.T, fresh proto.Message, decode func([]byte) (proto.Message, error)) {
	t.Helper()
	fields := fresh.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		msg := fresh.ProtoReflect().New()
		setOneField(t, msg, fd)
		src := msg.Interface()
		wire, err := proto.Marshal(src)
		if err != nil {
			t.Fatalf("marshal %s: %v", fd.FullName(), err)
		}
		got, err := decode(wire)
		if err != nil {
			t.Fatalf("decode %s: %v", fd.FullName(), err)
		}
		if !proto.Equal(src, got) {
			t.Fatalf("field %s NOT round-tripped by fastpb (dropped or mis-decoded):\n src=%v\n got=%v",
				fd.FullName(), src, got)
		}
	}
}

func TestCoverage_FieldData(t *testing.T) {
	assertFieldCoverage(t, &schemapb.FieldData{}, func(b []byte) (proto.Message, error) {
		m := &schemapb.FieldData{}
		return m, UnmarshalFieldData(b, m)
	})
}

func TestCoverage_ScalarField(t *testing.T) {
	assertFieldCoverage(t, &schemapb.ScalarField{}, func(b []byte) (proto.Message, error) {
		m := &schemapb.ScalarField{}
		return m, dec{}.scalarField(b, m)
	})
}

func TestCoverage_VectorField(t *testing.T) {
	assertFieldCoverage(t, &schemapb.VectorField{}, func(b []byte) (proto.Message, error) {
		m := &schemapb.VectorField{}
		return m, unmarshalVectorField(b, m)
	})
}

func TestCoverage_IDs(t *testing.T) {
	assertFieldCoverage(t, &schemapb.IDs{}, func(b []byte) (proto.Message, error) {
		m := &schemapb.IDs{}
		return m, dec{}.ids(b, m)
	})
}

func TestCoverage_SearchResultData(t *testing.T) {
	assertFieldCoverage(t, &schemapb.SearchResultData{}, func(b []byte) (proto.Message, error) {
		m := &schemapb.SearchResultData{}
		return m, UnmarshalSearchResultData(b, m)
	})
}

func TestCoverage_RetrieveResults(t *testing.T) {
	assertFieldCoverage(t, &internalpb.RetrieveResults{}, func(b []byte) (proto.Message, error) {
		m := &internalpb.RetrieveResults{}
		return m, UnmarshalRetrieveResults(b, m)
	})
}

func TestCoverage_InsertRequest(t *testing.T) {
	assertFieldCoverage(t, &milvuspb.InsertRequest{}, func(b []byte) (proto.Message, error) {
		m := &milvuspb.InsertRequest{}
		return m, UnmarshalInsertRequest(b, m)
	})
}

// TestStructArrays_RoundTrips documents the previously-dropped field explicitly.
func TestStructArrays_RoundTrips(t *testing.T) {
	roundTripFieldData(t, &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
		FieldName: "structs",
		FieldId:   200,
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{
			Fields: []*schemapb.FieldData{
				{Type: schemapb.DataType_Int64, FieldName: "sub", FieldId: 201, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}}},
			},
		}},
	})
}
