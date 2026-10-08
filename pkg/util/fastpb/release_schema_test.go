package fastpb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// These are the audited v2.6.25 descriptors, not master's field allowlists.
// Pin kinds as well as field numbers so a wire representation change fails.
func TestProtoContract_ReleaseFieldKindsPinned(t *testing.T) {
	cases := []struct {
		message proto.Message
		want    string
	}{
		{&internalpb.RetrieveResults{}, "1=message,2=message,3=int64,4=message,5=message,6=int64,7=string,8=int64,13=message,14=int64,15=bool,16=int64,17=int64"},
		{&milvuspb.InsertRequest{}, "1=message,2=string,3=string,4=string,5=message,6=uint32,7=uint32,8=uint64,9=string"},
		{&milvuspb.UpsertRequest{}, "1=message,2=string,3=string,4=string,5=message,6=uint32,7=uint32,8=uint64,9=bool,10=string,11=message"},
		{&schemapb.SearchResultData{}, "1=int64,2=int64,3=message,4=float,5=message,6=int64,7=string,8=message,9=int64,10=float,11=message,12=float,13=string,14=message,15=message"},
		{&schemapb.FieldData{}, "1=enum,2=string,3=message,4=message,5=int64,6=bool,7=bool,8=message"},
		{&schemapb.ScalarField{}, "1=message,2=message,3=message,4=message,5=message,6=message,7=message,8=message,9=message,10=message,11=message,12=message"},
		{&schemapb.VectorField{}, "1=int64,2=message,3=bytes,4=bytes,5=bytes,6=message,7=bytes,8=message"},
		{&schemapb.IDs{}, "1=message,2=message"},
		{&schemapb.SparseFloatArray{}, "1=bytes,2=int64"},
		{&schemapb.FloatArray{}, "1=float"}, {&schemapb.LongArray{}, "1=int64"},
		{&schemapb.IntArray{}, "1=int32"}, {&schemapb.BoolArray{}, "1=bool"},
		{&schemapb.DoubleArray{}, "1=double"}, {&schemapb.BytesArray{}, "1=bytes"},
		{&schemapb.JSONArray{}, "1=bytes"}, {&schemapb.StringArray{}, "1=string"},
	}
	for _, c := range cases {
		fields := c.message.ProtoReflect().Descriptor().Fields()
		var parts []string
		kinds := make(map[int]string)
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			kinds[int(field.Number())] = string(field.Kind().String())
		}
		for _, number := range fieldNumbers(c.message) {
			parts = append(parts, fmt.Sprintf("%d=%s", number, kinds[number]))
		}
		require.Equal(t, c.want, strings.Join(parts, ","), string(c.message.ProtoReflect().Descriptor().FullName()))
	}
}

func TestReleaseAbsentFieldsRemainUnknown(t *testing.T) {
	var retrieveWire []byte
	retrieveWire = protowire.AppendTag(retrieveWire, 18, protowire.VarintType)
	retrieveWire = protowire.AppendVarint(retrieveWire, 1)
	retrieveWire = protowire.AppendTag(retrieveWire, 19, protowire.BytesType)
	retrieveWire = protowire.AppendBytes(retrieveWire, []byte{8, 42})
	var wantRetrieve, gotRetrieve internalpb.RetrieveResults
	require.NoError(t, proto.Unmarshal(retrieveWire, &wantRetrieve))
	require.NoError(t, UnmarshalRetrieveResults(retrieveWire, &gotRetrieve))
	require.True(t, proto.Equal(&wantRetrieve, &gotRetrieve))
	require.Equal(t, retrieveWire, []byte(gotRetrieve.ProtoReflect().GetUnknown()))

	var searchWire []byte
	for _, field := range []protowire.Number{17, 18, 19} {
		searchWire = protowire.AppendTag(searchWire, field, protowire.BytesType)
		searchWire = protowire.AppendBytes(searchWire, []byte{8, 42})
	}
	var wantSearch, gotSearch schemapb.SearchResultData
	require.NoError(t, proto.Unmarshal(searchWire, &wantSearch))
	require.NoError(t, UnmarshalSearchResultData(searchWire, &gotSearch))
	require.True(t, proto.Equal(&wantSearch, &gotSearch))
	require.Equal(t, searchWire, []byte(gotSearch.ProtoReflect().GetUnknown()))

	// Older receivers do not know Mol as a oneof member. It must remain unknown,
	// while known variants retain their normal wire-order semantics.
	first := &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}}}
	last := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"last"}}}}
	scalarWire, err := proto.Marshal(first)
	require.NoError(t, err)
	for _, field := range []protowire.Number{13, 14} {
		scalarWire = protowire.AppendTag(scalarWire, field, protowire.BytesType)
		scalarWire = protowire.AppendBytes(scalarWire, []byte{10, 1, 'x'})
	}
	tail, err := proto.Marshal(last)
	require.NoError(t, err)
	scalarWire = append(scalarWire, tail...)
	var wantScalar, gotScalar schemapb.ScalarField
	require.NoError(t, proto.Unmarshal(scalarWire, &wantScalar))
	require.NoError(t, (dec{}).scalarField(scalarWire, &gotScalar))
	require.True(t, proto.Equal(&wantScalar, &gotScalar))
	require.NotEmpty(t, gotScalar.ProtoReflect().GetUnknown())
	require.Equal(t, []string{"last"}, gotScalar.GetStringData().GetData())
}

func TestReleaseDecodedFieldsOwnTheirBuffer(t *testing.T) {
	fields := append(varcharFieldData(8, 12), vectorFieldData(8, 4)...)
	fields = append(fields, &schemapb.FieldData{Type: schemapb.DataType_BinaryVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2, 3, 4, 5, 6, 7, 8}}}}})
	messages := []proto.Message{
		&internalpb.RetrieveResults{FieldsData: fields},
		&milvuspb.InsertRequest{FieldsData: fields, NumRows: 8},
		&milvuspb.UpsertRequest{FieldsData: fields, NumRows: 8},
		&schemapb.SearchResultData{FieldsData: fields},
	}
	for _, src := range messages {
		wire, err := proto.Marshal(src)
		require.NoError(t, err)
		target := src.ProtoReflect().New().Interface()
		if search, ok := target.(*schemapb.SearchResultData); ok {
			require.NoError(t, UnmarshalSearchResultData(wire, search))
		} else {
			handled, err := TryUnmarshal(target, wire)
			require.True(t, handled)
			require.NoError(t, err)
		}
		for i := range wire {
			wire[i] = 0xff
		}
		require.True(t, proto.Equal(src, target), "%s aliases recycled input", src.ProtoReflect().Descriptor().FullName())
	}
}

func TestReleaseUnknownScalarBetweenSameKnownVariant(t *testing.T) {
	first := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"first"}}}}
	last := &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"last"}}}}
	for _, field := range []protowire.Number{13, 14} {
		t.Run(fmt.Sprintf("unknown_%d", field), func(t *testing.T) {
			wire, err := proto.Marshal(first)
			require.NoError(t, err)
			unknown := protowire.AppendTag(nil, field, protowire.BytesType)
			unknown = protowire.AppendBytes(unknown, []byte{10, 1, 'x'})
			wire = append(wire, unknown...)
			tail, err := proto.Marshal(last)
			require.NoError(t, err)
			wire = append(wire, tail...)
			var want, got schemapb.ScalarField
			require.NoError(t, proto.Unmarshal(wire, &want))
			require.NoError(t, (dec{}).scalarField(wire, &got))
			require.Equal(t, []string{"first", "last"}, want.GetStringData().GetData())
			require.Equal(t, []string{"first", "last"}, got.GetStringData().GetData())
			require.Equal(t, unknown, []byte(got.ProtoReflect().GetUnknown()))
			require.True(t, proto.Equal(&want, &got))
		})
	}
}
