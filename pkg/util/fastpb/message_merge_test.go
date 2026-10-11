package fastpb

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// concat marshals each message and concatenates the bytes. proto3 defines the
// concatenation of two serialized messages to be equivalent to merging them, so
// this produces a wire buffer where a singular embedded message field appears
// twice — the case fastpb must merge (not replace) to match proto.Unmarshal.
func concat(t *testing.T, msgs ...proto.Message) []byte {
	var out []byte
	for _, m := range msgs {
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		out = append(out, b...)
	}
	return out
}

// TestSingularMessageMerge verifies that a singular non-oneof embedded message
// appearing more than once on the wire is merged field-by-field (proto3 semantics),
// matching proto.Unmarshal exactly, rather than last-wins replaced.
func TestSingularMessageMerge(t *testing.T) {
	t.Run("InsertRequest.Base", func(t *testing.T) {
		wire := concat(t,
			&milvuspb.InsertRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert}, CollectionName: "c"},
			&milvuspb.InsertRequest{Base: &commonpb.MsgBase{Timestamp: 123}, NumRows: 5},
		)
		want := &milvuspb.InsertRequest{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &milvuspb.InsertRequest{}
		require.NoError(t, UnmarshalInsertRequest(wire, got))
		// sanity: the two Base occurrences must have merged, not last-wins replaced.
		require.Equal(t, commonpb.MsgType_Insert, want.Base.MsgType)
		require.Equal(t, uint64(123), want.Base.Timestamp)
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})

	t.Run("RetrieveResults.Base/Status/Ids/CostAggregation", func(t *testing.T) {
		wire := concat(t,
			&internalpb.RetrieveResults{
				Base:            &commonpb.MsgBase{MsgType: commonpb.MsgType_Retrieve},
				Status:          &commonpb.Status{Code: 1},
				Ids:             &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
				CostAggregation: &internalpb.CostAggregation{ResponseTime: 10},
			},
			&internalpb.RetrieveResults{
				Base:            &commonpb.MsgBase{Timestamp: 7},
				Status:          &commonpb.Status{Reason: "x"},
				Ids:             &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3}}}},
				CostAggregation: &internalpb.CostAggregation{TotalNQ: 2},
			},
		)
		want := &internalpb.RetrieveResults{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &internalpb.RetrieveResults{}
		require.NoError(t, UnmarshalRetrieveResults(wire, got))
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})

	t.Run("SearchResultData.Ids/GroupByFieldValue", func(t *testing.T) {
		wire := concat(t,
			&schemapb.SearchResultData{
				NumQueries:        1,
				Ids:               &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
				GroupByFieldValue: &schemapb.FieldData{FieldId: 10, Type: schemapb.DataType_Int64},
			},
			&schemapb.SearchResultData{
				TopK:              5,
				Ids:               &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}},
				GroupByFieldValue: &schemapb.FieldData{FieldName: "g"},
			},
		)
		want := &schemapb.SearchResultData{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &schemapb.SearchResultData{}
		require.NoError(t, UnmarshalSearchResultData(wire, got))
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})
}

func TestRepeatedMessageOneofMerge(t *testing.T) {
	t.Run("FieldData.Scalars", func(t *testing.T) {
		wire := concat(t,
			&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}},
			}}},
			&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{2}}},
			}}},
		)
		want := &schemapb.FieldData{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &schemapb.FieldData{}
		require.NoError(t, UnmarshalFieldData(wire, got))
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})

	t.Run("ScalarField.LongData", func(t *testing.T) {
		wire := concat(t,
			&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}},
			&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{2}}}},
		)
		want := &schemapb.ScalarField{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &schemapb.ScalarField{}
		require.NoError(t, dec{}.scalarField(wire, got))
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})

	t.Run("VectorField.FloatVector", func(t *testing.T) {
		wire := concat(t,
			&schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1}}}},
			&schemapb.VectorField{Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{2}}}},
		)
		want := &schemapb.VectorField{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &schemapb.VectorField{}
		require.NoError(t, unmarshalVectorField(wire, got))
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})

	t.Run("IDs.IntId", func(t *testing.T) {
		wire := concat(t,
			&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2}}}},
		)
		want := &schemapb.IDs{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &schemapb.IDs{}
		require.NoError(t, dec{}.ids(wire, got))
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})

	t.Run("different variants remain last-wins", func(t *testing.T) {
		wire := concat(t,
			&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{}}},
			&schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 8}}},
		)
		want := &schemapb.FieldData{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &schemapb.FieldData{}
		require.NoError(t, UnmarshalFieldData(wire, got))
		assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
	})
}

func TestElementIndicesMerge(t *testing.T) {
	wire := concat(t,
		&schemapb.SearchResultData{ElementIndices: &schemapb.LongArray{Data: []int64{1}}},
		&schemapb.SearchResultData{ElementIndices: &schemapb.LongArray{Data: []int64{2}}},
	)
	want := &schemapb.SearchResultData{}
	require.NoError(t, proto.Unmarshal(wire, want))
	got := &schemapb.SearchResultData{}
	require.NoError(t, UnmarshalSearchResultData(wire, got))
	assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
}

func TestMixedPackedUnpackedFixed32Order(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field protowire.Number
	}{
		{name: "scores", field: 4},
		{name: "distances", field: 10},
		{name: "recalls", field: 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packedFirst := protowire.AppendFixed32(nil, math.Float32bits(1))
			packedLast := protowire.AppendFixed32(nil, math.Float32bits(3))
			var wire []byte
			wire = protowire.AppendTag(wire, tc.field, protowire.BytesType)
			wire = protowire.AppendBytes(wire, packedFirst)
			wire = protowire.AppendTag(wire, tc.field, protowire.Fixed32Type)
			wire = protowire.AppendFixed32(wire, math.Float32bits(2))
			wire = protowire.AppendTag(wire, tc.field, protowire.BytesType)
			wire = protowire.AppendBytes(wire, packedLast)

			want := &schemapb.SearchResultData{}
			require.NoError(t, proto.Unmarshal(wire, want))
			got := &schemapb.SearchResultData{}
			require.NoError(t, UnmarshalSearchResultData(wire, got))
			assert.True(t, proto.Equal(got, want), "got=%v want=%v", got, want)
		})
	}
}
