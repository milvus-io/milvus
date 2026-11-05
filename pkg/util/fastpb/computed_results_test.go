package fastpb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// computed_results is a new ordinary message field. The existing rest ->
// protoMerge fallback must populate the known field, including repeated wire
// occurrences, rather than merely leaving it in the unknown-field buffer.
func TestRetrieveComputedResultsFallback(t *testing.T) {
	for _, repeat := range []bool{false, true} {
		src := &internalpb.RetrieveResults{ReqID: 42, FeatureBits: 1<<63 | 0b1011, ComputedResults: []*schemapb.FieldData{{FieldId: 1, FieldName: "matches", Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}}}}}}

		wire, err := proto.Marshal(src)
		require.NoError(t, err)
		if repeat {
			wire = append(wire, wire...)
		}
		want := &internalpb.RetrieveResults{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &internalpb.RetrieveResults{}
		handled, err := TryUnmarshal(got, wire)
		require.True(t, handled)
		require.NoError(t, err)
		require.NotNil(t, got.ComputedResults)
		require.True(t, proto.Equal(want, got))
		require.Empty(t, got.ProtoReflect().GetUnknown())
	}
	malformed := protowire.AppendTag(nil, 22, protowire.BytesType)
	malformed = protowire.AppendVarint(malformed, 10)
	malformed = append(malformed, 1)
	require.Error(t, UnmarshalRetrieveResults(malformed, &internalpb.RetrieveResults{}))
}
