package fastpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// appendGroup appends a proto2 group-encoded field (start-group ... end-group,
// wire types 3/4) carrying one nested varint. proto3 never produces this; the
// official codec preserves it as an unknown field.
func appendGroup(b []byte, fieldNum int32) []byte {
	b = protowire.AppendTag(b, protowire.Number(fieldNum), protowire.StartGroupType)
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, 42)
	b = protowire.AppendTag(b, protowire.Number(fieldNum), protowire.EndGroupType)
	return b
}

// TestProto2GroupFallback: a wire buffer containing a proto2 group (which fastpb
// does not decode) must fall back to the official codec and yield a byte-for-byte
// identical message, never an error (issue: fastpb proto2 group handling).
func TestProto2GroupFallback(t *testing.T) {
	t.Run("InsertRequest/top-level group", func(t *testing.T) {
		canonical, err := proto.Marshal(&milvuspb.InsertRequest{
			CollectionName: "c", DbName: "db", NumRows: 3,
			FieldsData: []*schemapb.FieldData{{FieldId: 1, Type: schemapb.DataType_Int64, FieldName: "id"}},
		})
		require.NoError(t, err)
		wire := appendGroup(append([]byte{}, canonical...), 1000)

		want := &milvuspb.InsertRequest{}
		require.NoError(t, proto.Unmarshal(wire, want), "official codec must accept the group")
		got := &milvuspb.InsertRequest{}
		require.NoError(t, UnmarshalInsertRequest(wire, got), "fastpb must fall back, not error")
		assert.True(t, proto.Equal(got, want), "fallback result must equal official decode")
	})

	t.Run("InsertRequest/group nested in fields_data", func(t *testing.T) {
		// Build a FieldData submessage whose bytes carry a nested group, exercising
		// errProto2 propagation up from the nested fieldData decoder to the entry.
		fdBytes, err := proto.Marshal(&schemapb.FieldData{FieldId: 1, Type: schemapb.DataType_Int64, FieldName: "id"})
		require.NoError(t, err)
		fdBytes = appendGroup(fdBytes, 999)

		var wire []byte
		wire = protowire.AppendTag(wire, 3, protowire.BytesType) // collection_name
		wire = protowire.AppendString(wire, "c")
		wire = protowire.AppendTag(wire, 5, protowire.BytesType) // fields_data (5)
		wire = protowire.AppendBytes(wire, fdBytes)

		want := &milvuspb.InsertRequest{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &milvuspb.InsertRequest{}
		require.NoError(t, UnmarshalInsertRequest(wire, got))
		assert.True(t, proto.Equal(got, want))
	})

	t.Run("SearchResultData/top-level group", func(t *testing.T) {
		canonical, err := proto.Marshal(&schemapb.SearchResultData{NumQueries: 2, TopK: 5})
		require.NoError(t, err)
		wire := appendGroup(append([]byte{}, canonical...), 1000)

		want := &schemapb.SearchResultData{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &schemapb.SearchResultData{}
		require.NoError(t, UnmarshalSearchResultData(wire, got))
		assert.True(t, proto.Equal(got, want))
	})

	t.Run("RetrieveResults/top-level group", func(t *testing.T) {
		canonical, err := proto.Marshal(&internalpb.RetrieveResults{ReqID: 7, AllRetrieveCount: 9})
		require.NoError(t, err)
		wire := appendGroup(append([]byte{}, canonical...), 1000)

		want := &internalpb.RetrieveResults{}
		require.NoError(t, proto.Unmarshal(wire, want))
		got := &internalpb.RetrieveResults{}
		require.NoError(t, UnmarshalRetrieveResults(wire, got))
		assert.True(t, proto.Equal(got, want))
	})
}
