package loadmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestSchemaDoesNotIdentifyLoadConfiguration(t *testing.T) {
	// Schema was removed from the load-config protocol. Historical wire values
	// remain unknown fields and must not identify or populate the desired config.
	persisted := func(version uint64) *querypb.CollectionLoadInfo {
		info := &querypb.CollectionLoadInfo{CollectionID: 1}
		wire, err := proto.Marshal(info)
		require.NoError(t, err)
		wire = protowire.AppendVarint(protowire.AppendTag(wire, 11, protowire.VarintType), version)
		require.NoError(t, proto.Unmarshal(wire, info))
		require.NotEmpty(t, info.ProtoReflect().GetUnknown())
		return info
	}
	before := buildFromPersisted(persisted(1), nil, nil)
	after := buildFromPersisted(persisted(7), nil, nil)
	require.Equal(t, before, after)
	require.Equal(t, loadInfoVersion(before), loadInfoVersion(after))
	require.Nil(t, after.toCollectionLoadInfoProto().ProtoReflect().Descriptor().Fields().ByNumber(11))
	require.Empty(t, after.toCollectionLoadInfoProto().ProtoReflect().GetUnknown())
	header := &messagespb.AlterLoadConfigMessageHeader{CollectionId: 1}
	wire, err := proto.Marshal(header)
	require.NoError(t, err)
	wire = protowire.AppendVarint(protowire.AppendTag(wire, 8, protowire.VarintType), 9)
	require.NoError(t, proto.Unmarshal(wire, header))
	require.NotEmpty(t, header.ProtoReflect().GetUnknown())
	require.Equal(t, before, FromAlterLoadConfigMessage(header))
}
