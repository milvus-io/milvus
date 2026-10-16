package loadmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestSchemaDoesNotIdentifyLoadConfiguration(t *testing.T) {
	first := &querypb.CollectionLoadInfo{}
	latest := &querypb.CollectionLoadInfo{}
	require.NoError(t, protojson.Unmarshal([]byte(`{"collectionID":"1","schemaVersion":1}`), first))
	require.NoError(t, protojson.Unmarshal([]byte(`{"collectionID":"1","schemaVersion":7}`), latest))
	before := buildFromPersisted(first, nil, nil)
	after := buildFromPersisted(latest, nil, nil)
	require.Equal(t, before, after)
	require.Equal(t, loadInfoVersion(before), loadInfoVersion(after))
	require.Equal(t, int64(0), after.toCollectionLoadInfoProto().ProtoReflect().Get(latest.ProtoReflect().Descriptor().Fields().ByNumber(11)).Int())
	header := &messagespb.AlterLoadConfigMessageHeader{}
	require.NoError(t, protojson.Unmarshal([]byte(`{"collectionId":"1","schemaVersion":9}`), header))
	require.Equal(t, before, FromAlterLoadConfigMessage(header))
}
