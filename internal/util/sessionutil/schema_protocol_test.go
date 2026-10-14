package sessionutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestSessionSchemaProtocolSerialization(t *testing.T) {
	var legacy Session
	require.NoError(t, json.Unmarshal([]byte(`{"ServerID":42,"ServerName":"querynode","Version":"3.0.0"}`), &legacy))
	require.Empty(t, legacy.QueryViewSchemaProtocol, "old registration must not imply schema protocol support")
	legacy.QueryViewSchemaProtocol = common.QueryViewSchemaProtocol
	data, err := json.Marshal(&legacy)
	require.NoError(t, err)
	var restored Session
	require.NoError(t, json.Unmarshal(data, &restored))
	require.Equal(t, common.QueryViewSchemaProtocol, restored.QueryViewSchemaProtocol)
}
