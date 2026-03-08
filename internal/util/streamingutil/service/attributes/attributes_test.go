package attributes

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestAttributes(t *testing.T) {
	attr := new(Attributes)
	serverID := GetServerID(attr)
	assert.Nil(t, serverID)
	assert.Nil(t, GetChannelAssignmentInfoFromAttributes(attr))
	assert.Nil(t, GetSessionFromAttributes(attr))

	attr = new(Attributes)
	attr = WithChannelAssignmentInfo(attr, &types.StreamingNodeAssignment{
		NodeInfo: types.StreamingNodeInfo{
			ServerID: 1,
			Address:  "localhost:8080",
		},
	})
	assert.NotNil(t, GetServerID(attr))
	assert.Equal(t, int64(1), *GetServerID(attr))
	assert.NotNil(t, GetChannelAssignmentInfoFromAttributes(attr))
	assert.Equal(t, "localhost:8080", GetChannelAssignmentInfoFromAttributes(attr).NodeInfo.Address)

	attr = new(Attributes)
	attr = WithSession(attr, &sessionutil.SessionRaw{
		ServerID: 1,
	})
	assert.NotNil(t, GetServerID(attr))
	assert.Equal(t, int64(1), *GetServerID(attr))
	assert.NotNil(t, GetSessionFromAttributes(attr))
	assert.Equal(t, int64(1), GetSessionFromAttributes(attr).ServerID)

	attr = new(Attributes)
	attr = WithServerID(attr, 1)
	assert.Equal(t, int64(1), *GetServerID(attr))
}
