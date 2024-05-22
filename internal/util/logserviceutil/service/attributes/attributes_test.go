package attributes

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
)

func TestAttributes(t *testing.T) {
	attr := new(Attributes)
	serverID := GetServerID(attr)
	assert.Nil(t, serverID)
	assert.Nil(t, GetChannelAssignmentInfoFromAttributes(attr))
	assert.Nil(t, GetSessionFromAttributes(attr))

	attr = new(Attributes)
	attr = WithChannelAssignmentInfo(attr, &logpb.LogNodeAssignment{
		ServerID: 1,
		Address:  "localhost:8080",
	})
	assert.NotNil(t, GetServerID(attr))
	assert.Equal(t, int64(1), *GetServerID(attr))
	assert.NotNil(t, GetChannelAssignmentInfoFromAttributes(attr))
	assert.Equal(t, "localhost:8080", GetChannelAssignmentInfoFromAttributes(attr).Address)

	attr = new(Attributes)
	attr = WithSession(attr, &sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 1,
		},
	})
	assert.NotNil(t, GetServerID(attr))
	assert.Equal(t, int64(1), *GetServerID(attr))
	assert.NotNil(t, GetSessionFromAttributes(attr))
	assert.Equal(t, int64(1), GetSessionFromAttributes(attr).ServerID)
}
