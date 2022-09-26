package rootcoord

import (
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"

	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/stretchr/testify/assert"
)

func Test_expireCacheConfig_apply(t *testing.T) {
	c := defaultExpireCacheConfig()
	req := &proxypb.InvalidateCollMetaCacheRequest{}
	c.apply(req)
	assert.Nil(t, req.GetBase())
	opt := expireCacheWithDropFlag()
	opt(&c)
	c.apply(req)
	assert.Equal(t, commonpb.MsgType_DropCollection, req.GetBase().GetMsgType())
}
