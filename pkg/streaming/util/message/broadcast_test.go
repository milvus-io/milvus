package message

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

func TestBroadcastResult(t *testing.T) {
	r := BroadcastResult[*CreateDatabaseMessageHeader, *CreateDatabaseMessageBody]{
		Message: nil,
		Results: map[string]*AppendResult{
			"v1":                                  {TimeTick: 1},
			"v2":                                  {TimeTick: 2},
			"abc" + funcutil.ControlChannelSuffix: {TimeTick: 3},
		},
	}

	assert.ElementsMatch(t, []string{"v1", "v2"}, r.GetVChannelsWithoutControlChannel())
	assert.NotNil(t, r.GetControlChannelResult())
	assert.Equal(t, uint64(3), r.GetMaxTimeTick())
}
