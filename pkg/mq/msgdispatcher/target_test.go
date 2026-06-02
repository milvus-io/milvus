package msgdispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSendTimeout(t *testing.T) {
	target := newTarget(&StreamConfig{
		VChannel: "test1",
		Pos:      &msgpb.MsgPosition{},
	}, false)

	time.Sleep(paramtable.Get().MQCfg.MaxTolerantLag.GetAsDuration(time.Second))

	counter := 0
	for i := 0; i < 10; i++ {
		err := target.send(&msgstream.MsgPack{})
		if err != nil {
			mlog.Error(context.TODO(), "send failed", mlog.Int("idx", i), mlog.Err(err))
			counter++
		}
	}
	assert.Equal(t, counter, 0)
}

func TestSendTimeTickFiltering(t *testing.T) {
	target := newTarget(&StreamConfig{
		VChannel: "test1",
		Pos:      &msgpb.MsgPosition{},
	}, true)
	target.send(&msgstream.MsgPack{
		EndPositions: []*msgpb.MsgPosition{
			{
				Timestamp: 1,
			},
		},
	})

	target.send(&msgstream.MsgPack{
		EndPositions: []*msgpb.MsgPosition{
			{
				Timestamp: 1,
			},
		},
	})
}
