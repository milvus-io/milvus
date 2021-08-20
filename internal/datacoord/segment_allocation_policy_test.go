package datacoord

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

func TestSealSegmentPolicy(t *testing.T) {
	t.Run("test seal segment by lifetime", func(t *testing.T) {
		lifetime := 2 * time.Second
		now := time.Now()
		curTS := now.UnixNano() / int64(time.Millisecond)
		nosealTs := (now.Add(lifetime / 2)).UnixNano() / int64(time.Millisecond)
		sealTs := (now.Add(lifetime)).UnixNano() / int64(time.Millisecond)

		p := sealByLifetimePolicy(lifetime)

		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				LastExpireTime: tsoutil.ComposeTS(curTS, 0),
			},
		}

		shouldSeal := p(segment, tsoutil.ComposeTS(nosealTs, 0))
		assert.False(t, shouldSeal)

		shouldSeal = p(segment, tsoutil.ComposeTS(sealTs, 0))
		assert.True(t, shouldSeal)
	})
}
