package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestWALMetrics(t *testing.T) {
	now := time.Now()
	rw := RWWALMetrics{
		ChannelInfo: PChannelInfo{
			Name: "ch1",
		},
		MVCCTimeTick:     tsoutil.ComposeTSByTime(now),
		RecoveryTimeTick: tsoutil.ComposeTSByTime(now.Add(-time.Second)),
	}
	assert.Equal(t, time.Second, rw.RecoveryLag())
	rw.MVCCTimeTick = tsoutil.ComposeTSByTime(now.Add(-2 * time.Second))
	assert.Zero(t, rw.RecoveryLag())
}
