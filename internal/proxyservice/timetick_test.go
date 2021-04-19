package proxyservice

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

func TestTimeTick_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ttStream := msgstream.NewSimpleMsgStream()
	sourceID := 1
	peerIds := []UniqueID{UniqueID(sourceID)}
	interval := 100
	minTtInterval := Timestamp(interval)

	durationInterval := time.Duration(interval*int(math.Pow10(6))) >> 18
	ttStreamProduceLoop(ctx, ttStream, durationInterval, int64(sourceID))

	ttBarrier := newSoftTimeTickBarrier(ctx, ttStream, peerIds, minTtInterval)
	channels := msgstream.NewSimpleMsgStream()

	tick := newTimeTick(ctx, ttBarrier, channels)
	err := tick.Start()
	assert.Equal(t, nil, err)
	defer tick.Close()
}

func TestTimeTick_Close(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ttStream := msgstream.NewSimpleMsgStream()
	sourceID := 1
	peerIds := []UniqueID{UniqueID(sourceID)}
	interval := 100
	minTtInterval := Timestamp(interval)

	durationInterval := time.Duration(interval*int(math.Pow10(6))) >> 18
	ttStreamProduceLoop(ctx, ttStream, durationInterval, int64(sourceID))

	ttBarrier := newSoftTimeTickBarrier(ctx, ttStream, peerIds, minTtInterval)
	channels := msgstream.NewSimpleMsgStream()

	tick := newTimeTick(ctx, ttBarrier, channels)
	err := tick.Start()
	assert.Equal(t, nil, err)
	defer tick.Close()
}
