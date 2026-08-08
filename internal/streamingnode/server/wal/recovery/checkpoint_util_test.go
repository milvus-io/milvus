//go:build test
// +build test

package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestCheckpointComparisonFallsBackToTimeTickAcrossWALBackends(t *testing.T) {
	rmqCheckpoint := &WALCheckpoint{
		MessageID: rmq.NewRmqID(100),
		TimeTick:  200,
	}
	testCheckpoint := &WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(10),
		TimeTick:  120,
	}

	require.NotPanics(t, func() {
		require.Same(t, testCheckpoint, minCheckpointByMessageID(rmqCheckpoint, testCheckpoint))
	})
	require.True(t, checkpointCovers(rmqCheckpoint, &WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(20),
		TimeTick:  199,
	}))
	require.False(t, checkpointCovers(rmqCheckpoint, &WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(20),
		TimeTick:  201,
	}))
	require.True(t, sameWALCheckpoint(rmqCheckpoint, &WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(20),
		TimeTick:  200,
	}))
}
