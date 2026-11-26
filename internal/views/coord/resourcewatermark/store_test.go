package resourcewatermark

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestWatermarksValidityFreshnessAndStreamFencing(t *testing.T) {
	s := New()
	_, ok := s.Read(1, time.Now())
	require.False(t, ok)
	first := s.Start(1)
	_, ok = s.Read(1, time.Now())
	require.False(t, ok, "opening a stream is not a zero-watermark report")
	first(&viewpb.NodeResourceWatermarks{})
	sample, ok := s.Read(1, time.Now())
	require.True(t, ok, "explicit zero is valid")
	_, ok = s.Read(1, sample.ReceivedAt.Add(MaxAge+time.Nanosecond))
	require.False(t, ok)
	for _, invalid := range []float64{-1, 101, math.NaN(), math.Inf(1), math.Inf(-1)} {
		first(&viewpb.NodeResourceWatermarks{Memory: invalid})
		_, ok = s.Read(1, time.Now())
		require.False(t, ok)
		first(&viewpb.NodeResourceWatermarks{Disk: invalid})
		_, ok = s.Read(1, time.Now())
		require.False(t, ok)
	}
	first(&viewpb.NodeResourceWatermarks{Memory: 80, Disk: 100})
	sample, ok = s.Read(1, time.Now())
	require.True(t, ok, "valid reports recover after a malformed measurement")
	require.Equal(t, 100.0, sample.Disk)
	second := s.Start(1)
	second(&viewpb.NodeResourceWatermarks{Memory: 20, Disk: 30})
	first(&viewpb.NodeResourceWatermarks{Memory: 99, Disk: 99})
	first(nil)
	sample, ok = s.Read(1, time.Now())
	require.True(t, ok)
	require.Equal(t, 20.0, sample.Memory)
	require.Equal(t, 30.0, sample.Disk)
	require.Zero(t, testing.AllocsPerRun(100, func() { _, _ = s.Read(1, time.Now()) }))
	second(nil)
	require.Empty(t, s.nodes, "disconnected nodes cannot accumulate")
	second(&viewpb.NodeResourceWatermarks{})
	require.Empty(t, s.nodes, "late reports cannot resurrect a disconnected stream")
}
