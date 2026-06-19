package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithClusterLevelBroadcast(t *testing.T) {
	cc := ClusterChannels{
		Channels:       []string{"pchannel1", "pchannel2"},
		ControlChannel: "pchannel1_vcchan",
	}

	msg := NewFlushAllMessageBuilderV2().
		WithHeader(&FlushAllMessageHeader{}).
		WithBody(&FlushAllMessageBody{}).
		WithClusterLevelBroadcast(cc).
		MustBuildBroadcast()

	// The message should be marked as pchannel-level.
	assert.True(t, msg.IsPChannelLevel())

	// The broadcast header should contain control channel substituted for pchannel1.
	bh := msg.BroadcastHeader()
	assert.NotNil(t, bh)
	assert.ElementsMatch(t, []string{"pchannel1_vcchan", "pchannel2"}, bh.VChannels)

	// Split should produce messages each marked as pchannel-level.
	msg.WithBroadcastID(1)
	splitMsgs := msg.SplitIntoMutableMessage()
	assert.Len(t, splitMsgs, 2)
	for _, sm := range splitMsgs {
		assert.True(t, sm.IsPChannelLevel())
	}
}

func TestWithClusterLevelBroadcastPanics(t *testing.T) {
	t.Run("EmptyChannels", func(t *testing.T) {
		assert.Panics(t, func() {
			NewFlushAllMessageBuilderV2().
				WithHeader(&FlushAllMessageHeader{}).
				WithBody(&FlushAllMessageBody{}).
				WithClusterLevelBroadcast(ClusterChannels{
					ControlChannel: "pchannel1_vcchan",
				}).
				MustBuildBroadcast()
		})
	})

	t.Run("EmptyControlChannel", func(t *testing.T) {
		assert.Panics(t, func() {
			NewFlushAllMessageBuilderV2().
				WithHeader(&FlushAllMessageHeader{}).
				WithBody(&FlushAllMessageBody{}).
				WithClusterLevelBroadcast(ClusterChannels{
					Channels: []string{"pchannel1"},
				}).
				MustBuildBroadcast()
		})
	})

	t.Run("NonPChannelInChannels", func(t *testing.T) {
		assert.Panics(t, func() {
			NewFlushAllMessageBuilderV2().
				WithHeader(&FlushAllMessageHeader{}).
				WithBody(&FlushAllMessageBody{}).
				WithClusterLevelBroadcast(ClusterChannels{
					Channels:       []string{"pchannel1", "pchannel2_100v0"},
					ControlChannel: "pchannel1_vcchan",
				}).
				MustBuildBroadcast()
		})
	})

	t.Run("ControlChannelNotOnAnyPChannel", func(t *testing.T) {
		assert.Panics(t, func() {
			NewFlushAllMessageBuilderV2().
				WithHeader(&FlushAllMessageHeader{}).
				WithBody(&FlushAllMessageBody{}).
				WithClusterLevelBroadcast(ClusterChannels{
					Channels:       []string{"pchannel1", "pchannel2"},
					ControlChannel: "pchannel3_vcchan",
				}).
				MustBuildBroadcast()
		})
	})
}

func TestPChannel(t *testing.T) {
	t.Run("WithVChannel", func(t *testing.T) {
		msg := &messageImpl{
			payload: []byte("test"),
			properties: propertiesImpl{
				messageVChannel: "pchannel1_v0",
			},
		}
		assert.Equal(t, "pchannel1", msg.PChannel())
	})

	t.Run("EmptyVChannel", func(t *testing.T) {
		msg := &messageImpl{
			payload:    []byte("test"),
			properties: propertiesImpl{},
		}
		assert.Equal(t, "", msg.PChannel())
	})

	t.Run("SplitBroadcastMessages", func(t *testing.T) {
		cc := ClusterChannels{
			Channels:       []string{"pchannel1", "pchannel2"},
			ControlChannel: "pchannel1_vcchan",
		}
		msg := NewFlushAllMessageBuilderV2().
			WithHeader(&FlushAllMessageHeader{}).
			WithBody(&FlushAllMessageBody{}).
			WithClusterLevelBroadcast(cc).
			MustBuildBroadcast()
		msg.WithBroadcastID(1)

		splitMsgs := msg.SplitIntoMutableMessage()
		pchannels := make([]string, 0, len(splitMsgs))
		for _, sm := range splitMsgs {
			pchannels = append(pchannels, sm.PChannel())
		}
		assert.ElementsMatch(t, []string{"pchannel1", "pchannel2"}, pchannels)
	})
}

func TestIsPChannelLevel(t *testing.T) {
	t.Run("NotPChannelLevel", func(t *testing.T) {
		msg := &messageImpl{
			payload:    []byte("test"),
			properties: propertiesImpl{},
		}
		assert.False(t, msg.IsPChannelLevel())
	})

	t.Run("IsPChannelLevel", func(t *testing.T) {
		msg := &messageImpl{
			payload: []byte("test"),
			properties: propertiesImpl{
				messagePChannelLevel: "",
			},
		}
		assert.True(t, msg.IsPChannelLevel())
	})
}
