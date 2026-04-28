package adaptor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	mock_message "github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
)

func TestOldVersionLastConfirmedTracker_DefaultWindowSize(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(0)
	assert.Equal(t, 30, tracker.windowSize)
}

func TestOldVersionLastConfirmedTracker_BeforeWindowFull(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(3)

	ids := make([]*mock_message.MockMessageID, 3)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	// First message: should return itself (the first one)
	result := tracker.Track(ids[0])
	assert.Equal(t, ids[0], result)

	// Second message: still returns the first one (window not full)
	result = tracker.Track(ids[1])
	assert.Equal(t, ids[0], result)

	// Third message: still returns the first one (window size = 3, need 4th to start sliding)
	result = tracker.Track(ids[2])
	assert.Equal(t, ids[0], result)
}

func TestOldVersionLastConfirmedTracker_WindowSliding(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(3)

	ids := make([]*mock_message.MockMessageID, 6)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	// Fill the window: track ids[0], ids[1], ids[2]
	tracker.Track(ids[0])
	tracker.Track(ids[1])
	tracker.Track(ids[2])

	// 4th message (ids[3]): window is now [ids[0], ids[1], ids[2], ids[3]]
	// len=4 > windowSize=3, return ids[4-3-1] = ids[0]
	result := tracker.Track(ids[3])
	assert.Equal(t, ids[0], result, "should return the message 3 positions back")

	// 5th message (ids[4]): window is [ids[0]..ids[4]]
	// return ids[5-3-1] = ids[1]
	result = tracker.Track(ids[4])
	assert.Equal(t, ids[1], result, "should return the message 3 positions back")

	// 6th message (ids[5]): window is [ids[0]..ids[5]]
	// return ids[6-3-1] = ids[2]
	result = tracker.Track(ids[5])
	assert.Equal(t, ids[2], result, "should return the message 3 positions back")
}

func TestOldVersionLastConfirmedTracker_WindowSizeOne(t *testing.T) {
	tracker := newOldVersionLastConfirmedTracker(1)

	ids := make([]*mock_message.MockMessageID, 3)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	// First message: returns itself
	result := tracker.Track(ids[0])
	assert.Equal(t, ids[0], result)

	// Second message: returns the one 1 position back = ids[0]
	result = tracker.Track(ids[1])
	assert.Equal(t, ids[0], result)

	// Third message: returns ids[1]
	result = tracker.Track(ids[2])
	assert.Equal(t, ids[1], result)
}

func TestOldVersionLastConfirmedTracker_LargeWindow(t *testing.T) {
	windowSize := 30
	tracker := newOldVersionLastConfirmedTracker(windowSize)

	totalMessages := 100
	ids := make([]*mock_message.MockMessageID, totalMessages)
	for i := range ids {
		ids[i] = mock_message.NewMockMessageID(t)
	}

	for i := 0; i < totalMessages; i++ {
		result := tracker.Track(ids[i])
		if i < windowSize {
			// Before window is full, always return the first ID
			assert.Equal(t, ids[0], result, "before window full, should return first ID at i=%d", i)
		} else {
			// After window is full, return the ID from windowSize positions back
			expected := ids[i-windowSize]
			assert.Equal(t, expected, result, "should return ID from %d positions back at i=%d", windowSize, i)
		}
	}
}
