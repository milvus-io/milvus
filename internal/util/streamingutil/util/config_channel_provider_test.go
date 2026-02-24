package util

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestConfigChannelProvider_GetInitialChannels(t *testing.T) {
	paramtable.Init()
	provider := NewConfigChannelProvider()
	defer provider.Close()

	initial := provider.GetInitialChannels()
	assert.NotEmpty(t, initial)

	expected := GetAllTopicsFromConfiguration().Collect()
	sort.Strings(expected)
	sort.Strings(initial)
	assert.Equal(t, expected, initial)
}

func TestConfigChannelProvider_DetectsNewChannels(t *testing.T) {
	paramtable.Init()

	originalNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetValue()
	provider := NewConfigChannelProvider()
	defer provider.Close()

	initial := provider.GetInitialChannels()
	initialCount := len(initial)

	newNum := initialCount + 1
	paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, fmt.Sprintf("%d", newNum))
	defer paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, originalNum)

	select {
	case newChannels := <-provider.NewIncomingChannels():
		assert.Len(t, newChannels, 1)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for new channel notification")
	}
}

func TestConfigChannelProvider_NoDuplicates(t *testing.T) {
	paramtable.Init()

	originalNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetValue()
	provider := NewConfigChannelProvider()
	defer provider.Close()

	// Trigger a config change with the same value, should not produce new channels.
	paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, originalNum)

	select {
	case newChannels := <-provider.NewIncomingChannels():
		t.Fatalf("unexpected new channels: %v", newChannels)
	case <-time.After(1 * time.Second):
		// Expected: no notification when config produces the same channels
	}
}

func TestConfigChannelProvider_CloseStopsWatching(t *testing.T) {
	paramtable.Init()
	provider := NewConfigChannelProvider()
	provider.Close()

	_, ok := <-provider.NewIncomingChannels()
	assert.False(t, ok, "channel should be closed after provider.Close()")
}

func TestConfigChannelProvider_CloseUnblocksInFlightSend(t *testing.T) {
	paramtable.Init()

	originalNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetValue()
	provider := NewConfigChannelProvider()

	initial := provider.GetInitialChannels()
	initialCount := len(initial)

	// Trigger a config change that will produce new channels.
	// Nobody reads from NewIncomingChannels(), so the background goroutine
	// will block on the channel send.
	newNum := initialCount + 1
	paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, fmt.Sprintf("%d", newNum))
	defer paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, originalNum)

	// Give the background goroutine time to pick up the trigger and block on send.
	time.Sleep(200 * time.Millisecond)

	// Close must not deadlock: it should cancel the blocked send and wait for
	// the background goroutine to exit.
	done := make(chan struct{})
	go func() {
		provider.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close returned successfully.
	case <-time.After(5 * time.Second):
		t.Fatal("Close() deadlocked while background goroutine was blocked on channel send")
	}
}
