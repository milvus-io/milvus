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
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for new channel notification")
	}
}

func TestConfigChannelProvider_NoDuplicates(t *testing.T) {
	paramtable.Init()
	provider := NewConfigChannelProvider()
	defer provider.Close()

	select {
	case newChannels := <-provider.NewIncomingChannels():
		t.Fatalf("unexpected new channels: %v", newChannels)
	case <-time.After(3 * time.Second):
		// Expected: no notification when config has not changed
	}
}

func TestConfigChannelProvider_CloseStopsPolling(t *testing.T) {
	paramtable.Init()
	provider := NewConfigChannelProvider()
	provider.Close()

	_, ok := <-provider.NewIncomingChannels()
	assert.False(t, ok, "channel should be closed after provider.Close()")
}
