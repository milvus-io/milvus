package util

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestConfigChannelProvider_GetInitialChannels(t *testing.T) {
	paramtable.Init()
	provider := newTestConfigChannelProvider()
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
	provider := newTestConfigChannelProvider()
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

func TestConfigChannelProvider_DetectsChannelsFromPChannelStats(t *testing.T) {
	paramtable.Init()

	originalNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetValue()
	originalDML := paramtable.Get().CommonCfg.RootCoordDml.GetValue()
	paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "2")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootCoordDml.Key, "rootcoord-dml")
	defer paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, originalNum)
	defer paramtable.Get().Save(paramtable.Get().CommonCfg.RootCoordDml.Key, originalDML)

	statsFuture := syncutil.NewFuture[*testPChannelStats]()
	provider := NewConfigChannelProviderWithPChannelStatsManager(statsFuture)
	defer provider.Close()

	prefix := paramtable.Get().CommonCfg.RootCoordDml.GetValue()
	statsFuture.Set(&testPChannelStats{
		pchannels: []string{fmt.Sprintf("%s_3", prefix)},
	})

	select {
	case newChannels := <-provider.NewIncomingChannels():
		require.ElementsMatch(t, []string{
			fmt.Sprintf("%s_2", prefix),
			fmt.Sprintf("%s_3", prefix),
		}, newChannels)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pchannel stats notification")
	}
}

func TestConfigChannelProvider_NoDuplicates(t *testing.T) {
	paramtable.Init()

	originalNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetValue()
	provider := newTestConfigChannelProvider()
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
	provider := newTestConfigChannelProvider()
	provider.Close()

	_, ok := <-provider.NewIncomingChannels()
	assert.False(t, ok, "channel should be closed after provider.Close()")
}

func TestConfigChannelProvider_CloseUnblocksInFlightSend(t *testing.T) {
	paramtable.Init()

	originalNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetValue()
	provider := newTestConfigChannelProvider()

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

func TestGetAllTopicsFromConfigurationAndPChannelsKeepsNonDMLChannels(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "1")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootCoordDml.Key, "rootcoord-dml")
	defer paramtable.Get().Reset(paramtable.Get().RootCoordCfg.DmlChannelNum.Key)
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.RootCoordDml.Key)

	channels := getAllTopicsFromConfigurationAndPChannels([]string{"custom-pchannel"})

	assert.True(t, channels.Contain("custom-pchannel"))
	assert.True(t, channels.Contain(fmt.Sprintf("%s_0", paramtable.Get().CommonCfg.RootCoordDml.GetValue())))
}

type testPChannelStats struct {
	pchannels []string
}

func (s *testPChannelStats) PChannels() []string {
	return append([]string(nil), s.pchannels...)
}

func newTestConfigChannelProvider() *ConfigChannelProvider {
	statsFuture := syncutil.NewFuture[*testPChannelStats]()
	statsFuture.Set(&testPChannelStats{})
	return NewConfigChannelProviderWithPChannelStatsManager(statsFuture)
}
