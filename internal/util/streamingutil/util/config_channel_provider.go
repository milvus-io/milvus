package util

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ConfigChannelProvider implements channel.ChannelProvider by watching
// the Milvus configuration for new DML channel names.
type ConfigChannelProvider struct {
	notifier        *syncutil.AsyncTaskNotifier[struct{}]
	known           typeutil.Set[string]
	initialChannels []string
	ch              chan []string
	trigger         chan struct{}
	handler         config.EventHandler
	statsReady      <-chan struct{}
	getPChannels    func() []string
}

// NewConfigChannelProviderWithPChannelStatsManager creates a ConfigChannelProvider
// that reads channel names from configuration and also discovers pchannels
// recovered from collection metadata.
func NewConfigChannelProviderWithPChannelStatsManager[T interface{ PChannels() []string }](statsFuture *syncutil.Future[T]) *ConfigChannelProvider {
	currentTopics := GetAllTopicsFromConfiguration()
	initial := currentTopics.Collect()
	sort.Strings(initial)

	p := &ConfigChannelProvider{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		known:           currentTopics,
		initialChannels: initial,
		ch:              make(chan []string),
		trigger:         make(chan struct{}, 1),
	}
	p.statsReady = statsFuture.Done()
	p.getPChannels = func() []string {
		return statsFuture.Get().PChannels()
	}
	p.handler = config.NewHandler("config_channel_provider", func(event *config.Event) {
		// Non-blocking send to coalesce rapid config changes.
		select {
		case p.trigger <- struct{}{}:
		default:
		}
	})
	go p.background()
	paramtable.Get().Watch(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, p.handler)
	return p
}

// GetInitialChannels returns the channel names known at startup time.
func (p *ConfigChannelProvider) GetInitialChannels() []string {
	return p.initialChannels
}

// NewIncomingChannels returns a read-only channel that delivers slices
// of newly discovered channel names.
func (p *ConfigChannelProvider) NewIncomingChannels() <-chan []string {
	return p.ch
}

// Close stops the provider and closes the notification channel.
func (p *ConfigChannelProvider) Close() {
	paramtable.Get().Unwatch(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, p.handler)
	p.notifier.Cancel()
	p.notifier.BlockUntilFinish()
	close(p.ch)
}

// background is the single goroutine that processes config change triggers.
func (p *ConfigChannelProvider) background() {
	defer p.notifier.Finish(struct{}{})
	statsReady := p.statsReady
	for {
		select {
		case <-p.trigger:
			p.onConfigChanged()
		case <-statsReady:
			statsReady = nil
			p.onPChannelStatsReady()
		case <-p.notifier.Context().Done():
			return
		}
	}
}

func (p *ConfigChannelProvider) onConfigChanged() {
	p.notifyNewChannels(GetAllTopicsFromConfiguration(), "ConfigChannelProvider detected new channels")
}

func (p *ConfigChannelProvider) onPChannelStatsReady() {
	p.notifyNewChannels(
		getAllTopicsFromConfigurationAndPChannels(p.getPChannels()),
		"ConfigChannelProvider detected new channels from pchannel stats",
	)
}

func (p *ConfigChannelProvider) notifyNewChannels(current typeutil.Set[string], logMessage string) {
	var newChannels []string
	current.Range(func(name string) bool {
		if !p.known.Contain(name) {
			newChannels = append(newChannels, name)
			p.known.Insert(name)
		}
		return true
	})
	if len(newChannels) > 0 {
		sort.Strings(newChannels)
		log.Info(logMessage, zap.Strings("newChannels", newChannels))
		select {
		case p.ch <- newChannels:
		case <-p.notifier.Context().Done():
		}
	}
}

func getAllTopicsFromConfigurationAndPChannels(pchannels []string) typeutil.Set[string] {
	current := GetAllTopicsFromConfiguration()
	if paramtable.Get().CommonCfg.PreCreatedTopicEnabled.GetAsBool() {
		for _, pchannel := range pchannels {
			if !current.Contain(pchannel) {
				log.Warn("skip pchannel not in pre-created topic list",
					zap.String("pchannel", pchannel))
			}
		}
		return current
	}

	prefix := paramtable.Get().CommonCfg.RootCoordDml.GetValue()
	for _, pchannel := range pchannels {
		if idx, ok := parseConfiguredDMLChannelIndex(pchannel, prefix); ok {
			for i := 0; i <= idx; i++ {
				current.Insert(fmt.Sprintf("%s_%d", prefix, i))
			}
			continue
		}
		current.Insert(pchannel)
	}
	return current
}

func parseConfiguredDMLChannelIndex(pchannel string, prefix string) (int, bool) {
	suffix, ok := strings.CutPrefix(pchannel, prefix+"_")
	if !ok || suffix == "" {
		return 0, false
	}
	idx, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, false
	}
	if idx < 0 || fmt.Sprintf("%s_%d", prefix, idx) != pchannel {
		return 0, false
	}
	return idx, true
}
