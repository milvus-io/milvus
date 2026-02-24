package util

import (
	"sort"

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
}

// NewConfigChannelProvider creates a ConfigChannelProvider that reads the
// current set of topics from configuration and watches for config changes
// to detect any newly added topics.
func NewConfigChannelProvider() *ConfigChannelProvider {
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
	for {
		select {
		case <-p.trigger:
			p.onConfigChange()
		case <-p.notifier.Context().Done():
			return
		}
	}
}

func (p *ConfigChannelProvider) onConfigChange() {
	current := GetAllTopicsFromConfiguration()
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
		log.Info("ConfigChannelProvider detected new channels",
			zap.Strings("newChannels", newChannels))
		select {
		case p.ch <- newChannels:
		case <-p.notifier.Context().Done():
		}
	}
}
