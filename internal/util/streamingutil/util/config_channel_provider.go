package util

import (
	"context"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const defaultPollingInterval = 10 * time.Second

// ConfigChannelProvider implements channel.ChannelProvider by polling
// the Milvus configuration for new DML channel names.
type ConfigChannelProvider struct {
	initialChannels []string
	ch              chan []string
	cancel          context.CancelFunc
}

// NewConfigChannelProvider creates a ConfigChannelProvider that reads the
// current set of topics from configuration and starts a background goroutine
// to detect any newly added topics.
func NewConfigChannelProvider() *ConfigChannelProvider {
	currentTopics := GetAllTopicsFromConfiguration()
	initial := currentTopics.Collect()
	sort.Strings(initial)

	ctx, cancel := context.WithCancel(context.Background())
	p := &ConfigChannelProvider{
		initialChannels: initial,
		ch:              make(chan []string, 1),
		cancel:          cancel,
	}
	go p.pollLoop(ctx, currentTopics)
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

// Close stops the background polling goroutine and closes the notification channel.
func (p *ConfigChannelProvider) Close() {
	p.cancel()
}

// pollLoop periodically checks the configuration for new topics and
// sends any newly discovered names on the notification channel.
func (p *ConfigChannelProvider) pollLoop(ctx context.Context, known typeutil.Set[string]) {
	defer close(p.ch)

	ticker := time.NewTicker(defaultPollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := GetAllTopicsFromConfiguration()
			var newChannels []string
			current.Range(func(name string) bool {
				if !known.Contain(name) {
					newChannels = append(newChannels, name)
					known.Insert(name)
				}
				return true
			})
			if len(newChannels) > 0 {
				sort.Strings(newChannels)
				log.Info("ConfigChannelProvider detected new channels",
					zap.Strings("newChannels", newChannels))
				select {
				case p.ch <- newChannels:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}
