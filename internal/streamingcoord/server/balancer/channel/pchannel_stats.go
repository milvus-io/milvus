package channel

import (
	"sync"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var StaticPChannelStatsManager = syncutil.NewFuture[*PchannelStatsManager]()

// RecoverPChannelStatsManager recovers the pchannel stats manager.
func RecoverPChannelStatsManager(vchannels []string) {
	m := &PchannelStatsManager{
		mu:    sync.Mutex{},
		n:     syncutil.NewVersionedNotifier(),
		stats: make(map[ChannelID]*pchannelStats),
	}
	m.AddVChannel(vchannels...)
	StaticPChannelStatsManager.Set(m)
}

// PchannelStatsManager is the manager of pchannel stats.
// TODO: Bad implementation here, because current vchannel info is not fully managed by streaming service.
// It's a temporary solution to manage the vchannel info in the pchannel stats for balancing, should be refactored in future.
type PchannelStatsManager struct {
	mu    sync.Mutex
	n     *syncutil.VersionedNotifier
	stats map[ChannelID]*pchannelStats
}

// WatchAtChannelCountChanged returns a channel that will be notified when the channel count changed.
func (pm *PchannelStatsManager) WatchAtChannelCountChanged() *syncutil.VersionedListener {
	return pm.n.Listen(syncutil.VersionedListenAtEarliest)
}

// GetPChannelStats returns the stats of the pchannel.
func (pm *PchannelStatsManager) GetPChannelStats(channelID ChannelID) *pchannelStats {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.stats[channelID]; !ok {
		pm.stats[channelID] = &pchannelStats{
			mu:        sync.Mutex{},
			vchannels: make(map[string]int64),
		}
	}
	return pm.stats[channelID]
}

// AddVChannel adds a vchannel to the pchannel.
func (pm *PchannelStatsManager) AddVChannel(vchannels ...string) {
	for _, vchannel := range vchannels {
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		p := pm.GetPChannelStats(types.ChannelID{
			Name: pchannel,
		})
		p.AddVChannel(vchannel)
	}
	pm.n.NotifyAll()
}

// RemoveVChannel removes a vchannel from the pchannel.
func (pm *PchannelStatsManager) RemoveVChannel(vchannels ...string) {
	for _, vchannel := range vchannels {
		pchannel := funcutil.ToPhysicalChannel(vchannel)
		p := pm.GetPChannelStats(types.ChannelID{
			Name: pchannel,
		})
		p.RemoveVChannel(vchannel)
	}
	pm.n.NotifyAll()
}

// pchannelStats is the stats of the pchannel.
type pchannelStats struct {
	mu        sync.Mutex
	vchannels map[string]int64 // indicate how much vchannel is available at current pchannel.
}

// VChannelCount returns the count of vchannel in the pchannel.
func (s *pchannelStats) VChannelCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.vchannels)
}

// AddVChannel adds a vchannel to the pchannel.
func (s *pchannelStats) AddVChannel(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.vchannels == nil {
		s.vchannels = make(map[string]int64)
	}
	s.vchannels[name] = funcutil.GetCollectionIDFromVChannel(name)
}

// RemoveVChannel removes a vchannel from the pchannel.
func (s *pchannelStats) RemoveVChannel(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vchannels, name)
}

// View returns the View of the pchannel stats.
func (s *pchannelStats) View() PChannelStatsView {
	s.mu.Lock()
	defer s.mu.Unlock()
	vchannels := make(map[string]int64, len(s.vchannels))
	for k, v := range s.vchannels {
		vchannels[k] = v
	}
	return PChannelStatsView{
		VChannels: vchannels,
	}
}
