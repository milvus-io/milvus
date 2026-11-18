package dataview

import (
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// DataViewRetention returns each VChannel's exclusive Transform start on the
// requested PChannel; it does not aggregate unrelated channels.
// F never decreases across DataVersions, so only the oldest retained version
// of each collection is needed. Include temporary tombstones: catalog deletion
// can still fail and make those versions acquirable again.
func (m *dataViewManager) DataViewRetention(pchannel string) map[string]uint64 {
	m.mu.RLock()
	states := make([]*collectionState, 0, len(m.states))
	for _, state := range m.states {
		states = append(states, state)
	}
	m.mu.RUnlock()

	frontiers := make(map[string]uint64)
	for _, state := range states {
		state.mu.Lock()
		var oldest *versionEntry
		for _, entry := range state.versions {
			if oldest == nil || compareDataVersion(entry.view.GetDataVersion(), oldest.view.GetDataVersion()) < 0 {
				oldest = entry
			}
		}
		if oldest == nil {
			// No snapshot yet. The caller keeps the channel's local origin pin.
			state.mu.Unlock()
			continue
		}
		for _, shard := range oldest.view.GetShards() {
			if funcutil.ToPhysicalChannel(shard.GetVchannel()) == pchannel {
				frontiers[shard.GetVchannel()] = transformStart(shard)
			}
		}
		state.mu.Unlock()
	}
	return frontiers
}
