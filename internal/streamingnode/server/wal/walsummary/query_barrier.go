package walsummary

// ObserveQueryBarrier exposes volatile coverage for one VChannel. The caller
// must have observed an ordered WAL prefix through timetick, including all
// preceding Summary records. This is not permission to seal, acknowledge,
// materialize or discard records, and is deliberately absent from recovery.
func (m *Manager) ObserveQueryBarrier(vchannel string, timetick uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if timetick <= m.queryBarriers[vchannel] {
		return
	}
	if m.queryBarriers == nil {
		m.queryBarriers = make(map[string]uint64)
	}
	m.queryBarriers[vchannel] = timetick
	// Bounded SN bootstrap readers use global change tokens; continuous QN
	// subscriptions use scoped tokens, so unrelated VChannels stay asleep.
	m.notifyReadersLocked()
	m.notifyTransformLocked(vchannel)
}
