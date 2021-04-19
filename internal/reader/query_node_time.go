package reader

type QueryNodeTime struct {
	ReadTimeSyncMin Timestamp
	ReadTimeSyncMax Timestamp
	WriteTimeSync   Timestamp
	ServiceTimeSync Timestamp
	TSOTimeSync     Timestamp
}

type TimeRange struct {
	timestampMin Timestamp
	timestampMax Timestamp
}

func (t *QueryNodeTime) updateReadTimeSync() {
	t.ReadTimeSyncMin = t.ReadTimeSyncMax
	// TODO: Add time sync
	t.ReadTimeSyncMax = 1
}

func (t *QueryNodeTime) updateWriteTimeSync() {
	// TODO: Add time sync
	t.WriteTimeSync = 0
}

func (t *QueryNodeTime) updateSearchServiceTime(timeRange TimeRange) {
	t.ServiceTimeSync = timeRange.timestampMax
}

func (t *QueryNodeTime) updateTSOTimeSync() {
	// TODO: Add time sync
	t.TSOTimeSync = 0
}
