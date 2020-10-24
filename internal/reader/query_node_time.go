package reader

type QueryNodeTime struct {
	ReadTimeSyncMin uint64
	ReadTimeSyncMax uint64
	WriteTimeSync   uint64
	ServiceTimeSync uint64
	TSOTimeSync     uint64
}

type TimeRange struct {
	timestampMin uint64
	timestampMax uint64
}

func (t *QueryNodeTime) UpdateReadTimeSync() {
	t.ReadTimeSyncMin = t.ReadTimeSyncMax
	// TODO: Add time sync
	t.ReadTimeSyncMax = 1
}

func (t *QueryNodeTime) UpdateWriteTimeSync() {
	// TODO: Add time sync
	t.WriteTimeSync = 0
}

func (t *QueryNodeTime) UpdateSearchTimeSync(timeRange TimeRange) {
	t.ServiceTimeSync = timeRange.timestampMax
}

func (t *QueryNodeTime) UpdateTSOTimeSync() {
	// TODO: Add time sync
	t.TSOTimeSync = 0
}
