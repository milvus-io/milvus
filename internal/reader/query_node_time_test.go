package reader

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueryNodeTime_UpdateReadTimeSync(t *testing.T) {
	queryNodeTimeSync := &QueryNodeTime{
		ReadTimeSyncMin: uint64(0),
		ReadTimeSyncMax: uint64(1),
		WriteTimeSync:   uint64(2),
		ServiceTimeSync: uint64(3),
		TSOTimeSync:     uint64(4),
	}

	queryNodeTimeSync.UpdateReadTimeSync()

	assert.Equal(t, queryNodeTimeSync.ReadTimeSyncMin, uint64(1))
}

func TestQueryNodeTime_UpdateSearchTimeSync(t *testing.T) {
	queryNodeTimeSync := &QueryNodeTime{
		ReadTimeSyncMin: uint64(0),
		ReadTimeSyncMax: uint64(1),
		WriteTimeSync:   uint64(2),
		ServiceTimeSync: uint64(3),
		TSOTimeSync:     uint64(4),
	}

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: 1,
	}
	queryNodeTimeSync.UpdateSearchTimeSync(timeRange)

	assert.Equal(t, queryNodeTimeSync.ServiceTimeSync, uint64(1))
}

func TestQueryNodeTime_UpdateTSOTimeSync(t *testing.T) {
	// TODO: add UpdateTSOTimeSync test
}

func TestQueryNodeTime_UpdateWriteTimeSync(t *testing.T) {
	// TODO: add UpdateWriteTimeSync test
}
