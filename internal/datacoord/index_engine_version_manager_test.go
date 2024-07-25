package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func Test_IndexEngineVersionManager_GetMergedIndexVersion(t *testing.T) {
	m := newIndexEngineVersionManager()

	// empty
	curV, err := m.GetCurrentIndexEngineVersion()
	assert.Error(t, err)
	assert.Zero(t, curV)

	// startup
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 0},
			},
		},
	})

	curV, err = m.GetCurrentIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(20), curV)

	minV, err := m.GetMinimalIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(0), minV)

	// add node
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 10, MinimalIndexVersion: 5},
		},
	})

	curV, err = m.GetCurrentIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(10), curV)

	minV, err = m.GetMinimalIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(5), minV)

	// update
	m.Update(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 2},
		},
	})

	curV, err = m.GetCurrentIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(5), curV)

	minV, err = m.GetMinimalIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(2), minV)

	// remove
	m.RemoveNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 3},
		},
	})

	curV, err = m.GetCurrentIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(20), curV)

	minV, err = m.GetMinimalIndexEngineVersion()
	assert.NoError(t, err)
	assert.Equal(t, int32(0), minV)
}
