package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func Test_IndexEngineVersionManager_GetMergedIndexVersion(t *testing.T) {
	m := newIndexEngineVersionManager()

	// empty
	assert.Zero(t, m.GetCurrentIndexEngineVersion())

	// startup
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 0},
			},
		},
	})
	assert.Equal(t, int32(20), m.GetCurrentIndexEngineVersion())
	assert.Equal(t, int32(0), m.GetMinimalIndexEngineVersion())

	// add node
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 10, MinimalIndexVersion: 5},
		},
	})
	assert.Equal(t, int32(10), m.GetCurrentIndexEngineVersion())
	assert.Equal(t, int32(5), m.GetMinimalIndexEngineVersion())

	// update
	m.Update(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 2},
		},
	})
	assert.Equal(t, int32(5), m.GetCurrentIndexEngineVersion())
	assert.Equal(t, int32(2), m.GetMinimalIndexEngineVersion())

	// remove
	m.RemoveNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 3},
		},
	})
	assert.Equal(t, int32(20), m.GetCurrentIndexEngineVersion())
	assert.Equal(t, int32(0), m.GetMinimalIndexEngineVersion())
}

func Test_IndexEngineVersionManager_GetMergedScalarIndexVersion(t *testing.T) {
	m := newIndexEngineVersionManager()

	// empty
	assert.Zero(t, m.GetCurrentScalarIndexEngineVersion())

	// startup
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 0},
			},
		},
	})
	assert.Equal(t, int32(20), m.GetCurrentScalarIndexEngineVersion())
	assert.Equal(t, int32(0), m.GetMinimalScalarIndexEngineVersion())

	// add node
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:                 2,
			ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 10, MinimalIndexVersion: 5},
		},
	})
	assert.Equal(t, int32(10), m.GetCurrentScalarIndexEngineVersion())
	assert.Equal(t, int32(5), m.GetMinimalScalarIndexEngineVersion())

	// update
	m.Update(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:                 2,
			ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 2},
		},
	})
	assert.Equal(t, int32(5), m.GetCurrentScalarIndexEngineVersion())
	assert.Equal(t, int32(2), m.GetMinimalScalarIndexEngineVersion())

	// remove
	m.RemoveNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 3},
		},
	})
	assert.Equal(t, int32(20), m.GetCurrentScalarIndexEngineVersion())
	assert.Equal(t, int32(0), m.GetMinimalScalarIndexEngineVersion())
}
