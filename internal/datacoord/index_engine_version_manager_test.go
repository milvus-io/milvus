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

func Test_IndexEngineVersionManager_GetIndexNoneEncoding(t *testing.T) {
	m := newIndexEngineVersionManager()

	// empty
	assert.False(t, m.GetIndexNonEncoding())

	// startup
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 0},
				IndexNonEncoding:         false,
			},
		},
	})
	assert.False(t, m.GetIndexNonEncoding())

	// add node
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:                 2,
			ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 10, MinimalIndexVersion: 5},
			IndexNonEncoding:         true,
		},
	})
	// server1 is still use int8 encoding, the global index encoding must be int8
	assert.False(t, m.GetIndexNonEncoding())

	// update
	m.Update(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:                 2,
			ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 2},
			IndexNonEncoding:         true,
		},
	})
	assert.False(t, m.GetIndexNonEncoding())

	// remove
	m.RemoveNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           1,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 5, MinimalIndexVersion: 3},
		},
	})
	// after removing server1, then global none encoding should be true
	assert.True(t, m.GetIndexNonEncoding())
}

func Test_IndexEngineVersionManager_StartupWithOfflineNodeCleanup(t *testing.T) {
	m := newIndexEngineVersionManager()

	// First startup with initial nodes
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 10},
			},
		},
		"2": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           2,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 15, MinimalIndexVersion: 5},
			},
		},
	})

	// Verify both nodes are present
	assert.Equal(t, int32(15), m.GetCurrentIndexEngineVersion()) // min of 20 and 15
	assert.Equal(t, int32(10), m.GetMinimalIndexEngineVersion()) // max of 10 and 5

	// Second startup with only one node online (node 2 is offline)
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 25, MinimalIndexVersion: 12},
			},
		},
	})

	// Verify offline node 2 is cleaned up and only node 1 remains
	assert.Equal(t, int32(25), m.GetCurrentIndexEngineVersion())
	assert.Equal(t, int32(12), m.GetMinimalIndexEngineVersion())

	// Verify that node 2's data is actually removed from internal maps
	vm := m.(*versionManagerImpl)
	_, exists := vm.versions[2]
	assert.False(t, exists, "offline node should be removed from versions map")
	_, exists = vm.scalarIndexVersions[2]
	assert.False(t, exists, "offline node should be removed from scalarIndexVersions map")
	_, exists = vm.indexNonEncoding[2]
	assert.False(t, exists, "offline node should be removed from indexNonEncoding map")
}

func Test_IndexEngineVersionManager_StartupWithNewAndOfflineNodes(t *testing.T) {
	m := newIndexEngineVersionManager()

	// First startup
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 10},
			},
		},
		"2": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           2,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 15, MinimalIndexVersion: 5},
			},
		},
	})

	// Second startup: node 2 offline, node 3 comes online, node 1 still online
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 22, MinimalIndexVersion: 11},
			},
		},
		"3": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           3,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 18, MinimalIndexVersion: 8},
			},
		},
	})

	// Verify node 2 is cleaned up and node 3 is added
	assert.Equal(t, int32(18), m.GetCurrentIndexEngineVersion()) // min of 22 and 18
	assert.Equal(t, int32(11), m.GetMinimalIndexEngineVersion()) // max of 11 and 8

	vm := m.(*versionManagerImpl)
	// Node 1 should still exist
	_, exists := vm.versions[1]
	assert.True(t, exists, "online node 1 should remain")
	// Node 2 should be removed
	_, exists = vm.versions[2]
	assert.False(t, exists, "offline node 2 should be removed")
	// Node 3 should be added
	_, exists = vm.versions[3]
	assert.True(t, exists, "new online node 3 should be added")
}

func Test_IndexEngineVersionManager_StartupWithEmptySession(t *testing.T) {
	m := newIndexEngineVersionManager()

	// First startup with nodes
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 10},
			},
		},
	})

	assert.Equal(t, int32(20), m.GetCurrentIndexEngineVersion())

	// Second startup with no nodes (all offline)
	m.Startup(map[string]*sessionutil.Session{})

	// Should return default values when no nodes are online
	assert.Equal(t, int32(0), m.GetCurrentIndexEngineVersion())
	assert.Equal(t, int32(0), m.GetMinimalIndexEngineVersion())

	vm := m.(*versionManagerImpl)
	assert.Empty(t, vm.versions, "all nodes should be cleaned up")
	assert.Empty(t, vm.scalarIndexVersions, "all nodes should be cleaned up")
	assert.Empty(t, vm.indexNonEncoding, "all nodes should be cleaned up")
}

func Test_IndexEngineVersionManager_removeNodeByID(t *testing.T) {
	m := newIndexEngineVersionManager()

	// Add some nodes first
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:                 1,
			IndexEngineVersion:       sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MinimalIndexVersion: 10},
			ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 15, MinimalIndexVersion: 5},
			IndexNonEncoding:         true,
		},
	})

	vm := m.(*versionManagerImpl)

	// Verify node is added
	_, exists := vm.versions[1]
	assert.True(t, exists)
	_, exists = vm.scalarIndexVersions[1]
	assert.True(t, exists)
	_, exists = vm.indexNonEncoding[1]
	assert.True(t, exists)

	// Remove node by ID
	vm.removeNodeByID(1)

	// Verify node is completely removed
	_, exists = vm.versions[1]
	assert.False(t, exists, "node should be removed from versions map")
	_, exists = vm.scalarIndexVersions[1]
	assert.False(t, exists, "node should be removed from scalarIndexVersions map")
	_, exists = vm.indexNonEncoding[1]
	assert.False(t, exists, "node should be removed from indexNonEncoding map")
}
