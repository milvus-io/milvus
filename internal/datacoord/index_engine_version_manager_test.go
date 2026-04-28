package datacoord

import (
	"math"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func Test_IndexEngineVersionManager_GetMinimalSessionVer(t *testing.T) {
	m := newIndexEngineVersionManager()

	// empty - should return zero version
	assert.Equal(t, semver.Version{}, m.GetMinimalSessionVer())

	// startup with single node
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			Version: semver.MustParse("2.6.0"),
			SessionRaw: sessionutil.SessionRaw{
				ServerID: 1,
			},
		},
	})
	assert.Equal(t, semver.MustParse("2.6.0"), m.GetMinimalSessionVer())

	// add node with lower version - should return the lower one
	m.AddNode(&sessionutil.Session{
		Version: semver.MustParse("2.5.0"),
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 2,
		},
	})
	assert.Equal(t, semver.MustParse("2.5.0"), m.GetMinimalSessionVer())

	// add node with higher version - should still return the lowest
	m.AddNode(&sessionutil.Session{
		Version: semver.MustParse("2.7.0"),
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 3,
		},
	})
	assert.Equal(t, semver.MustParse("2.5.0"), m.GetMinimalSessionVer())

	// update node 2 to higher version - should now return 2.6.0
	m.Update(&sessionutil.Session{
		Version: semver.MustParse("2.8.0"),
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 2,
		},
	})
	assert.Equal(t, semver.MustParse("2.6.0"), m.GetMinimalSessionVer())

	// remove node 1 - should return 2.7.0 (min of 2.8.0 and 2.7.0)
	m.RemoveNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID: 1,
		},
	})
	assert.Equal(t, semver.MustParse("2.7.0"), m.GetMinimalSessionVer())

	// verify sessionVersion map is correctly updated
	vm := m.(*versionManagerImpl)
	_, exists := vm.sessionVersion[1]
	assert.False(t, exists, "removed node should not exist in sessionVersion map")
	_, exists = vm.sessionVersion[2]
	assert.True(t, exists, "node 2 should exist in sessionVersion map")
	_, exists = vm.sessionVersion[3]
	assert.True(t, exists, "node 3 should exist in sessionVersion map")
}

func Test_IndexEngineVersionManager_GetMaximumIndexEngineVersion(t *testing.T) {
	m := newIndexEngineVersionManager()

	// empty - returns MaxInt32 (no upper bound)
	assert.Equal(t, int32(math.MaxInt32), m.GetMaximumIndexEngineVersion())

	// all nodes report Maximum=0 (old QNs) - returns MaxInt32
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 20, MaximumIndexVersion: 0},
			},
		},
	})
	assert.Equal(t, int32(math.MaxInt32), m.GetMaximumIndexEngineVersion())

	// mix of old QN (Max=0) and new QN (Max=30) - skip old, return 30
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           2,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 15, MaximumIndexVersion: 30},
		},
	})
	assert.Equal(t, int32(30), m.GetMaximumIndexEngineVersion())

	// add another new QN with lower Max - returns MIN
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:           3,
			IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 18, MaximumIndexVersion: 25},
		},
	})
	assert.Equal(t, int32(25), m.GetMaximumIndexEngineVersion())

	// remove the node with lower Max - returns 30
	m.RemoveNode(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 3}})
	assert.Equal(t, int32(30), m.GetMaximumIndexEngineVersion())
}

func Test_IndexEngineVersionManager_GetMaximumScalarIndexEngineVersion(t *testing.T) {
	m := newIndexEngineVersionManager()

	// empty - returns MaxInt32
	assert.Equal(t, int32(math.MaxInt32), m.GetMaximumScalarIndexEngineVersion())

	// all nodes report Maximum=0 (old QNs)
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 2, MaximumIndexVersion: 0},
			},
		},
	})
	assert.Equal(t, int32(math.MaxInt32), m.GetMaximumScalarIndexEngineVersion())

	// new QN with Maximum set
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:                 2,
			ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 2, MaximumIndexVersion: 5},
		},
	})
	assert.Equal(t, int32(5), m.GetMaximumScalarIndexEngineVersion())

	// another QN with lower Maximum
	m.AddNode(&sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID:                 3,
			ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 2, MaximumIndexVersion: 3},
		},
	})
	assert.Equal(t, int32(3), m.GetMaximumScalarIndexEngineVersion())
}

func Test_IndexEngineVersionManager_ResolveVecIndexVersion(t *testing.T) {
	paramtable.Init()

	t.Run("no target override", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 10, MaximumIndexVersion: 20},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "-1")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "false")

		assert.Equal(t, int32(10), m.ResolveVecIndexVersion())
	})

	t.Run("target override without force rebuild", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 10, MaximumIndexVersion: 20},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "15")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "false")

		// max(current=10, target=15) = 15
		assert.Equal(t, int32(15), m.ResolveVecIndexVersion())
	})

	t.Run("force rebuild with target in safe range", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 3, CurrentIndexVersion: 10, MaximumIndexVersion: 20},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "5")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "true")

		// force rebuild: target=5 is within [minimal=3, max=20], use directly
		assert.Equal(t, int32(5), m.ResolveVecIndexVersion())
	})

	t.Run("force rebuild with target below cluster minimal - clamped to minimal", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 8, CurrentIndexVersion: 10, MaximumIndexVersion: 20},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "5")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "true")

		// force rebuild: target=5 < clusterMinimal=8, clamped to 8
		assert.Equal(t, int32(8), m.ResolveVecIndexVersion())
	})

	t.Run("target exceeds maximum - clamped", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 3, CurrentIndexVersion: 10, MaximumIndexVersion: 20},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "25")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "true")

		// target=25 > max=20, clamped to 20
		assert.Equal(t, int32(20), m.ResolveVecIndexVersion())
	})

	t.Run("multi-node force rebuild clamped to cluster minimal", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		// QN1: Min=5, QN2: Min=8 => cluster minimal = MAX(5,8) = 8
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 5, CurrentIndexVersion: 15, MaximumIndexVersion: 25},
			},
		})
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           2,
				IndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 8, CurrentIndexVersion: 12, MaximumIndexVersion: 30},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "6")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "true")

		// force rebuild: target=6 < clusterMinimal=8, clamped to 8
		// clusterMax = MIN(25,30) = 25
		assert.Equal(t, int32(8), m.ResolveVecIndexVersion())
	})

	t.Run("target below current without force", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 3, CurrentIndexVersion: 10, MaximumIndexVersion: 20},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "5")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "false")

		// max(current=10, target=5) = 10
		assert.Equal(t, int32(10), m.ResolveVecIndexVersion())
	})

	t.Run("all old QNs - no upper bound check", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:           1,
				IndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 0, CurrentIndexVersion: 10, MaximumIndexVersion: 0},
			},
		})
		Params.Save("dataCoord.targetVecIndexVersion", "15")
		Params.Save("dataCoord.forceRebuildSegmentIndex", "false")

		// old QN (Max=0) => GetMaximum returns MaxInt32, no upper clamp
		assert.Equal(t, int32(15), m.ResolveVecIndexVersion())
	})
}

func Test_IndexEngineVersionManager_ResolveScalarIndexVersion(t *testing.T) {
	paramtable.Init()

	t.Run("no target override", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 2, MaximumIndexVersion: 5},
			},
		})
		Params.Save("dataCoord.targetScalarIndexVersion", "-1")
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "false")

		assert.Equal(t, int32(2), m.ResolveScalarIndexVersion())
	})

	t.Run("target override without force rebuild", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{CurrentIndexVersion: 2, MaximumIndexVersion: 5},
			},
		})
		Params.Save("dataCoord.targetScalarIndexVersion", "3")
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "false")

		// max(current=2, target=3) = 3
		assert.Equal(t, int32(3), m.ResolveScalarIndexVersion())
	})

	t.Run("force rebuild with target in safe range", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 1, CurrentIndexVersion: 3, MaximumIndexVersion: 5},
			},
		})
		Params.Save("dataCoord.targetScalarIndexVersion", "2")
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "true")

		// force rebuild: target=2 is within [minimal=1, max=5], use directly
		assert.Equal(t, int32(2), m.ResolveScalarIndexVersion())
	})

	t.Run("force rebuild with target below cluster minimal - clamped to minimal", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 2, CurrentIndexVersion: 3, MaximumIndexVersion: 5},
			},
		})
		Params.Save("dataCoord.targetScalarIndexVersion", "1")
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "true")

		// force rebuild: target=1 < clusterMinimal=2, clamped to 2
		assert.Equal(t, int32(2), m.ResolveScalarIndexVersion())
	})

	t.Run("target exceeds maximum - clamped", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 1, CurrentIndexVersion: 2, MaximumIndexVersion: 5},
			},
		})
		Params.Save("dataCoord.targetScalarIndexVersion", "10")
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "true")

		// target=10 > max=5, clamped to 5
		assert.Equal(t, int32(5), m.ResolveScalarIndexVersion())
	})

	t.Run("multi-node force rebuild clamped to cluster minimal", func(t *testing.T) {
		m := newIndexEngineVersionManager()
		// QN1: Min=1, QN2: Min=2 => cluster minimal = MAX(1,2) = 2
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 1,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 1, CurrentIndexVersion: 3, MaximumIndexVersion: 5},
			},
		})
		m.AddNode(&sessionutil.Session{
			SessionRaw: sessionutil.SessionRaw{
				ServerID:                 2,
				ScalarIndexEngineVersion: sessionutil.IndexEngineVersion{MinimalIndexVersion: 2, CurrentIndexVersion: 4, MaximumIndexVersion: 6},
			},
		})
		Params.Save("dataCoord.targetScalarIndexVersion", "1")
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "true")

		// force rebuild: target=1 < clusterMinimal=2, clamped to 2
		// clusterCurrent = MIN(3,4) = 3, clusterMax = MIN(5,6) = 5
		assert.Equal(t, int32(2), m.ResolveScalarIndexVersion())
	})
}

func Test_IndexEngineVersionManager_SessionVersionCleanupOnStartup(t *testing.T) {
	m := newIndexEngineVersionManager()

	// First startup with initial nodes
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			Version: semver.MustParse("2.6.0"),
			SessionRaw: sessionutil.SessionRaw{
				ServerID: 1,
			},
		},
		"2": {
			Version: semver.MustParse("2.5.0"),
			SessionRaw: sessionutil.SessionRaw{
				ServerID: 2,
			},
		},
	})
	assert.Equal(t, semver.MustParse("2.5.0"), m.GetMinimalSessionVer())

	// Second startup with only node 1 online (node 2 is offline)
	m.Startup(map[string]*sessionutil.Session{
		"1": {
			Version: semver.MustParse("2.6.5"),
			SessionRaw: sessionutil.SessionRaw{
				ServerID: 1,
			},
		},
	})

	// Verify offline node 2 is cleaned up
	assert.Equal(t, semver.MustParse("2.6.5"), m.GetMinimalSessionVer())

	vm := m.(*versionManagerImpl)
	_, exists := vm.sessionVersion[2]
	assert.False(t, exists, "offline node should be removed from sessionVersion map")
}
