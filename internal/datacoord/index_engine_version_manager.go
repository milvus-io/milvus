package datacoord

import (
	"math"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// IndexEngineVersionManager manages the index engine versions reported by all QueryNodes in the cluster.
//
// Each QueryNode registers its supported index version range [MinimalIndexVersion, CurrentIndexVersion]
// in its session. This manager aggregates versions from all QNs to determine cluster-wide compatibility:
//
//   - GetCurrent*Version(): Returns MIN of all QNs' CurrentIndexVersion.
//     This is the highest version that ALL QueryNodes can load.
//     Used when building new indexes to ensure all QNs can load them (rolling upgrade safe).
//
//   - GetMinimal*Version(): Returns MAX of all QNs' MinimalIndexVersion.
//     This is the lowest version that ANY QueryNode requires.
//     Indexes below this version may fail to load on some QNs.
//     TODO: This is not currently used in the codebase, could be used to check if the index is of too old to
//     load on any query nodes.
//
// Vector index versions come from knowhere library, while scalar index versions are defined by Milvus.
type IndexEngineVersionManager interface {
	Startup(sessions map[string]*sessionutil.Session)
	AddNode(session *sessionutil.Session)
	RemoveNode(session *sessionutil.Session)
	Update(session *sessionutil.Session)
	StartupByRole(role string, sessions map[string]*sessionutil.Session)
	AddByRole(role string, session *sessionutil.Session)
	RemoveByRole(role string, session *sessionutil.Session)
	UpdateByRole(role string, session *sessionutil.Session)
	GetClusterMinIndexStorePathVersion() indexpb.IndexStorePathVersion

	// Vector index version methods (from knowhere library)
	GetCurrentIndexEngineVersion() int32
	GetMinimalIndexEngineVersion() int32

	// Maximum version methods
	GetMaximumIndexEngineVersion() int32
	GetMaximumScalarIndexEngineVersion() int32

	// Scalar index version methods (Milvus-defined)
	GetCurrentScalarIndexEngineVersion() int32
	GetMinimalScalarIndexEngineVersion() int32

	// Resolve methods: compute final build version considering target override and max clamp
	ResolveVecIndexVersion() int32
	ResolveScalarIndexVersion() int32

	GetIndexNonEncoding() bool

	GetMinimalSessionVer() semver.Version
}

type versionManagerImpl struct {
	mu                  lock.Mutex
	versions            map[int64]sessionutil.IndexEngineVersion
	scalarIndexVersions map[int64]sessionutil.IndexEngineVersion
	indexNonEncoding    map[int64]bool
	sessionVersion      map[int64]semver.Version

	indexStorePathVersionByRole map[string]map[int64]int32
}

func newIndexEngineVersionManager() IndexEngineVersionManager {
	return &versionManagerImpl{
		versions:            map[int64]sessionutil.IndexEngineVersion{},
		scalarIndexVersions: map[int64]sessionutil.IndexEngineVersion{},
		indexNonEncoding:    map[int64]bool{},
		sessionVersion:      map[int64]semver.Version{},
		indexStorePathVersionByRole: map[string]map[int64]int32{
			typeutil.QueryNodeRole: {},
		},
	}
}

func (m *versionManagerImpl) Startup(sessions map[string]*sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessionMap := lo.MapKeys(sessions, func(session *sessionutil.Session, _ string) int64 {
		return session.ServerID
	})

	// clean offline nodes
	for sessionID := range m.versions {
		if _, ok := sessionMap[sessionID]; !ok {
			m.removeNodeByID(sessionID)
		}
	}

	// deal with new online nodes
	for _, session := range sessions {
		m.addOrUpdate(session)
	}

	m.startupByRoleLocked(typeutil.QueryNodeRole, sessions)
}

func (m *versionManagerImpl) AddNode(session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addOrUpdate(session)
	m.addOrUpdatePathVersionLocked(typeutil.QueryNodeRole, session)
}

func (m *versionManagerImpl) RemoveNode(session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.removeNodeByID(session.ServerID)
	m.removePathVersionLocked(typeutil.QueryNodeRole, session)
}

func (m *versionManagerImpl) removeNodeByID(sessionID int64) {
	delete(m.versions, sessionID)
	delete(m.scalarIndexVersions, sessionID)
	delete(m.indexNonEncoding, sessionID)
	delete(m.sessionVersion, sessionID)
}

func (m *versionManagerImpl) Update(session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addOrUpdate(session)
	m.addOrUpdatePathVersionLocked(typeutil.QueryNodeRole, session)
}

func (m *versionManagerImpl) StartupByRole(role string, sessions map[string]*sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startupByRoleLocked(role, sessions)
}

func (m *versionManagerImpl) startupByRoleLocked(role string, sessions map[string]*sessionutil.Session) {
	if !isIndexStorePathVersionRole(role) {
		return
	}

	sessionMap := lo.MapKeys(sessions, func(session *sessionutil.Session, _ string) int64 {
		return session.ServerID
	})
	roleMap := m.ensurePathVersionRoleMap(role)
	for sessionID := range roleMap {
		if _, ok := sessionMap[sessionID]; !ok {
			delete(roleMap, sessionID)
		}
	}
	for _, session := range sessions {
		m.addOrUpdatePathVersionLocked(role, session)
	}
}

func (m *versionManagerImpl) AddByRole(role string, session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addOrUpdatePathVersionLocked(role, session)
}

func (m *versionManagerImpl) RemoveByRole(role string, session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.removePathVersionLocked(role, session)
}

func (m *versionManagerImpl) UpdateByRole(role string, session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addOrUpdatePathVersionLocked(role, session)
}

func (m *versionManagerImpl) addOrUpdatePathVersionLocked(role string, session *sessionutil.Session) {
	if !isIndexStorePathVersionRole(role) || session == nil {
		return
	}
	m.ensurePathVersionRoleMap(role)[session.ServerID] = session.MaxIndexStorePathVersion
}

func (m *versionManagerImpl) removePathVersionLocked(role string, session *sessionutil.Session) {
	if !isIndexStorePathVersionRole(role) || session == nil {
		return
	}
	delete(m.ensurePathVersionRoleMap(role), session.ServerID)
}

func (m *versionManagerImpl) ensurePathVersionRoleMap(role string) map[int64]int32 {
	if m.indexStorePathVersionByRole == nil {
		m.indexStorePathVersionByRole = map[string]map[int64]int32{}
	}
	if _, ok := m.indexStorePathVersionByRole[role]; !ok {
		m.indexStorePathVersionByRole[role] = map[int64]int32{}
	}
	return m.indexStorePathVersionByRole[role]
}

func isIndexStorePathVersionRole(role string) bool {
	return role == typeutil.QueryNodeRole
}

func (m *versionManagerImpl) GetClusterMinIndexStorePathVersion() indexpb.IndexStorePathVersion {
	m.mu.Lock()
	defer m.mu.Unlock()

	minVersion := int32(math.MaxInt32)
	hasSession := false
	for _, version := range m.ensurePathVersionRoleMap(typeutil.QueryNodeRole) {
		hasSession = true
		if version < minVersion {
			minVersion = version
		}
	}
	if !hasSession {
		return indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
	}
	return indexpb.IndexStorePathVersion(minVersion)
}

func (m *versionManagerImpl) addOrUpdate(session *sessionutil.Session) {
	log.Info("addOrUpdate version", zap.Int64("nodeId", session.ServerID),
		zap.String("sessionVersion", session.Version.String()),
		zap.Int32("minimal", session.IndexEngineVersion.MinimalIndexVersion),
		zap.Int32("current", session.IndexEngineVersion.CurrentIndexVersion),
		zap.Int32("maximum", session.IndexEngineVersion.MaximumIndexVersion),
		zap.Int32("currentScalar", session.ScalarIndexEngineVersion.CurrentIndexVersion),
		zap.Int32("maximumScalar", session.ScalarIndexEngineVersion.MaximumIndexVersion))
	m.versions[session.ServerID] = session.IndexEngineVersion
	m.scalarIndexVersions[session.ServerID] = session.ScalarIndexEngineVersion
	m.indexNonEncoding[session.ServerID] = session.IndexNonEncoding
	m.sessionVersion[session.ServerID] = session.Version
}

func (m *versionManagerImpl) GetCurrentIndexEngineVersion() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getCurrentVersion()
}

func (m *versionManagerImpl) getCurrentVersion() int32 {
	if len(m.versions) == 0 {
		return 0
	}

	current := int32(math.MaxInt32)
	for _, version := range m.versions {
		if version.CurrentIndexVersion < current {
			current = version.CurrentIndexVersion
		}
	}
	return current
}

func (m *versionManagerImpl) GetMinimalIndexEngineVersion() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getMinimalVersion()
}

func (m *versionManagerImpl) getMinimalVersion() int32 {
	if len(m.versions) == 0 {
		return 0
	}

	minimal := int32(0)
	for _, version := range m.versions {
		if version.MinimalIndexVersion > minimal {
			minimal = version.MinimalIndexVersion
		}
	}
	return minimal
}

func (m *versionManagerImpl) GetCurrentScalarIndexEngineVersion() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getCurrentScalarVersion()
}

func (m *versionManagerImpl) getCurrentScalarVersion() int32 {
	if len(m.scalarIndexVersions) == 0 {
		return 0
	}

	current := int32(math.MaxInt32)
	for _, version := range m.scalarIndexVersions {
		if version.CurrentIndexVersion < current {
			current = version.CurrentIndexVersion
		}
	}
	return current
}

func (m *versionManagerImpl) GetMinimalScalarIndexEngineVersion() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getMinimalScalarVersion()
}

func (m *versionManagerImpl) getMinimalScalarVersion() int32 {
	if len(m.scalarIndexVersions) == 0 {
		return 0
	}

	minimal := int32(0)
	for _, version := range m.scalarIndexVersions {
		if version.MinimalIndexVersion > minimal {
			minimal = version.MinimalIndexVersion
		}
	}
	return minimal
}

func (m *versionManagerImpl) GetMaximumIndexEngineVersion() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getMaximumVersion()
}

func (m *versionManagerImpl) getMaximumVersion() int32 {
	if len(m.versions) == 0 {
		return math.MaxInt32
	}

	maximum := int32(math.MaxInt32)
	for _, version := range m.versions {
		if version.MaximumIndexVersion == 0 {
			continue
		}
		if version.MaximumIndexVersion < maximum {
			maximum = version.MaximumIndexVersion
		}
	}
	return maximum
}

func (m *versionManagerImpl) GetMaximumScalarIndexEngineVersion() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getMaximumScalarVersion()
}

func (m *versionManagerImpl) getMaximumScalarVersion() int32 {
	if len(m.scalarIndexVersions) == 0 {
		return math.MaxInt32
	}

	maximum := int32(math.MaxInt32)
	for _, version := range m.scalarIndexVersions {
		if version.MaximumIndexVersion == 0 {
			continue
		}
		if version.MaximumIndexVersion < maximum {
			maximum = version.MaximumIndexVersion
		}
	}
	return maximum
}

// clampVersion clamps v into [minV, maxV], logging a rate-limited warning on each adjustment.
func clampVersion(v, minV, maxV int32, name string) int32 {
	if v < minV {
		log.RatedWarn(60, name+" below cluster minimum, clamping",
			zap.Int32("target", v), zap.Int32("minimum", minV))
		v = minV
	}
	if v > maxV {
		log.RatedWarn(60, name+" exceeds cluster maximum, clamping",
			zap.Int32("target", v), zap.Int32("maximum", maxV))
		v = maxV
	}
	return v
}

func (m *versionManagerImpl) ResolveVecIndexVersion() int32 {
	m.mu.Lock()
	current, minimal, maximum := m.getCurrentVersion(), m.getMinimalVersion(), m.getMaximumVersion()
	m.mu.Unlock()

	version := current
	if Params.DataCoordCfg.TargetVecIndexVersion.GetAsInt64() != -1 {
		target := Params.DataCoordCfg.TargetVecIndexVersion.GetAsInt32()
		if Params.DataCoordCfg.ForceRebuildSegmentIndex.GetAsBool() {
			version = target
		} else {
			version = max(version, target)
		}
	}
	return clampVersion(version, minimal, maximum, "targetVecIndexVersion")
}

func (m *versionManagerImpl) ResolveScalarIndexVersion() int32 {
	m.mu.Lock()
	current, minimal, maximum := m.getCurrentScalarVersion(), m.getMinimalScalarVersion(), m.getMaximumScalarVersion()
	m.mu.Unlock()

	version := current
	if Params.DataCoordCfg.TargetScalarIndexVersion.GetAsInt64() != -1 {
		target := Params.DataCoordCfg.TargetScalarIndexVersion.GetAsInt32()
		if Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.GetAsBool() {
			version = target
		} else {
			version = max(version, target)
		}
	}
	return clampVersion(version, minimal, maximum, "targetScalarIndexVersion")
}

func (m *versionManagerImpl) GetIndexNonEncoding() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.indexNonEncoding) == 0 {
		log.Info("indexNonEncoding map is empty")
		// by default, we fall back to old index format for safety
		return false
	}
	noneEncoding := true
	for _, encoding := range m.indexNonEncoding {
		noneEncoding = noneEncoding && encoding
	}
	return noneEncoding
}

func (m *versionManagerImpl) GetMinimalSessionVer() semver.Version {
	m.mu.Lock()
	defer m.mu.Unlock()

	minVer := semver.Version{}
	first := true
	for _, version := range m.sessionVersion {
		if first {
			minVer = version
			first = false
		} else if version.LT(minVer) {
			minVer = version
		}
	}
	return minVer
}
