package datacoord

import (
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
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
}

func newIndexEngineVersionManager() IndexEngineVersionManager {
	return &versionManagerImpl{
		versions:            map[int64]sessionutil.IndexEngineVersion{},
		scalarIndexVersions: map[int64]sessionutil.IndexEngineVersion{},
		indexNonEncoding:    map[int64]bool{},
		sessionVersion:      map[int64]semver.Version{},
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
}

func (m *versionManagerImpl) AddNode(session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addOrUpdate(session)
}

func (m *versionManagerImpl) RemoveNode(session *sessionutil.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.removeNodeByID(session.ServerID)
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
}

// configuredIndexStorePathVersion parses dataCoord.index.storePathVersion. Only the two layouts
// the enum defines are accepted; anything else (including a malformed value, which the paramtable
// getters silently coerce to 0) falls back to the legacy layout and is logged, so an operator typo
// is visible instead of being read as an opt-in.
func configuredIndexStorePathVersion() indexpb.IndexStorePathVersion {
	raw := Params.DataCoordCfg.IndexStorePathVersion.GetValue()
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err == nil {
		switch version := indexpb.IndexStorePathVersion(parsed); version {
		case indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED,
			indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED:
			return version
		}
	}
	mlog.RatedWarn(context.TODO(), rate.Limit(60), "unsupported dataCoord.index.storePathVersion, falling back to the legacy index layout",
		mlog.String("value", raw))
	return indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
}

// GetClusterMinIndexStorePathVersion returns the index file layout to use for new index builds.
//
// COLLECTION_ROOTED requires BOTH:
//   - the operator to opt in via dataCoord.index.storePathVersion, because a binary older than
//     this one cannot read that layout and the opt-in is what gives up rollback compatibility;
//   - no QueryNode to still report an older release line, because QueryNodes rebuild the remote
//     index prefix themselves (storage/FileManager.h GetRemoteIndexObjectPrefix), so an older one
//     would look for the files under the legacy layout. The comparison below is against the
//     version each QueryNode publishes in its session, which is the compile-time common.Version
//     constant, so it only separates release lines (2.6.x vs 3.0.x): two binaries on the same
//     line report the identical version and cannot be told apart here.
//
// Falling back to BUILD_ROOTED is always safe: the layout is recorded per SegmentIndex, so
// records built earlier keep being read and GC'd under the layout they were built with.
func (m *versionManagerImpl) GetClusterMinIndexStorePathVersion() indexpb.IndexStorePathVersion {
	if configuredIndexStorePathVersion() != indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED {
		return indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.sessionVersion) == 0 {
		return indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
	}
	for _, version := range m.sessionVersion {
		if version.LT(common.Version) {
			return indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED
		}
	}
	return indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED
}

func (m *versionManagerImpl) addOrUpdate(session *sessionutil.Session) {
	mlog.Info(context.TODO(), "addOrUpdate version", mlog.Int64("nodeId", session.ServerID),
		mlog.String("sessionVersion", session.Version.String()),
		mlog.Int32("minimal", session.IndexEngineVersion.MinimalIndexVersion),
		mlog.Int32("current", session.IndexEngineVersion.CurrentIndexVersion),
		mlog.Int32("maximum", session.IndexEngineVersion.MaximumIndexVersion),
		mlog.Int32("currentScalar", session.ScalarIndexEngineVersion.CurrentIndexVersion),
		mlog.Int32("maximumScalar", session.ScalarIndexEngineVersion.MaximumIndexVersion))
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
		return noSessionVersion(segcore.GetIndexEngineInfo().CurrentIndexVersion)
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
		return noSessionVersion(common.CurrentScalarIndexEngineVersion)
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
	return getMaximumVersionFrom(m.versions, segcore.GetIndexEngineInfo().CurrentIndexVersion)
}

func (m *versionManagerImpl) GetMaximumScalarIndexEngineVersion() int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getMaximumScalarVersion()
}

func (m *versionManagerImpl) getMaximumScalarVersion() int32 {
	return getMaximumVersionFrom(m.scalarIndexVersions, common.CurrentScalarIndexEngineVersion)
}

// noSessionVersion is the current index engine version with no QueryNode
// session registered, given the version compiled into this coordinator.
//
// A stock binary answers 0, as it always has. The current version is the MIN
// over every QueryNode's, the highest version all of them can load, and with
// none registered there is nothing to take a minimum of: datacoord coming up
// before any QueryNode is ordinary during a full restart or a rolling upgrade,
// and an index or compaction output built in that window with this
// coordinator's own version could not be loaded by an older QueryNode that
// registers afterwards. Zero is the answer that assumes nothing.
//
// With a form installed (extension.FormInstalled) the answer is the version
// compiled into the coordinator. Such a deployment rolls every role from one
// image, so a QueryNode started later runs this same version, and it loads
// collections on demand, where version 0 costs it something: knowhere reads 0
// as "only DISKANN loads off disk" and misroutes other disk indexes onto the
// in-memory path. The same assumption sets the ceiling with no session
// (getMaximumVersionFrom), so an operator override above it is clamped rather
// than written into index builds nothing can load. If the assumption is wrong
// - a QueryNode on an older image joins - its session replaces both figures
// the moment it registers, and the answers become cluster-wide again.
func noSessionVersion(compiledIn int32) int32 {
	if extension.FormInstalled() {
		return compiledIn
	}
	return 0
}

// getMaximumVersionFrom returns the highest index version every registered
// QueryNode can load. compiledIn is the answer when none is registered and a
// form is installed: the version this coordinator's own image can load. A
// stock binary keeps the unbounded MaxInt32 it always had.
//
// The bound is the same assumption noSessionVersion makes with no session -- a
// QueryNode started later runs this image -- and it has a consequence worth
// stating, because it is the whole point of returning it here. This is the
// ceiling clampVersion applies to dataCoord.targetVecIndexVersion and
// dataCoord.targetScalarIndexVersion, so an operator override above what this
// image can load is clamped down to it (with the rate-limited warning) instead
// of being written into index builds unchecked. An unbounded MaxInt32 makes
// the clamp a no-op precisely when there is no QueryNode to disprove the
// override, which is what a stock binary does and keeps doing.
func getMaximumVersionFrom(versions map[int64]sessionutil.IndexEngineVersion, compiledIn int32) int32 {
	if len(versions) == 0 {
		if extension.FormInstalled() {
			return compiledIn
		}
		return math.MaxInt32
	}

	maximum := int32(math.MaxInt32)
	for _, version := range versions {
		// Old QueryNodes do not report MaximumIndexVersion. In that case, use
		// CurrentIndexVersion as the conservative upper bound; maxVersion should
		// never be lower than the current version that the node already supports.
		maxVersion := max(version.CurrentIndexVersion, version.MaximumIndexVersion)
		if maxVersion == 0 {
			continue
		}
		if maxVersion < maximum {
			maximum = maxVersion
		}
	}
	return maximum
}

// clampVersion clamps v into [minV, maxV], logging a rate-limited warning on each adjustment.
func clampVersion(v, minV, maxV int32, name string) int32 {
	if v < minV {
		mlog.RatedWarn(context.TODO(), rate.Limit(60), name+" below cluster minimum, clamping",
			mlog.Int32("target", v), mlog.Int32("minimum", minV))
		v = minV
	}
	if v > maxV {
		mlog.RatedWarn(context.TODO(), rate.Limit(60), name+" exceeds cluster maximum, clamping",
			mlog.Int32("target", v), mlog.Int32("maximum", maxV))
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
		mlog.Info(context.TODO(), "indexNonEncoding map is empty")
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
