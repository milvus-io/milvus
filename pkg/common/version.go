package common

import semver "github.com/blang/semver/v4"

// Version current version for session
var Version semver.Version

// MinVersionForV3StatsAttemptPath is the first Milvus version whose QueryNode can
// load V3 Text/JSON stats from task-attempt subdirectories. It protects the
// dormant writer-side capability as well; DataCoord intentionally leaves that
// capability disabled until rolling upgrades from every supported source
// version have completed.
var MinVersionForV3StatsAttemptPath = semver.MustParse("3.0.2")

func init() {
	Version = semver.MustParse("3.0.0-beta")
}
