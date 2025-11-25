package common

import semver "github.com/blang/semver/v4"

const (
	VersionChecker260 = "<2.6.0-dev"
	VersionChecker266 = "<2.6.6-dev"
)

// Version current version for session
var Version semver.Version

func init() {
	Version = semver.MustParse("2.6.6")
}
