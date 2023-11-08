package common

import semver "github.com/blang/semver/v4"

// Version current version for session
var Version semver.Version

func init() {
	Version, _ = semver.Parse("2.3.3")
}
