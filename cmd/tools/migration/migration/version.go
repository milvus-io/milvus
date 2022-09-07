package migration

import "github.com/blang/semver/v4"

const (
	version210Str = "2.1.0"
	version220Str = "2.2.0"
)

var version210, version220 semver.Version

func init() {
	version210, _ = semver.Parse(version210Str)
	version220, _ = semver.Parse(version220Str)
}
