package versions

import "github.com/blang/semver/v4"

const (
	version210Str = "2.1.0"
	version220Str = "2.2.0"
	version230Str = "2.3.0"
	VersionMaxStr = "1000.1000.1000"
)

var (
	Version220 semver.Version
	Version210 semver.Version
	Version230 semver.Version
	VersionMax semver.Version
)

func init() {
	Version210, _ = semver.Parse(version210Str)
	Version220, _ = semver.Parse(version220Str)
	Version230, _ = semver.Parse(version230Str)
	VersionMax, _ = semver.Parse(VersionMaxStr)
}

func Range21x(version semver.Version) bool {
	return version.GTE(Version210) && version.LT(Version220)
}

func Range22x(version semver.Version) bool {
	return version.GTE(Version220) && version.LT(Version230)
}
