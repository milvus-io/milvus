package common

import (
	"os"

	semver "github.com/blang/semver/v4"
)

const gitBuildTagsEnvKey = "MILVUS_GIT_BUILD_TAGS"

// Version current version for session
var Version semver.Version

func init() {
	Version = semver.MustParse("3.0.0-beta")
}

// MilvusVersion returns the build-injected version, falling back to the
// in-process version when it is empty or unknown.
func MilvusVersion() string {
	if v := os.Getenv(gitBuildTagsEnvKey); v != "" && v != "unknown" {
		return v
	}
	return Version.String()
}
