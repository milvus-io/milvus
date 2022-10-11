package meta

import (
	"github.com/blang/semver/v4"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp

type Meta struct {
	SourceVersion semver.Version
	Version       semver.Version

	Meta210 *All210
	Meta220 *All220
}
