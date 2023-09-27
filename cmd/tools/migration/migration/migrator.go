package migration

import (
	"fmt"

	"github.com/blang/semver/v4"

	"github.com/milvus-io/milvus/cmd/tools/migration/meta"
	"github.com/milvus-io/milvus/cmd/tools/migration/versions"
)

type Migrator interface {
	Migrate(metas *meta.Meta) (*meta.Meta, error)
}

func NewMigrator(sourceVersion, targetVersion string) (Migrator, error) {
	source, err := semver.Parse(sourceVersion)
	if err != nil {
		return nil, err
	}

	target, err := semver.Parse(targetVersion)
	if err != nil {
		return nil, err
	}

	if versions.Range21x(source) && versions.Range22x(target) {
		return newMigrator210To220(), nil
	}

	return nil, fmt.Errorf("migration from source version to target version is forbidden, source: %s, target: %s",
		sourceVersion, targetVersion)
}
