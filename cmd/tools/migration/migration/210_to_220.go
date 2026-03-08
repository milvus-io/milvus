package migration

import (
	"github.com/milvus-io/milvus/cmd/tools/migration/meta"
)

type migrator210To220 struct{}

func (m migrator210To220) Migrate(metas *meta.Meta) (*meta.Meta, error) {
	return meta.From210To220(metas)
}

func newMigrator210To220() *migrator210To220 {
	return &migrator210To220{}
}
