package backend

import (
	"fmt"

	"github.com/blang/semver/v4"

	"github.com/milvus-io/milvus/cmd/tools/migration/configs"
	"github.com/milvus-io/milvus/cmd/tools/migration/meta"
	"github.com/milvus-io/milvus/cmd/tools/migration/versions"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

type Backend interface {
	Load() (*meta.Meta, error)
	Save(meta *meta.Meta) error
	Clean() error
	Backup(meta *meta.Meta, backupFile string) error
	BackupV2(file string) error
	Restore(backupFile string) error
}

func NewBackend(cfg *configs.MilvusConfig, version string) (Backend, error) {
	if cfg.MetaStoreCfg.MetaStoreType.GetValue() != util.MetaStoreTypeEtcd {
		return nil, fmt.Errorf("%s is not supported now", cfg.MetaStoreCfg.MetaStoreType.GetValue())
	}
	v, err := semver.Parse(version)
	if err != nil {
		return nil, err
	}
	if versions.Range21x(v) {
		return newEtcd210(cfg)
	} else if versions.Range22x(v) {
		return newEtcd220(cfg)
	}
	return nil, fmt.Errorf("version not supported: %s", version)
}
