package migration

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/util"
)

type Backend interface {
	Backup(dryRun bool) error
	RollbackFromBackup() error
	CleanBackup() error
	Load() (*Meta, error)
	Save(meta *Meta, dryRun bool) error
	Clean() error
}

func NewBackend(opt *backendOpt) (Backend, error) {
	switch opt.config.MetaStoreCfg.MetaStoreType {
	case util.MetaStoreTypeMysql:
		return nil, fmt.Errorf("%s is not supported now", opt.config.MetaStoreCfg.MetaStoreType)
	}
	if opt.version.GT(version210) {
		return newEtcd220(opt)
	}
	return newEtcd210(opt)
}

func constructBackupKey(prefix, key string) string {
	return prefix + key
}
