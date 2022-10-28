package configs

import (
	"fmt"

	"github.com/milvus-io/milvus/cmd/tools/migration/console"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

const (
	RunCmd      = "run"
	BackupCmd   = "backup"
	RollbackCmd = "rollback"
)

type RunConfig struct {
	base           *paramtable.BaseTable
	Cmd            string
	RunWithBackup  bool
	SourceVersion  string
	TargetVersion  string
	BackupFilePath string
}

func newRunConfig(base *paramtable.BaseTable) *RunConfig {
	c := &RunConfig{}
	c.init(base)
	return c
}

func (c *RunConfig) String() string {
	if c == nil {
		return ""
	}
	switch c.Cmd {
	case RunCmd:
		return fmt.Sprintf("Cmd: %s, SourceVersion: %s, TargetVersion: %s, BackupFilePath: %s, RunWithBackup: %v",
			c.Cmd, c.SourceVersion, c.TargetVersion, c.BackupFilePath, c.RunWithBackup)
	case BackupCmd:
		return fmt.Sprintf("Cmd: %s, SourceVersion: %s, BackupFilePath: %s",
			c.Cmd, c.SourceVersion, c.BackupFilePath)
	case RollbackCmd:
		return fmt.Sprintf("Cmd: %s, SourceVersion: %s, TargetVersion: %s, BackupFilePath: %s",
			c.Cmd, c.SourceVersion, c.TargetVersion, c.BackupFilePath)
	default:
		return fmt.Sprintf("invalid cmd: %s", c.Cmd)
	}
}

func (c *RunConfig) show() {
	console.Warning(c.String())
}

func (c *RunConfig) init(base *paramtable.BaseTable) {
	c.base = base

	c.Cmd = c.base.LoadWithDefault("cmd.type", "")
	c.RunWithBackup = c.base.ParseBool("cmd.runWithBackup", false)
	c.SourceVersion = c.base.LoadWithDefault("config.sourceVersion", "")
	c.TargetVersion = c.base.LoadWithDefault("config.targetVersion", "")
	c.BackupFilePath = c.base.LoadWithDefault("config.backupFilePath", "")
}

type MilvusConfig struct {
	MetaStoreCfg *paramtable.MetaStoreConfig
	EtcdCfg      *paramtable.EtcdConfig
	MysqlCfg     *paramtable.MetaDBConfig
}

func newMilvusConfig(base *paramtable.BaseTable) *MilvusConfig {
	c := &MilvusConfig{}
	c.init(base)
	return c
}

func (c *MilvusConfig) init(base *paramtable.BaseTable) {
	c.MetaStoreCfg = &paramtable.MetaStoreConfig{}
	c.EtcdCfg = &paramtable.EtcdConfig{}
	c.MysqlCfg = &paramtable.MetaDBConfig{}

	c.MetaStoreCfg.Base = base
	c.MetaStoreCfg.LoadCfgToMemory()

	switch c.MetaStoreCfg.MetaStoreType {
	case util.MetaStoreTypeMysql:
		c.MysqlCfg.Base = base
		c.MysqlCfg.LoadCfgToMemory()
	default:
	}

	c.EtcdCfg.Base = base
	c.EtcdCfg.LoadCfgToMemory()
}

func (c *MilvusConfig) String() string {
	if c == nil {
		return ""
	}
	switch c.MetaStoreCfg.MetaStoreType {
	case util.MetaStoreTypeEtcd:
		return fmt.Sprintf("Type: %s, EndPoints: %v, MetaRootPath: %s", c.MetaStoreCfg.MetaStoreType, c.EtcdCfg.Endpoints, c.EtcdCfg.MetaRootPath)
	default:
		return fmt.Sprintf("unsupported meta store: %s", c.MetaStoreCfg.MetaStoreType)
	}
}

func (c *MilvusConfig) show() {
	console.Warning(c.String())
}

type Config struct {
	base *paramtable.BaseTable
	*RunConfig
	*MilvusConfig
}

func (c *Config) init(yamlFile string) {
	c.base = paramtable.NewBaseTableFromYamlOnly(yamlFile)
	c.RunConfig = newRunConfig(c.base)
	c.MilvusConfig = newMilvusConfig(c.base)

	c.RunConfig.show()
	c.MilvusConfig.show()
}

func NewConfig(yamlFile string) *Config {
	c := &Config{}
	c.init(yamlFile)
	return c
}
