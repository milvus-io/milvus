package paramtable

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

const cipherYamlFile = "cipher.yaml"

type cipherConfig struct {
	cipherBase *BaseTable

	SoPathGo    ParamItem  `refreshable:"false"`
	SoPathCpp   ParamItem  `refreshable:"false"`
	KmsProvider ParamItem  `refreshable:"false"`
	SoConfigs   ParamGroup `refreshable:"true"`
}

func (c *cipherConfig) init(base *BaseTable) {
	c.cipherBase = base
	log.Info("cipher config", zap.Any("cipher", base.FileConfigs()))

	c.SoPathGo = ParamItem{
		Key:     "soPath.go",
		Version: "2.6.0",
	}
	c.SoPathGo.Init(base.mgr)

	c.SoPathCpp = ParamItem{
		Key:     "soPath.cpp",
		Version: "2.6.0",
	}
	c.SoPathCpp.Init(base.mgr)
}

func (c *cipherConfig) Save(key string, value string) error {
	return c.cipherBase.Save(key, value)
}
