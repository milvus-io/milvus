package paramtable

import (
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const cipherYamlFile = "cipher.yaml"

type cipherConfig struct {
	cipherBase *BaseTable

	SoPathGo       ParamItem `refreshable:"false"`
	SoPathCpp      ParamItem `refreshable:"false"`
	DefaultRootKey ParamItem `refreshable:"false"`
}

func (c *cipherConfig) init(base *BaseTable) {
	c.cipherBase = base
	log.Info("init cipher config")

	c.SoPathGo = ParamItem{
		Key:     "cipherPlugin.soPathGo",
		Version: "2.6.0",
	}
	c.SoPathGo.Init(base.mgr)

	c.SoPathCpp = ParamItem{
		Key:     "cipherPlugin.soPathCpp",
		Version: "2.6.0",
	}
	c.SoPathCpp.Init(base.mgr)

	c.DefaultRootKey = ParamItem{
		Key:     "cipherPlugin.defaultKmsKeyArn",
		Version: "2.6.0",
	}
	c.DefaultRootKey.Init(base.mgr)
}

func (c *cipherConfig) Save(key string, value string) error {
	return c.cipherBase.Save(key, value)
}
