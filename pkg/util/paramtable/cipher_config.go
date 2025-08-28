package paramtable

import (
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const cipherYamlFile = "hook.yaml"

type cipherConfig struct {
	cipherBase *BaseTable

	SoPathGo              ParamItem `refreshable:"false"`
	SoPathCpp             ParamItem `refreshable:"false"`
	DefaultRootKey        ParamItem `refreshable:"false"`
	RotationPeriodInHours ParamItem `refreshable:"false"`
	KmsProvider           ParamItem `refreshable:"false"`
}

func (c *cipherConfig) init(base *BaseTable) {
	c.cipherBase = base
	log.Info("init cipher config")

	c.SoPathGo = ParamItem{
		Key:     "cipherPlugin.soPathGo",
		Version: "2.6.1",
	}
	c.SoPathGo.Init(base.mgr)

	c.SoPathCpp = ParamItem{
		Key:     "cipherPlugin.soPathCpp",
		Version: "2.6.1",
	}
	c.SoPathCpp.Init(base.mgr)

	c.DefaultRootKey = ParamItem{
		Key:     "cipherPlugin.defaultKmsKeyArn",
		Version: "2.6.1",
	}
	c.DefaultRootKey.Init(base.mgr)

	c.RotationPeriodInHours = ParamItem{
		Key:          "cipherPlugin.rotationPeriodInHours",
		Version:      "2.6.1",
		DefaultValue: "8764",
	}
	c.RotationPeriodInHours.Init(base.mgr)

	c.KmsProvider = ParamItem{
		Key:     "cipherPlugin.kmsProvider",
		Version: "2.6.1",
	}
	c.KmsProvider.Init(base.mgr)
}

func (c *cipherConfig) Save(key string, value string) error {
	return c.cipherBase.Save(key, value)
}

func (c *cipherConfig) GetAll() map[string]string {
	return c.cipherBase.mgr.GetConfigs()
}
