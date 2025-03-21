package paramtable

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const cipherYamlFile = "cipher.yaml"

type cipherConfig struct {
	cipherBase *BaseTable

	SoPathGo ParamItem `refreshable:"false"`
	// KmsProvider ParamItem  `refreshable:"false"`
	SoConfigs ParamGroup `refreshable:"true"`
}

func (c *cipherConfig) init(base *BaseTable) {
	c.cipherBase = base
	log.Info("cipher config", zap.Any("cipher", base.FileConfigs()))

	c.SoPathGo = ParamItem{
		Key:     "soPath.go",
		Version: "2.6.0",
	}
	c.SoPathGo.Init(base.mgr)

	// c.KmsProvider = ParamItem{
	//     Key:          "kmsProvider",
	//     Version:      "2.6.0",
	//     DefaultValue: "unsafe",
	// }
	// c.KmsProvider.Init(base.mgr)
	//
	c.SoConfigs = ParamGroup{
		KeyPrefix: "",
		Version:   "2.6.0",
	}
	c.SoConfigs.Init(base.mgr)
}

func (c *cipherConfig) WatchCipherWithPrefix(ident string, keyPrefix string, onEvent func(*config.Event)) {
	c.cipherBase.mgr.Dispatcher.RegisterForKeyPrefix(keyPrefix, config.NewHandler(ident, onEvent))
}

func (c *cipherConfig) GetAll() map[string]string {
	return c.cipherBase.mgr.GetConfigs()
}

func (c *cipherConfig) Save(key string, value string) error {
	return c.cipherBase.Save(key, value)
}
