package paramtable

import (
	"github.com/milvus-io/milvus/pkg/v2/log"
)

const cipherYamlFile = "hook.yaml"

type cipherConfig struct {
	cipherBase *BaseTable

	SoPathGo               ParamItem `refreshable:"false"`
	SoPathCpp              ParamItem `refreshable:"false"`
	DefaultRootKey         ParamItem `refreshable:"true"`
	KmsAwsRoleARN          ParamItem `refreshable:"true"`
	KmsAwsExternalID       ParamItem `refreshable:"true"`
	RotationPeriodInHours  ParamItem `refreshable:"true"`
	UpdatePerieldInMinutes ParamItem `refreshable:"true"`
	EnalbeDiskEncryption   ParamItem `refreshable:"false"`
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
		Key:          "cipherPlugin.kms.defaultKey",
		Version:      "2.6.1",
		FallbackKeys: []string{"cipherPlugin.defaultKmsKeyArn"},
	}
	c.DefaultRootKey.Init(base.mgr)

	c.KmsAwsRoleARN = ParamItem{
		Key:     "cipherPlugin.kms.credentials.aws.roleARN",
		Version: "2.6.1",
	}
	c.KmsAwsRoleARN.Init(base.mgr)

	c.KmsAwsExternalID = ParamItem{
		Key:     "cipherPlugin.kms.credentials.aws.externalID",
		Version: "2.6.1",
	}
	c.KmsAwsExternalID.Init(base.mgr)

	c.RotationPeriodInHours = ParamItem{
		Key:          "cipherPlugin.rotationPeriodInHours",
		Version:      "2.6.1",
		DefaultValue: "8764",
	}
	c.RotationPeriodInHours.Init(base.mgr)

	c.UpdatePerieldInMinutes = ParamItem{
		Key:          "cipherPlugin.updatePerieldInMinutes",
		Version:      "2.6.1",
		DefaultValue: "60",
	}
	c.UpdatePerieldInMinutes.Init(base.mgr)

	c.EnalbeDiskEncryption = ParamItem{
		Key:          "cipherPlugin.enableDiskEncryption",
		Version:      "2.6.1",
		DefaultValue: "false",
	}
	c.EnalbeDiskEncryption.Init(base.mgr)
}

func (c *cipherConfig) Save(key string, value string) error {
	return c.cipherBase.Save(key, value)
}

func (c *cipherConfig) GetAll() map[string]string {
	return c.cipherBase.mgr.GetConfigs()
}
