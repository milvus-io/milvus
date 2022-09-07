package migration

import (
	"github.com/blang/semver/v4"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type metaStoreType = string

type backendOpt struct {
	version semver.Version
	config  *paramtable.ServiceParam
}

func (opt *backendOpt) SetVersion(version semver.Version) {
	opt.version = version
}

func ReadYamlAsBackendOpt(yamlFile string) *backendOpt {
	var config = &paramtable.ServiceParam{}
	config.BaseTable = paramtable.BaseTable{}
	config.GlobalInitWithYaml(yamlFile)
	config.Init()
	return &backendOpt{
		config: config,
	}
}
