package paramtable

import "testing"

func TestIndexEngineConfig_Init(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	cfg := &params.IndexEngineConfig
	print(cfg)
}

func TestIndexEngineConfig_Get(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	cfg := &params.IndexEngineConfig
	diskANNbuild := cfg.getIndexParam("DISKANN", BuildStage)
	print(diskANNbuild)
}
