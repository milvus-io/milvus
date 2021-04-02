package main

import (
	"os"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/roles"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/logutil"
)

func initRoles(roles *roles.MilvusRoles) {
	roles.EnableMaster = true
	roles.EnableProxyService = true
	roles.EnableProxyNode = true
	roles.EnableQueryService = true
	roles.EnableQueryNode = true
	roles.EnableDataService = true
	roles.EnableDataNode = true
	roles.EnableIndexService = true
	roles.EnableIndexNode = true
	roles.EnableMsgStreamService = true
}

func initLogCfg() log.Config {
	logCfg := log.Config{}
	logCfg.Format = "text"
	logCfg.Level = "debug"
	logCfg.Development = true
	logCfg.File.MaxSize = 300
	logCfg.File.MaxBackups = 20
	logCfg.File.MaxDays = 10

	// FIXME(wxyu): Load from config files
	//logCfg.File.Filename = ""
	ciFileDir := "/milvus-distributed/logs/"
	if _, err := os.Stat(ciFileDir); err == nil {
		logCfg.File.Filename = ciFileDir + "singlenode.log"
	} else {
		logCfg.File.Filename = "/tmp/milvus/singlenode.log"
	}
	return logCfg
}

func main() {
	var roles roles.MilvusRoles
	initRoles(&roles)
	os.Setenv("QUERY_NODE_ID", "1")
	os.Setenv("DEPLOY_MODE", "SINGLE_NODE")

	logCfg := initLogCfg()
	logutil.SetupLogger(&logCfg)
	roles.Run(true)
}
