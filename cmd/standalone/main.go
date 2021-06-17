// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package main

import (
	"os"

	"github.com/milvus-io/milvus/cmd/distributed/roles"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/logutil"
)

func initRoles(roles *roles.MilvusRoles) {
	roles.EnableMaster = true
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
	logCfg.File.Filename = ""
	// ciFileDir := "/milvus/logs/"
	// if _, err := os.Stat(ciFileDir); err == nil {
	// 	logCfg.File.Filename = ciFileDir + "standalone.log"
	// } else {
	// 	logCfg.File.Filename = "/tmp/milvus/standalone.log"
	// }
	return logCfg
}

func main() {
	var roles roles.MilvusRoles
	initRoles(&roles)
	os.Setenv("QUERY_NODE_ID", "1")
	os.Setenv("DEPLOY_MODE", "STANDALONE")

	logCfg := initLogCfg()
	logutil.SetupLogger(&logCfg)
	roles.Run(true)
}
