package main

import (
	"os"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/roles"
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

func main() {
	var roles roles.MilvusRoles
	initRoles(&roles)
	os.Setenv("QUERY_NODE_ID", "1")
	roles.Run(true)
}
