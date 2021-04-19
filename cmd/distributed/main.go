package main

import (
	"flag"
	"os"
	"strings"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/roles"
)

func initRoles(roles *roles.MilvusRoles) {
	flag.BoolVar(&roles.EnableMaster, "master-service", false, "start as master service")
	flag.BoolVar(&roles.EnableProxyService, "proxy-service", false, "start as proxy service")
	flag.BoolVar(&roles.EnableProxyNode, "proxy-node", false, "start as proxy node")
	flag.BoolVar(&roles.EnableQueryService, "query-service", false, "start as query service")
	flag.BoolVar(&roles.EnableQueryNode, "query-node", false, "start as query node")
	flag.BoolVar(&roles.EnableDataService, "data-service", false, "start as data service")
	flag.BoolVar(&roles.EnableDataNode, "data-node", false, "start as data node")
	flag.BoolVar(&roles.EnableIndexService, "index-service", false, "start as index service")
	flag.BoolVar(&roles.EnableIndexNode, "index-node", false, "start as index node")
	flag.BoolVar(&roles.EnableMsgStreamService, "msg-stream", false, "start as msg stream service")
	flag.Parse()

	if !roles.HasAnyRole() {
		for _, e := range os.Environ() {
			pairs := strings.SplitN(e, "=", 2)
			if len(pairs) == 2 {
				switch pairs[0] {
				case "ENABLE_MASTER":
					roles.EnableMaster = roles.EnvValue(pairs[1])
				case "ENABLE_PROXY_SERVICE":
					roles.EnableProxyService = roles.EnvValue(pairs[1])
				case "ENABLE_PROXY_NODE":
					roles.EnableProxyNode = roles.EnvValue(pairs[1])
				case "ENABLE_QUERY_SERVICE":
					roles.EnableQueryService = roles.EnvValue(pairs[1])
				case "ENABLE_QUERY_NODE":
					roles.EnableQueryNode = roles.EnvValue(pairs[1])
				case "ENABLE_DATA_SERVICE":
					roles.EnableDataService = roles.EnvValue(pairs[1])
				case "ENABLE_DATA_NODE":
					roles.EnableDataNode = roles.EnvValue(pairs[1])
				case "ENABLE_INDEX_SERVICE":
					roles.EnableIndexService = roles.EnvValue(pairs[1])
				case "ENABLE_INDEX_NODE":
					roles.EnableIndexNode = roles.EnvValue(pairs[1])
				case "ENABLE_MSGSTREAM_SERVICE":
					roles.EnableMsgStreamService = roles.EnvValue(pairs[1])
				}
			}
		}
	}
}

func main() {
	var roles roles.MilvusRoles
	initRoles(&roles)
	roles.Run()
}
