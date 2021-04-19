package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/components"
)

type MilvusRoles struct {
	EnableMaster           bool `env:"ENABLE_MASTER"`
	EnableProxyService     bool `env:"ENABLE_PROXY_SERVICE"`
	EnableProxyNode        bool `env:"ENABLE_PROXY_NODE"`
	EnableQueryService     bool `env:"ENABLE_QUERY_SERVICE"`
	EnableQueryNode        bool `env:"ENABLE_QUERY_NODE"`
	EnableDataService      bool `env:"ENABLE_DATA_SERVICE"`
	EnableDataNode         bool `env:"ENABLE_DATA_NODE"`
	EnableIndexService     bool `env:"ENABLE_INDEX_SERVICE"`
	EnableIndexNode        bool `env:"ENABLE_INDEX_NODE"`
	EnableMsgStreamService bool `env:"ENABLE_MSGSTREAM_SERVICE"`
}

func (mr *MilvusRoles) hasAnyRole() bool {
	return mr.EnableMaster || mr.EnableMsgStreamService ||
		mr.EnableProxyService || mr.EnableProxyNode ||
		mr.EnableQueryService || mr.EnableQueryNode ||
		mr.EnableDataService || mr.EnableDataNode ||
		mr.EnableIndexService || mr.EnableIndexNode
}

func (mr *MilvusRoles) envValue(env string) bool {
	env = strings.ToLower(env)
	env = strings.Trim(env, " ")
	if env == "1" || env == "true" {
		return true
	}
	return false
}

func main() {
	var roles MilvusRoles

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

	if !roles.hasAnyRole() {
		for _, e := range os.Environ() {
			pairs := strings.SplitN(e, "=", 2)
			if len(pairs) == 2 {
				switch pairs[0] {
				case "ENABLE_MASTER":
					roles.EnableMaster = roles.envValue(pairs[1])
				case "ENABLE_PROXY_SERVICE":
					roles.EnableProxyService = roles.envValue(pairs[1])
				case "ENABLE_PROXY_NODE":
					roles.EnableProxyNode = roles.envValue(pairs[1])
				case "ENABLE_QUERY_SERVICE":
					roles.EnableQueryService = roles.envValue(pairs[1])
				case "ENABLE_QUERY_NODE":
					roles.EnableQueryNode = roles.envValue(pairs[1])
				case "ENABLE_DATA_SERVICE":
					roles.EnableDataService = roles.envValue(pairs[1])
				case "ENABLE_DATA_NODE":
					roles.EnableDataNode = roles.envValue(pairs[1])
				case "ENABLE_INDEX_SERVICE":
					roles.EnableIndexService = roles.envValue(pairs[1])
				case "ENABLE_INDEX_NODE":
					roles.EnableIndexNode = roles.envValue(pairs[1])
				case "ENABLE_MSGSTREAM_SERVICE":
					roles.EnableMsgStreamService = roles.envValue(pairs[1])
				}
			}
		}
	}

	if !roles.hasAnyRole() {
		log.Printf("set the roles please ...")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var masterService *components.MasterService
	if roles.EnableMaster {
		log.Print("start as master service")
		go func() {
			var err error
			masterService, err = components.NewMasterService(ctx)
			if err != nil {
				panic(err)
			}
			_ = masterService.Run()
		}()
	}

	var proxyService *components.ProxyService
	if roles.EnableProxyService {
		log.Print("start as proxy service")
		go func() {
			var err error
			proxyService, err = components.NewProxyService(ctx)
			if err != nil {
				panic(err)
			}
			_ = proxyService.Run()
		}()
	}

	var proxyNode *components.ProxyNode
	if roles.EnableProxyNode {
		log.Print("start as proxy node")
		go func() {
			var err error
			proxyNode, err = components.NewProxyNode(ctx)
			if err != nil {
				panic(err)
			}
			_ = proxyNode.Run()
		}()
	}

	var queryService *components.QueryService
	if roles.EnableQueryService {
		log.Print("start as query service")
		go func() {
			var err error
			queryService, err = components.NewQueryService(ctx)
			if err != nil {
				panic(err)
			}
			_ = queryService.Run()
		}()
	}

	var queryNode *components.QueryNode
	if roles.EnableQueryNode {
		log.Print("start as query node")
		go func() {
			var err error
			queryNode, err = components.NewQueryNode(ctx)
			if err != nil {
				panic(err)
			}
			_ = queryNode.Run()
		}()
	}

	var dataService *components.DataService
	if roles.EnableDataService {
		log.Print("start as data service")
		go func() {
			var err error
			dataService, err = components.NewDataService(ctx)
			if err != nil {
				panic(err)
			}
			_ = dataService.Run()
		}()
	}

	var dataNode *components.DataNode
	if roles.EnableDataNode {
		log.Print("start as data node")
		go func() {
			var err error
			dataNode, err = components.NewDataNode(ctx)
			if err != nil {
				panic(err)
			}
			_ = dataNode.Run()
		}()
	}

	var indexService *components.IndexService
	if roles.EnableIndexService {
		log.Print("start as index service")
		go func() {
			var err error
			indexService, err = components.NewIndexService(ctx)
			if err != nil {
				panic(err)
			}
			_ = indexService.Run()
		}()
	}

	var indexNode *components.IndexNode
	if roles.EnableIndexNode {
		log.Print("start as index node")
		go func() {
			var err error
			indexNode, err = components.NewIndexNode(ctx)
			if err != nil {
				panic(err)
			}
			_ = indexNode.Run()
		}()
	}

	var msgStream *components.MsgStream
	if roles.EnableMsgStreamService {
		log.Print("start as msg stream service")
		go func() {
			var err error
			msgStream, err = components.NewMsgStreamService(ctx)
			if err != nil {
				panic(err)
			}
			_ = msgStream.Run()
		}()
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Printf("Get %s signal to exit", sig.String())

	if roles.EnableMaster {
		if masterService != nil {
			_ = masterService.Stop()
		}
		log.Printf("exit master service")
	}

	if roles.EnableProxyService {
		if proxyService != nil {
			_ = proxyService.Stop()
		}
		log.Printf("exit proxy service")
	}

	if roles.EnableProxyNode {
		if proxyNode != nil {
			_ = proxyNode.Stop()
		}
		log.Printf("exit proxy node")
	}

	if roles.EnableQueryService {
		if queryService != nil {
			_ = queryService.Stop()
		}
		log.Printf("exit query service")
	}

	if roles.EnableQueryNode {
		if queryNode != nil {
			_ = queryNode.Stop()
		}
		log.Printf("exit query node")
	}

	if roles.EnableDataService {
		if dataService != nil {
			_ = dataService.Stop()
		}
		log.Printf("exit data service")
	}

	if roles.EnableDataNode {
		if dataNode != nil {
			_ = dataNode.Stop()
		}
		log.Printf("exit data node")
	}

	if roles.EnableIndexService {
		if indexService != nil {
			_ = indexService.Stop()
		}
		log.Printf("exit index service")
	}

	if roles.EnableIndexNode {
		if indexNode != nil {
			_ = indexNode.Stop()
		}
		log.Printf("exit index node")
	}

	if roles.EnableMsgStreamService {
		if msgStream != nil {
			_ = msgStream.Stop()
		}
		log.Printf("exit msg stream service")
	}

}
