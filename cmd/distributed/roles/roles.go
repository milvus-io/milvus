package roles

import (
	"context"
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
	EnableStandalone       bool `env:"ENABLE_STANDALONE"`
}

func (mr *MilvusRoles) HasAnyRole() bool {
	return mr.EnableMaster || mr.EnableMsgStreamService ||
		mr.EnableProxyService || mr.EnableProxyNode ||
		mr.EnableQueryService || mr.EnableQueryNode ||
		mr.EnableDataService || mr.EnableDataNode ||
		mr.EnableIndexService || mr.EnableIndexNode || mr.EnableStandalone
}

func (mr *MilvusRoles) EnvValue(env string) bool {
	env = strings.ToLower(env)
	env = strings.Trim(env, " ")
	if env == "1" || env == "true" {
		return true
	}
	return false
}

func (mr *MilvusRoles) Run() {
	if !mr.HasAnyRole() {
		log.Printf("set the roles please ...")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var masterService *components.MasterService
	if mr.EnableMaster {
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
	if mr.EnableProxyService {
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
	if mr.EnableProxyNode {
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
	if mr.EnableQueryService {
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
	if mr.EnableQueryNode {
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
	if mr.EnableDataService {
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
	if mr.EnableDataNode {
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
	if mr.EnableIndexService {
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
	if mr.EnableIndexNode {
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
	if mr.EnableMsgStreamService {
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

	if mr.EnableMaster {
		if masterService != nil {
			_ = masterService.Stop()
		}
		log.Printf("exit master service")
	}

	if mr.EnableProxyService {
		if proxyService != nil {
			_ = proxyService.Stop()
		}
		log.Printf("exit proxy service")
	}

	if mr.EnableProxyNode {
		if proxyNode != nil {
			_ = proxyNode.Stop()
		}
		log.Printf("exit proxy node")
	}

	if mr.EnableQueryService {
		if queryService != nil {
			_ = queryService.Stop()
		}
		log.Printf("exit query service")
	}

	if mr.EnableQueryNode {
		if queryNode != nil {
			_ = queryNode.Stop()
		}
		log.Printf("exit query node")
	}

	if mr.EnableDataService {
		if dataService != nil {
			_ = dataService.Stop()
		}
		log.Printf("exit data service")
	}

	if mr.EnableDataNode {
		if dataNode != nil {
			_ = dataNode.Stop()
		}
		log.Printf("exit data node")
	}

	if mr.EnableIndexService {
		if indexService != nil {
			_ = indexService.Stop()
		}
		log.Printf("exit index service")
	}

	if mr.EnableIndexNode {
		if indexNode != nil {
			_ = indexNode.Stop()
		}
		log.Printf("exit index node")
	}

	if mr.EnableMsgStreamService {
		if msgStream != nil {
			_ = msgStream.Stop()
		}
		log.Printf("exit msg stream service")
	}
}
