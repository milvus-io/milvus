package roles

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/components"
	ds "github.com/zilliztech/milvus-distributed/internal/dataservice"
	"github.com/zilliztech/milvus-distributed/internal/logutil"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/util/rocksmq/server/rocksmq"
	"github.com/zilliztech/milvus-distributed/internal/util/trace"
)

func newMsgFactory(localMsg bool) msgstream.Factory {
	if localMsg {
		return msgstream.NewRmsFactory()
	}
	return msgstream.NewPmsFactory()
}

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

func (mr *MilvusRoles) Run(localMsg bool) {

	closer := trace.InitTracing("singleNode")
	if closer != nil {
		defer closer.Close()
	}

	if !mr.HasAnyRole() {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var masterService *components.MasterService
	if mr.EnableMaster {
		go func() {
			factory := newMsgFactory(localMsg)
			var err error
			masterService, err = components.NewMasterService(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = masterService.Run()
		}()
	}

	var proxyService *components.ProxyService
	if mr.EnableProxyService {
		go func() {
			factory := newMsgFactory(localMsg)
			var err error
			proxyService, err = components.NewProxyService(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = proxyService.Run()
		}()
	}

	var proxyNode *components.ProxyNode
	if mr.EnableProxyNode {
		go func() {
			factory := newMsgFactory(localMsg)
			var err error
			proxyNode, err = components.NewProxyNode(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = proxyNode.Run()
		}()
	}

	var queryService *components.QueryService
	if mr.EnableQueryService {
		go func() {
			factory := newMsgFactory(localMsg)
			var err error
			queryService, err = components.NewQueryService(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = queryService.Run()
		}()
	}

	var queryNode *components.QueryNode
	if mr.EnableQueryNode {
		go func() {
			factory := newMsgFactory(localMsg)
			var err error
			queryNode, err = components.NewQueryNode(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = queryNode.Run()
		}()
	}

	var dataService *components.DataService
	if mr.EnableDataService {
		go func() {
			factory := newMsgFactory(localMsg)
			var err error
			// Init data service params
			ds.Params.Init()
			logutil.SetupLogger(&ds.Params.Log)
			dataService, err = components.NewDataService(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = dataService.Run()
		}()
	}

	var dataNode *components.DataNode
	if mr.EnableDataNode {
		go func() {
			factory := newMsgFactory(localMsg)
			var err error
			dataNode, err = components.NewDataNode(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = dataNode.Run()
		}()
	}

	var indexService *components.IndexService
	if mr.EnableIndexService {
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
	fmt.Printf("Get %s signal to exit", sig.String())

	if mr.EnableMaster {
		if masterService != nil {
			_ = masterService.Stop()
		}
	}

	if mr.EnableProxyService {
		if proxyService != nil {
			_ = proxyService.Stop()
		}
	}

	if mr.EnableProxyNode {
		if proxyNode != nil {
			_ = proxyNode.Stop()
		}
	}

	if mr.EnableQueryService {
		if queryService != nil {
			_ = queryService.Stop()
		}
	}

	if mr.EnableQueryNode {
		if queryNode != nil {
			_ = queryNode.Stop()
		}
	}

	if mr.EnableDataService {
		if dataService != nil {
			_ = dataService.Stop()
		}
	}

	if mr.EnableDataNode {
		if dataNode != nil {
			_ = dataNode.Stop()
		}
	}

	if mr.EnableIndexService {
		if indexService != nil {
			_ = indexService.Stop()
		}
	}

	if mr.EnableIndexNode {
		if indexNode != nil {
			_ = indexNode.Stop()
		}
	}

	if mr.EnableMsgStreamService {
		if msgStream != nil {
			_ = msgStream.Stop()
		}
	}

	defer rocksmq.CloseRocksMQ()
}
