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

package roles

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/dataservice"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/indexservice"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/masterservice"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proxynode"
	"github.com/milvus-io/milvus/internal/querynode"
	"github.com/milvus-io/milvus/internal/queryservice"

	"github.com/milvus-io/milvus/cmd/distributed/components"
	"github.com/milvus-io/milvus/internal/logutil"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/util/trace"
)

func newMsgFactory(localMsg bool) msgstream.Factory {
	if localMsg {
		return msgstream.NewRmsFactory()
	}
	return msgstream.NewPmsFactory()
}

type MilvusRoles struct {
	EnableMaster           bool `env:"ENABLE_MASTER"`
	EnableProxyNode        bool `env:"ENABLE_PROXY_NODE"`
	EnableQueryService     bool `env:"ENABLE_QUERY_SERVICE"`
	EnableQueryNode        bool `env:"ENABLE_QUERY_NODE"`
	EnableDataService      bool `env:"ENABLE_DATA_SERVICE"`
	EnableDataNode         bool `env:"ENABLE_DATA_NODE"`
	EnableIndexService     bool `env:"ENABLE_INDEX_SERVICE"`
	EnableIndexNode        bool `env:"ENABLE_INDEX_NODE"`
	EnableMsgStreamService bool `env:"ENABLE_MSGSTREAM_SERVICE"`
}

func (mr *MilvusRoles) EnvValue(env string) bool {
	env = strings.ToLower(env)
	env = strings.Trim(env, " ")
	return env == "1" || env == "true"
}

func (mr *MilvusRoles) Run(localMsg bool) {
	if os.Getenv("DEPLOY_MODE") == "STANDALONE" {
		closer := trace.InitTracing("standalone")
		if closer != nil {
			defer closer.Close()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	if mr.EnableMaster {
		var ms *components.MasterService
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			masterservice.Params.Init()
			logutil.SetupLogger(&masterservice.Params.Log)
			defer log.Sync()

			factory := newMsgFactory(localMsg)
			var err error
			ms, err = components.NewMasterService(ctx, factory)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = ms.Run()
		}()

		wg.Wait()
		if ms != nil {
			defer ms.Stop()
		}

		metrics.RegisterMaster()
	}

	if mr.EnableProxyNode {
		var pn *components.ProxyNode
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			proxynode.Params.Init()
			logutil.SetupLogger(&proxynode.Params.Log)
			defer log.Sync()

			factory := newMsgFactory(localMsg)
			var err error
			pn, err = components.NewProxyNode(ctx, factory)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = pn.Run()
		}()

		wg.Wait()
		if pn != nil {
			defer pn.Stop()
		}

		metrics.RegisterProxyNode()
	}

	if mr.EnableQueryService {
		var qs *components.QueryService
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			queryservice.Params.Init()
			logutil.SetupLogger(&queryservice.Params.Log)
			defer log.Sync()

			factory := newMsgFactory(localMsg)
			var err error
			qs, err = components.NewQueryService(ctx, factory)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = qs.Run()
		}()

		wg.Wait()
		if qs != nil {
			defer qs.Stop()
		}

		metrics.RegisterQueryService()
	}

	if mr.EnableQueryNode {
		var qn *components.QueryNode
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			querynode.Params.Init()
			logutil.SetupLogger(&querynode.Params.Log)
			defer log.Sync()

			factory := newMsgFactory(localMsg)
			var err error
			qn, err = components.NewQueryNode(ctx, factory)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = qn.Run()
		}()

		wg.Wait()
		if qn != nil {
			defer qn.Stop()
		}

		metrics.RegisterQueryNode()
	}

	if mr.EnableDataService {
		var ds *components.DataService
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			dataservice.Params.Init()
			logutil.SetupLogger(&dataservice.Params.Log)
			defer log.Sync()

			factory := newMsgFactory(localMsg)
			var err error
			ds, err = components.NewDataService(ctx, factory)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = ds.Run()
		}()

		wg.Wait()
		if ds != nil {
			defer ds.Stop()
		}

		metrics.RegisterDataService()
	}

	if mr.EnableDataNode {
		var dn *components.DataNode
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			datanode.Params.Init()
			logutil.SetupLogger(&datanode.Params.Log)
			defer log.Sync()

			factory := newMsgFactory(localMsg)
			var err error
			dn, err = components.NewDataNode(ctx, factory)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = dn.Run()
		}()

		wg.Wait()
		if dn != nil {
			defer dn.Stop()
		}

		metrics.RegisterDataNode()
	}

	if mr.EnableIndexService {
		var is *components.IndexService
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			indexservice.Params.Init()
			logutil.SetupLogger(&indexservice.Params.Log)
			defer log.Sync()

			var err error
			is, err = components.NewIndexService(ctx)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = is.Run()
		}()

		wg.Wait()
		if is != nil {
			defer is.Stop()
		}

		metrics.RegisterIndexService()
	}

	if mr.EnableIndexNode {
		var in *components.IndexNode
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			indexnode.Params.Init()
			logutil.SetupLogger(&indexnode.Params.Log)
			defer log.Sync()

			var err error
			in, err = components.NewIndexNode(ctx)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = in.Run()
		}()

		wg.Wait()
		if in != nil {
			defer in.Stop()
		}

		metrics.RegisterIndexNode()
	}

	if mr.EnableMsgStreamService {
		var mss *components.MsgStream
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			var err error
			mss, err = components.NewMsgStreamService(ctx)
			if err != nil {
				panic(err)
			}
			wg.Done()
			_ = mss.Run()
		}()

		wg.Wait()
		if mss != nil {
			defer mss.Stop()
		}

		metrics.RegisterMsgStreamService()
	}

	metrics.ServeHTTP()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	fmt.Printf("Get %s signal to exit\n", sig.String())

	// some deferred Stop has race with context cancel
	cancel()
}
