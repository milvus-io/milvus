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
	"syscall"

	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/dataservice"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/indexservice"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/masterservice"
	"github.com/milvus-io/milvus/internal/proxynode"
	"github.com/milvus-io/milvus/internal/proxyservice"
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

func (mr *MilvusRoles) EnvValue(env string) bool {
	env = strings.ToLower(env)
	env = strings.Trim(env, " ")
	return env == "1" || env == "true"
}

func (mr *MilvusRoles) Run(localMsg bool) {
	closer := trace.InitTracing("standalone")
	if closer != nil {
		defer closer.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if mr.EnableMaster {
		var ms *components.MasterService

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
			_ = ms.Run()
		}()

		if ms != nil {
			defer ms.Stop()
		}
	}

	if mr.EnableProxyService {
		var ps *components.ProxyService

		go func() {
			proxyservice.Params.Init()
			logutil.SetupLogger(&proxyservice.Params.Log)
			defer log.Sync()

			factory := newMsgFactory(localMsg)
			var err error
			ps, err = components.NewProxyService(ctx, factory)
			if err != nil {
				panic(err)
			}
			_ = ps.Run()
		}()

		if ps != nil {
			defer ps.Stop()
		}
	}

	if mr.EnableProxyNode {
		var pn *components.ProxyNode

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
			_ = pn.Run()
		}()

		if pn != nil {
			defer pn.Stop()
		}
	}

	if mr.EnableQueryService {
		var qs *components.QueryService

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
			_ = qs.Run()
		}()

		if qs != nil {
			defer qs.Stop()
		}
	}

	if mr.EnableQueryNode {
		var qn *components.QueryNode

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
			_ = qn.Run()
		}()

		if qn != nil {
			defer qn.Stop()
		}
	}

	if mr.EnableDataService {
		var ds *components.DataService

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
			_ = ds.Run()
		}()

		if ds != nil {
			defer ds.Stop()
		}
	}

	if mr.EnableDataNode {
		var dn *components.DataNode

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
			_ = dn.Run()
		}()

		if dn != nil {
			defer dn.Stop()
		}
	}

	if mr.EnableIndexService {
		var is *components.IndexService

		go func() {
			indexservice.Params.Init()
			logutil.SetupLogger(&indexservice.Params.Log)
			defer log.Sync()

			var err error
			is, err = components.NewIndexService(ctx)
			if err != nil {
				panic(err)
			}
			_ = is.Run()
		}()

		if is != nil {
			defer is.Stop()
		}
	}

	if mr.EnableIndexNode {
		var in *components.IndexNode

		go func() {
			indexnode.Params.Init()
			logutil.SetupLogger(&indexnode.Params.Log)
			defer log.Sync()

			var err error
			in, err = components.NewIndexNode(ctx)
			if err != nil {
				panic(err)
			}
			_ = in.Run()
		}()

		if in != nil {
			in.Stop()
		}
	}

	if mr.EnableMsgStreamService {
		var mss *components.MsgStream

		go func() {
			var err error
			mss, err = components.NewMsgStreamService(ctx)
			if err != nil {
				panic(err)
			}
			_ = mss.Run()
		}()

		if mss != nil {
			defer mss.Stop()
		}
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	fmt.Printf("Get %s signal to exit", sig.String())
}
