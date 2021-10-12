// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package roles

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/milvus-io/milvus/internal/util/healthz"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/components"
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/indexcoord"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/logutil"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/querycoord"
	"github.com/milvus-io/milvus/internal/querynode"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
)

func newMsgFactory(localMsg bool) msgstream.Factory {
	if localMsg {
		return msgstream.NewRmsFactory()
	}
	return msgstream.NewPmsFactory()
}

type MilvusRoles struct {
	EnableRootCoord      bool `env:"ENABLE_ROOT_COORD"`
	EnableProxy          bool `env:"ENABLE_PROXY"`
	EnableQueryCoord     bool `env:"ENABLE_QUERY_COORD"`
	EnableQueryNode      bool `env:"ENABLE_QUERY_NODE"`
	EnableDataCoord      bool `env:"ENABLE_DATA_COORD"`
	EnableDataNode       bool `env:"ENABLE_DATA_NODE"`
	EnableIndexCoord     bool `env:"ENABLE_INDEX_COORD"`
	EnableIndexNode      bool `env:"ENABLE_INDEX_NODE"`
	EnableMsgStreamCoord bool `env:"ENABLE_MSGSTREAM_COORD"`
}

func (mr *MilvusRoles) EnvValue(env string) bool {
	env = strings.ToLower(env)
	env = strings.Trim(env, " ")
	return env == "1" || env == "true"
}

func (mr *MilvusRoles) setLogConfigFilename(filename string) *log.Config {
	paramtable.Params.Init()
	cfg := paramtable.Params.LogConfig
	if len(cfg.File.RootPath) != 0 {
		cfg.File.Filename = path.Join(cfg.File.RootPath, filename)
	} else {
		cfg.File.Filename = ""
	}
	return cfg
}

func (mr *MilvusRoles) runRootCoord(ctx context.Context, localMsg bool) *components.RootCoord {
	var rc *components.RootCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		rootcoord.Params.Init()

		f := setLoggerFunc(localMsg)
		rootcoord.Params.SetLogConfig(f)
		factory := newMsgFactory(localMsg)
		var err error
		rc, err = components.NewRootCoord(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: rc})
		}
		wg.Done()
		_ = rc.Run()
	}()
	wg.Wait()

	metrics.RegisterRootCoord()
	return rc
}

func (mr *MilvusRoles) runProxy(ctx context.Context, localMsg bool, alias string) *components.Proxy {
	var pn *components.Proxy
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		proxy.Params.InitAlias(alias)
		proxy.Params.Init()

		f := setLoggerFunc(localMsg)
		proxy.Params.SetLogConfig(f)
		factory := newMsgFactory(localMsg)
		var err error
		pn, err = components.NewProxy(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: pn})
		}
		wg.Done()
		_ = pn.Run()
	}()
	wg.Wait()

	metrics.RegisterProxy()
	return pn
}

func (mr *MilvusRoles) runQueryCoord(ctx context.Context, localMsg bool) *components.QueryCoord {
	var qs *components.QueryCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		querycoord.Params.Init()

		f := setLoggerFunc(localMsg)
		querycoord.Params.SetLogConfig(f)
		factory := newMsgFactory(localMsg)
		var err error
		qs, err = components.NewQueryCoord(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: qs})
		}
		wg.Done()
		_ = qs.Run()
	}()
	wg.Wait()

	metrics.RegisterQueryCoord()
	return qs
}

func (mr *MilvusRoles) runQueryNode(ctx context.Context, localMsg bool, alias string) *components.QueryNode {
	var qn *components.QueryNode
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		querynode.Params.InitAlias(alias)
		querynode.Params.Init()

		f := setLoggerFunc(localMsg)
		querynode.Params.SetLogConfig(f)
		factory := newMsgFactory(localMsg)
		var err error
		qn, err = components.NewQueryNode(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: qn})
		}
		wg.Done()
		_ = qn.Run()
	}()
	wg.Wait()

	metrics.RegisterQueryNode()
	return qn
}

func (mr *MilvusRoles) runDataCoord(ctx context.Context, localMsg bool) *components.DataCoord {
	var ds *components.DataCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		datacoord.Params.Init()

		f := setLoggerFunc(localMsg)
		datacoord.Params.SetLogConfig(f)
		factory := newMsgFactory(localMsg)
		var err error
		ds, err = components.NewDataCoord(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: ds})
		}
		wg.Done()
		_ = ds.Run()
	}()
	wg.Wait()

	metrics.RegisterDataCoord()
	return ds
}

func (mr *MilvusRoles) runDataNode(ctx context.Context, localMsg bool, alias string) *components.DataNode {
	var dn *components.DataNode
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		datanode.Params.InitAlias(alias)
		datanode.Params.Init()
		f := setLoggerFunc(localMsg)
		datanode.Params.SetLogConfig(f)
		factory := newMsgFactory(localMsg)
		var err error
		dn, err = components.NewDataNode(ctx, factory)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: dn})
		}
		wg.Done()
		_ = dn.Run()
	}()
	wg.Wait()

	metrics.RegisterDataNode()
	return dn
}

func (mr *MilvusRoles) runIndexCoord(ctx context.Context, localMsg bool) *components.IndexCoord {
	var is *components.IndexCoord
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		indexcoord.Params.Init()

		f := setLoggerFunc(localMsg)
		indexcoord.Params.SetLogConfig(f)
		var err error
		is, err = components.NewIndexCoord(ctx)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: is})
		}
		wg.Done()
		_ = is.Run()
	}()
	wg.Wait()

	metrics.RegisterIndexCoord()
	return is
}

func (mr *MilvusRoles) runIndexNode(ctx context.Context, localMsg bool, alias string) *components.IndexNode {
	var in *components.IndexNode
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		indexnode.Params.InitAlias(alias)
		indexnode.Params.Init()

		f := setLoggerFunc(localMsg)
		indexnode.Params.SetLogConfig(f)
		var err error
		in, err = components.NewIndexNode(ctx)
		if err != nil {
			panic(err)
		}
		if !localMsg {
			http.Handle(healthz.HealthzRouterPath, &componentsHealthzHandler{component: in})
		}
		wg.Done()
		_ = in.Run()
	}()
	wg.Wait()

	metrics.RegisterIndexNode()
	return in
}

func (mr *MilvusRoles) runMsgStreamCoord(ctx context.Context) *components.MsgStream {
	var mss *components.MsgStream
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		var err error
		mss, err = components.NewMsgStreamCoord(ctx)
		if err != nil {
			panic(err)
		}
		wg.Done()
		_ = mss.Run()
	}()
	wg.Wait()

	metrics.RegisterMsgStreamCoord()
	return mss
}

func (mr *MilvusRoles) Run(localMsg bool, alias string) {
	if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.StandaloneDeployMode {
		closer := trace.InitTracing("standalone")
		if closer != nil {
			defer closer.Close()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	// only standalone enable localMsg
	if localMsg {
		os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
		cfg := mr.setLogConfigFilename("standalone.log")
		logutil.SetupLogger(cfg)
		defer log.Sync()
	} else {
		err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode)
		if err != nil {
			log.Error("failed to set deploy mode: ", zap.Error(err))
		}
	}

	var rc *components.RootCoord
	if mr.EnableRootCoord {
		rc = mr.runRootCoord(ctx, localMsg)
		if rc != nil {
			defer rc.Stop()
		}
	}

	var pn *components.Proxy
	if mr.EnableProxy {
		pn = mr.runProxy(ctx, localMsg, alias)
		if pn != nil {
			defer pn.Stop()
		}
	}

	var qs *components.QueryCoord
	if mr.EnableQueryCoord {
		qs = mr.runQueryCoord(ctx, localMsg)
		if qs != nil {
			defer qs.Stop()
		}
	}

	var qn *components.QueryNode
	if mr.EnableQueryNode {
		qn = mr.runQueryNode(ctx, localMsg, alias)
		if qn != nil {
			defer qn.Stop()
		}
	}

	var ds *components.DataCoord
	if mr.EnableDataCoord {
		ds = mr.runDataCoord(ctx, localMsg)
		if ds != nil {
			defer ds.Stop()
		}
	}

	var dn *components.DataNode
	if mr.EnableDataNode {
		dn = mr.runDataNode(ctx, localMsg, alias)
		if dn != nil {
			defer dn.Stop()
		}
	}

	var is *components.IndexCoord
	if mr.EnableIndexCoord {
		is = mr.runIndexCoord(ctx, localMsg)
		if is != nil {
			defer is.Stop()
		}
	}

	var in *components.IndexNode
	if mr.EnableIndexNode {
		in = mr.runIndexNode(ctx, localMsg, alias)
		if in != nil {
			defer in.Stop()
		}
	}

	var mss *components.MsgStream
	if mr.EnableMsgStreamCoord {
		mss = mr.runMsgStreamCoord(ctx)
		if mss != nil {
			defer mss.Stop()
		}
	}

	if localMsg {
		standaloneHealthzHandler := func(w http.ResponseWriter, r *http.Request) {
			if rc == nil {
				rootCoordNotServingHandler(w, r)
				return
			}
			if qs == nil {
				queryCoordNotServingHandler(w, r)
				return
			}
			if ds == nil {
				dataCoordNotServingHandler(w, r)
				return
			}
			if is == nil {
				indexCoordNotServingHandler(w, r)
				return
			}
			// TODO(dragondriver): need to check node state?

			w.WriteHeader(http.StatusOK)
			w.Header().Set(healthz.ContentTypeHeader, healthz.ContentTypeText)
			_, err := fmt.Fprint(w, "OK")
			if err != nil {
				log.Warn("failed to send response",
					zap.Error(err))
			}

			// TODO(dragondriver): handle component states
		}
		http.HandleFunc(healthz.HealthzRouterPath, standaloneHealthzHandler)
	}

	metrics.ServeHTTP()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Error("Get signal to exit\n", zap.String("signal", sig.String()))

	// some deferred Stop has race with context cancel
	cancel()
}

func setLoggerFunc(localMsg bool) func(cfg log.Config) {
	if !localMsg {
		return func(cfg log.Config) {
			log.Info("Set log file to ", zap.String("path", cfg.File.Filename))
			logutil.SetupLogger(&cfg)
			defer log.Sync()
		}
	}
	// no need to setup logger for standalone
	return func(cfg log.Config) {}
}
