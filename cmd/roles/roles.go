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
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/milvus-io/milvus/cmd/components"
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/indexcoord"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/management"
	"github.com/milvus-io/milvus/internal/management/healthz"
	"github.com/milvus-io/milvus/internal/metrics"
	rocksmqimpl "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/proxy"
	querycoord "github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/querynode"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var Params paramtable.ComponentParam

// all milvus related metrics is in a separate registry
var Registry *prometheus.Registry

func init() {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	Registry.MustRegister(prometheus.NewGoCollector())
	metrics.RegisterMetaMetrics(Registry)
	metrics.RegisterStorageMetrics(Registry)
	metrics.RegisterMsgStreamMetrics(Registry)
}

func stopRocksmq() {
	rocksmqimpl.CloseRocksMQ()
}

type component interface {
	healthz.Indicator
	Run() error
	Stop() error
}

func cleanLocalDir(path string) {
	_, statErr := os.Stat(path)
	// path exist, but stat error
	if statErr != nil && !os.IsNotExist(statErr) {
		log.Warn("Check if path exists failed when clean local data cache", zap.Error(statErr))
		panic(statErr)
	}
	// path exist, remove all
	if statErr == nil {
		err := os.RemoveAll(path)
		if err != nil {
			log.Warn("Clean local data cache failed", zap.Error(err))
			panic(err)
		}
		log.Info("Clean local data cache", zap.String("path", path))
	}
}

func runComponent[T component](ctx context.Context,
	localMsg bool,
	params *paramtable.ComponentParam,
	extraInit func(),
	creator func(context.Context, dependency.Factory) (T, error),
	metricRegister func(*prometheus.Registry)) T {
	var role T
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		if extraInit != nil {
			extraInit()
		}
		factory := dependency.NewFactory(localMsg)
		var err error
		role, err = creator(ctx, factory)
		if localMsg {
			params.SetLogConfig(typeutil.StandaloneRole)
		} else {
			params.SetLogConfig(role.GetName())
		}
		if err != nil {
			panic(err)
		}
		wg.Done()
		if err := role.Run(); err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	healthz.Register(role)
	metricRegister(Registry)
	return role
}

// MilvusRoles decides which components are brought up with Milvus.
type MilvusRoles struct {
	EnableRootCoord  bool `env:"ENABLE_ROOT_COORD"`
	EnableProxy      bool `env:"ENABLE_PROXY"`
	EnableQueryCoord bool `env:"ENABLE_QUERY_COORD"`
	EnableQueryNode  bool `env:"ENABLE_QUERY_NODE"`
	EnableDataCoord  bool `env:"ENABLE_DATA_COORD"`
	EnableDataNode   bool `env:"ENABLE_DATA_NODE"`
	EnableIndexCoord bool `env:"ENABLE_INDEX_COORD"`
	EnableIndexNode  bool `env:"ENABLE_INDEX_NODE"`
}

// EnvValue not used now.
func (mr *MilvusRoles) EnvValue(env string) bool {
	env = strings.ToLower(env)
	env = strings.Trim(env, " ")
	return env == "1" || env == "true"
}

func (mr *MilvusRoles) printLDPreLoad() {
	const LDPreLoad = "LD_PRELOAD"
	val, ok := os.LookupEnv(LDPreLoad)
	if ok {
		log.Info("Enable Jemalloc", zap.String("Jemalloc Path", val))
	}
}

func (mr *MilvusRoles) runRootCoord(ctx context.Context, localMsg bool) *components.RootCoord {
	rootcoord.Params.InitOnce()
	return runComponent(ctx, localMsg, &rootcoord.Params, nil, components.NewRootCoord, metrics.RegisterRootCoord)
}

func (mr *MilvusRoles) runProxy(ctx context.Context, localMsg bool, alias string) *components.Proxy {
	proxy.Params.InitOnce()
	return runComponent(ctx, localMsg, &proxy.Params,
		func() {
			proxy.Params.ProxyCfg.InitAlias(alias)
		},
		components.NewProxy,
		metrics.RegisterProxy)
}

func (mr *MilvusRoles) runQueryCoord(ctx context.Context, localMsg bool) *components.QueryCoord {
	querycoord.Params.InitOnce()
	return runComponent(ctx, localMsg, querycoord.Params, nil, components.NewQueryCoord, metrics.RegisterQueryCoord)
}

func (mr *MilvusRoles) runQueryNode(ctx context.Context, localMsg bool, alias string) *components.QueryNode {
	querynode.Params.InitOnce()
	rootPath := indexnode.Params.LocalStorageCfg.Path
	queryDataLocalPath := filepath.Join(rootPath, typeutil.QueryNodeRole)
	cleanLocalDir(queryDataLocalPath)

	return runComponent(ctx, localMsg, &querynode.Params,
		func() {
			querynode.Params.QueryNodeCfg.InitAlias(alias)
		},
		components.NewQueryNode,
		metrics.RegisterQueryNode)
}

func (mr *MilvusRoles) runDataCoord(ctx context.Context, localMsg bool) *components.DataCoord {
	datacoord.Params.InitOnce()
	return runComponent(ctx, localMsg, &datacoord.Params, nil, components.NewDataCoord, metrics.RegisterDataCoord)
}

func (mr *MilvusRoles) runDataNode(ctx context.Context, localMsg bool, alias string) *components.DataNode {
	datanode.Params.InitOnce()
	return runComponent(ctx, localMsg, &datanode.Params,
		func() {
			datanode.Params.DataNodeCfg.InitAlias(alias)
		},
		components.NewDataNode,
		metrics.RegisterDataNode)
}

func (mr *MilvusRoles) runIndexCoord(ctx context.Context, localMsg bool) *components.IndexCoord {
	indexcoord.Params.InitOnce()
	return runComponent(ctx, localMsg, &indexcoord.Params, nil, components.NewIndexCoord, metrics.RegisterIndexCoord)
}

func (mr *MilvusRoles) runIndexNode(ctx context.Context, localMsg bool, alias string) *components.IndexNode {
	indexnode.Params.InitOnce()
	rootPath := indexnode.Params.LocalStorageCfg.Path
	indexDataLocalPath := filepath.Join(rootPath, typeutil.IndexNodeRole)
	cleanLocalDir(indexDataLocalPath)

	return runComponent(ctx, localMsg, &indexnode.Params,
		func() {
			indexnode.Params.IndexNodeCfg.InitAlias(alias)
		},
		components.NewIndexNode,
		metrics.RegisterIndexNode)
}

// Run Milvus components.
func (mr *MilvusRoles) Run(local bool, alias string) {
	log.Info("starting running Milvus components")
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		// some deferred Stop has race with context cancel
		cancel()
	}()
	mr.printLDPreLoad()

	// only standalone enable localMsg
	if local {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}
		Params.Init()

		if rocksPath := Params.RocksmqPath(); rocksPath != "" {
			if err := rocksmqimpl.InitRocksMQ(rocksPath); err != nil {
				panic(err)
			}
			defer stopRocksmq()
		}

		if Params.EtcdCfg.UseEmbedEtcd {
			// Start etcd server.
			etcd.InitEtcdServer(
				Params.EtcdCfg.UseEmbedEtcd,
				Params.EtcdCfg.ConfigPath,
				Params.EtcdCfg.DataDir,
				Params.EtcdCfg.EtcdLogPath,
				Params.EtcdCfg.EtcdLogLevel)
			defer etcd.StopEtcdServer()
		}
	} else {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}
	}

	management.ServeHTTP()

	if os.Getenv(metricsinfo.DeployModeEnvKey) == metricsinfo.StandaloneDeployMode {
		closer := trace.InitTracing("standalone")
		if closer != nil {
			defer closer.Close()
		}
	}

	var rc *components.RootCoord
	if mr.EnableRootCoord {
		rc = mr.runRootCoord(ctx, local)
		if rc != nil {
			defer rc.Stop()
		}
	}

	var pn *components.Proxy
	if mr.EnableProxy {
		pctx := log.WithModule(ctx, "Proxy")
		pn = mr.runProxy(pctx, local, alias)
		if pn != nil {
			defer pn.Stop()
		}
	}

	var qs *components.QueryCoord
	if mr.EnableQueryCoord {
		qs = mr.runQueryCoord(ctx, local)
		if qs != nil {
			defer qs.Stop()
		}
	}

	var qn *components.QueryNode
	if mr.EnableQueryNode {
		qn = mr.runQueryNode(ctx, local, alias)
		if qn != nil {
			defer qn.Stop()
		}
	}

	var ds *components.DataCoord
	if mr.EnableDataCoord {
		ds = mr.runDataCoord(ctx, local)
		if ds != nil {
			defer ds.Stop()
		}
	}

	var dn *components.DataNode
	if mr.EnableDataNode {
		dn = mr.runDataNode(ctx, local, alias)
		if dn != nil {
			defer dn.Stop()
		}
	}

	var is *components.IndexCoord
	if mr.EnableIndexCoord {
		is = mr.runIndexCoord(ctx, local)
		if is != nil {
			defer is.Stop()
		}
	}

	var in *components.IndexNode
	if mr.EnableIndexNode {
		in = mr.runIndexNode(ctx, local, alias)
		if in != nil {
			defer in.Stop()
		}
	}

	metrics.Register(Registry)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Error("Get signal to exit\n", zap.String("signal", sig.String()))
}
