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
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/components"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/management"
	"github.com/milvus-io/milvus/internal/management/healthz"
	"github.com/milvus-io/milvus/internal/metrics"
	rocksmqimpl "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
)

// all milvus related metrics is in a separate registry
var Registry *prometheus.Registry

func init() {
	Registry = prometheus.NewRegistry()
	Registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	Registry.MustRegister(prometheus.NewGoCollector())
	metrics.RegisterEtcdMetrics(Registry)
}

func stopRocksmq() {
	rocksmqimpl.CloseRocksMQ()
}

type component interface {
	healthz.Indicator
	Run() error
	Stop() error
}

func runComponent[T component](ctx context.Context,
	localMsg bool,
	creator func(context.Context, dependency.Factory) (T, error),
	metricRegister func(*prometheus.Registry)) T {
	var role T
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		factory := dependency.NewFactory(localMsg)
		var err error
		role, err = creator(ctx, factory)
		if localMsg {
			paramtable.SetRole(typeutil.StandaloneRole)
		} else {
			paramtable.SetRole(role.GetName())
		}
		if err != nil {
			panic(err)
		}
		wg.Done()
		_ = role.Run()
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
	return runComponent(ctx, localMsg, components.NewRootCoord, metrics.RegisterRootCoord)
}

func (mr *MilvusRoles) runProxy(ctx context.Context, localMsg bool) *components.Proxy {
	return runComponent(ctx, localMsg, components.NewProxy, metrics.RegisterProxy)
}

func (mr *MilvusRoles) runQueryCoord(ctx context.Context, localMsg bool) *components.QueryCoord {
	return runComponent(ctx, localMsg, components.NewQueryCoord, metrics.RegisterQueryCoord)
}

func (mr *MilvusRoles) runQueryNode(ctx context.Context, localMsg bool) *components.QueryNode {
	return runComponent(ctx, localMsg, components.NewQueryNode, metrics.RegisterQueryNode)
}

func (mr *MilvusRoles) runDataCoord(ctx context.Context, localMsg bool) *components.DataCoord {
	return runComponent(ctx, localMsg, components.NewDataCoord, metrics.RegisterDataCoord)
}

func (mr *MilvusRoles) runDataNode(ctx context.Context, localMsg bool) *components.DataNode {
	return runComponent(ctx, localMsg, components.NewDataNode, metrics.RegisterDataNode)
}

func (mr *MilvusRoles) runIndexCoord(ctx context.Context, localMsg bool) *components.IndexCoord {
	return runComponent(ctx, localMsg, components.NewIndexCoord, func(registry *prometheus.Registry) {})
}

func (mr *MilvusRoles) runIndexNode(ctx context.Context, localMsg bool) *components.IndexNode {
	return runComponent(ctx, localMsg, components.NewIndexNode, metrics.RegisterIndexNode)
}

func (mr *MilvusRoles) setupLogger() {
	logConfig := paramtable.Get().Log
	id := paramtable.GetNodeID()
	roleName := paramtable.GetRole()
	rootPath := logConfig.File.RootPath
	if rootPath != "" {
		logConfig.File.Filename = fmt.Sprintf("%s-%d.log", roleName, id)
	} else {
		logConfig.File.Filename = ""
	}

	logutil.SetupLogger(&logConfig)
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

		paramtable.Init()
		params := paramtable.Get()

		if params.RocksmqEnable() {
			path, err := params.Load("rocksmq.path")
			if err != nil {
				panic(err)
			}

			if err = rocksmqimpl.InitRocksMQ(path); err != nil {
				panic(err)
			}
			defer stopRocksmq()
		}

		if params.EtcdCfg.UseEmbedEtcd.GetAsBool() {
			// Start etcd server.
			etcd.InitEtcdServer(
				params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
				params.EtcdCfg.ConfigPath.GetValue(),
				params.EtcdCfg.DataDir.GetValue(),
				params.EtcdCfg.EtcdLogPath.GetValue(),
				params.EtcdCfg.EtcdLogLevel.GetValue())
			defer etcd.StopEtcdServer()
		}
	} else {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}
		paramtable.Init()
	}

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
		pn = mr.runProxy(pctx, local)
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
		qn = mr.runQueryNode(ctx, local)
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
		dn = mr.runDataNode(ctx, local)
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
		in = mr.runIndexNode(ctx, local)
		if in != nil {
			defer in.Stop()
		}
	}

	mr.setupLogger()

	metrics.Register(Registry)
	management.ServeHTTP()

	paramtable.SetCreateTime(time.Now())
	paramtable.SetUpdateTime(time.Now())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	sig := <-sc
	log.Error("Get signal to exit\n", zap.String("signal", sig.String()))
}
