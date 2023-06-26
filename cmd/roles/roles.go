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
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/components"
	"github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/http/healthz"
	rocksmqimpl "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	_ "github.com/milvus-io/milvus/pkg/util/symbolizer" // support symbolizer and crash dump
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// all milvus related metrics is in a separate registry
var Registry *metrics.MilvusRegistry

func init() {
	Registry = metrics.NewMilvusRegistry()
	metrics.Register(Registry.GoRegistry)
	metrics.RegisterMetaMetrics(Registry.GoRegistry)
	metrics.RegisterMsgStreamMetrics(Registry.GoRegistry)
	metrics.RegisterStorageMetrics(Registry.GoRegistry)
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
	runWg *sync.WaitGroup,
	creator func(context.Context, dependency.Factory) (T, error),
	metricRegister func(*prometheus.Registry),
) T {
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
		runWg.Done()
	}()
	wg.Wait()

	healthz.Register(role)
	metricRegister(Registry.GoRegistry)
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

func (mr *MilvusRoles) runRootCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.RootCoord {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewRootCoord, metrics.RegisterRootCoord)
}

func (mr *MilvusRoles) runProxy(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.Proxy {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewProxy, metrics.RegisterProxy)
}

func (mr *MilvusRoles) runQueryCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.QueryCoord {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewQueryCoord, metrics.RegisterQueryCoord)
}

func (mr *MilvusRoles) runQueryNode(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.QueryNode {
	wg.Add(1)
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	queryDataLocalPath := filepath.Join(rootPath, typeutil.QueryNodeRole)
	cleanLocalDir(queryDataLocalPath)

	return runComponent(ctx, localMsg, wg, components.NewQueryNode, metrics.RegisterQueryNode)
}

func (mr *MilvusRoles) runDataCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.DataCoord {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewDataCoord, metrics.RegisterDataCoord)
}

func (mr *MilvusRoles) runDataNode(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.DataNode {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewDataNode, metrics.RegisterDataNode)
}

func (mr *MilvusRoles) runIndexCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.IndexCoord {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewIndexCoord, func(registry *prometheus.Registry) {})
}

func (mr *MilvusRoles) runIndexNode(ctx context.Context, localMsg bool, wg *sync.WaitGroup) *components.IndexNode {
	wg.Add(1)
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	indexDataLocalPath := filepath.Join(rootPath, typeutil.IndexNodeRole)
	cleanLocalDir(indexDataLocalPath)

	return runComponent(ctx, localMsg, wg, components.NewIndexNode, metrics.RegisterIndexNode)
}

func (mr *MilvusRoles) setupLogger() {
	params := paramtable.Get()
	logConfig := log.Config{
		Level:     params.LogCfg.Level.GetValue(),
		GrpcLevel: params.LogCfg.GrpcLogLevel.GetValue(),
		Format:    params.LogCfg.Format.GetValue(),
		Stdout:    params.LogCfg.Stdout.GetAsBool(),
		File: log.FileLogConfig{
			RootPath:   params.LogCfg.RootPath.GetValue(),
			MaxSize:    params.LogCfg.MaxSize.GetAsInt(),
			MaxDays:    params.LogCfg.MaxAge.GetAsInt(),
			MaxBackups: params.LogCfg.MaxBackups.GetAsInt(),
		},
	}
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

// Register serves prometheus http service
func setupPrometheusHTTPServer(r *metrics.MilvusRegistry) {
	http.Register(&http.Handler{
		Path:    "/metrics",
		Handler: promhttp.HandlerFor(r, promhttp.HandlerOpts{}),
	})
	http.Register(&http.Handler{
		Path:    "/metrics_default",
		Handler: promhttp.Handler(),
	})
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
		paramtable.SetRole(typeutil.StandaloneRole)
	} else {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}
		paramtable.Init()
	}

	http.ServeHTTP()

	var rc *components.RootCoord
	var wg sync.WaitGroup
	if mr.EnableRootCoord {
		rc = mr.runRootCoord(ctx, local, &wg)
		if rc != nil {
			defer rc.Stop()
		}
	}

	var pn *components.Proxy
	if mr.EnableProxy {
		pn = mr.runProxy(ctx, local, &wg)
		if pn != nil {
			defer pn.Stop()
		}
	}

	var qs *components.QueryCoord
	if mr.EnableQueryCoord {
		qs = mr.runQueryCoord(ctx, local, &wg)
		if qs != nil {
			defer qs.Stop()
		}
	}

	var qn *components.QueryNode
	if mr.EnableQueryNode {
		qn = mr.runQueryNode(ctx, local, &wg)
		if qn != nil {
			defer qn.Stop()
		}
	}

	var ds *components.DataCoord
	if mr.EnableDataCoord {
		ds = mr.runDataCoord(ctx, local, &wg)
		if ds != nil {
			defer ds.Stop()
		}
	}

	var dn *components.DataNode
	if mr.EnableDataNode {
		dn = mr.runDataNode(ctx, local, &wg)
		if dn != nil {
			defer dn.Stop()
		}
	}

	var is *components.IndexCoord
	if mr.EnableIndexCoord {
		is = mr.runIndexCoord(ctx, local, &wg)
		if is != nil {
			defer is.Stop()
		}
	}

	var in *components.IndexNode
	if mr.EnableIndexNode {
		in = mr.runIndexNode(ctx, local, &wg)
		if in != nil {
			defer in.Stop()
		}
	}

	wg.Wait()

	mr.setupLogger()
	tracer.Init()
	setupPrometheusHTTPServer(Registry)

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
