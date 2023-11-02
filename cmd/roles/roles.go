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
	internalmetrics "github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/generic"
	"github.com/milvus-io/milvus/pkg/util/logutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	_ "github.com/milvus-io/milvus/pkg/util/symbolizer" // support symbolizer and crash dump
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// all milvus related metrics is in a separate registry
var Registry *internalmetrics.MilvusRegistry

func init() {
	Registry = internalmetrics.NewMilvusRegistry()
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
) component {
	var role T

	sign := make(chan struct{})
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
		close(sign)
		if err := role.Run(); err != nil {
			panic(err)
		}
		runWg.Done()
	}()

	<-sign

	healthz.Register(role)
	metricRegister(Registry.GoRegistry)
	if generic.IsZero(role) {
		return nil
	}
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

	Local    bool
	Alias    string
	Embedded bool

	closed chan struct{}
	once   sync.Once
}

// NewMilvusRoles creates a new MilvusRoles with private fields initialized.
func NewMilvusRoles() *MilvusRoles {
	mr := &MilvusRoles{
		closed: make(chan struct{}),
	}
	return mr
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

func (mr *MilvusRoles) runRootCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewRootCoord, metrics.RegisterRootCoord)
}

func (mr *MilvusRoles) runProxy(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewProxy, metrics.RegisterProxy)
}

func (mr *MilvusRoles) runQueryCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewQueryCoord, metrics.RegisterQueryCoord)
}

func (mr *MilvusRoles) runQueryNode(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	// clear local storage
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	queryDataLocalPath := filepath.Join(rootPath, typeutil.QueryNodeRole)
	cleanLocalDir(queryDataLocalPath)
	// clear mmap dir
	mmapDir := paramtable.Get().QueryNodeCfg.MmapDirPath.GetValue()
	if len(mmapDir) > 0 {
		cleanLocalDir(mmapDir)
	}

	return runComponent(ctx, localMsg, wg, components.NewQueryNode, metrics.RegisterQueryNode)
}

func (mr *MilvusRoles) runDataCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewDataCoord, metrics.RegisterDataCoord)
}

func (mr *MilvusRoles) runDataNode(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewDataNode, metrics.RegisterDataNode)
}

func (mr *MilvusRoles) runIndexCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewIndexCoord, func(registry *prometheus.Registry) {})
}

func (mr *MilvusRoles) runIndexNode(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
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
func setupPrometheusHTTPServer(r *internalmetrics.MilvusRegistry) {
	log.Info("setupPrometheusHTTPServer")
	http.Register(&http.Handler{
		Path:    "/metrics",
		Handler: promhttp.HandlerFor(r, promhttp.HandlerOpts{}),
	})
	http.Register(&http.Handler{
		Path:    "/metrics_default",
		Handler: promhttp.Handler(),
	})
}

func (mr *MilvusRoles) handleSignals() func() {
	sign := make(chan struct{})
	done := make(chan struct{})

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		defer close(done)
		for {
			select {
			case <-sign:
				log.Info("All cleanup done, handleSignals goroutine quit")
				return
			case sig := <-sc:
				log.Warn("Get signal to exit", zap.String("signal", sig.String()))
				mr.once.Do(func() {
					close(mr.closed)
					// reset other signals, only handle SIGINT from now
					signal.Reset(syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGTERM)
				})
			}
		}
	}()
	return func() {
		close(sign)
		<-done
	}
}

// Run Milvus components.
func (mr *MilvusRoles) Run() {
	// start signal handler, defer close func
	closeFn := mr.handleSignals()
	defer closeFn()

	log.Info("starting running Milvus components")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mr.printLDPreLoad()

	// only standalone enable localMsg
	if mr.Local {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}

		if mr.Embedded {
			// setup config for embedded milvus
			paramtable.InitWithBaseTable(paramtable.NewBaseTable(paramtable.Files([]string{"embedded-milvus.yaml"})))
		} else {
			paramtable.Init()
		}
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
	setupPrometheusHTTPServer(Registry)

	var wg sync.WaitGroup
	local := mr.Local

	var rootCoord, queryCoord, indexCoord, dataCoord component
	var proxy, dataNode, indexNode, queryNode component
	if mr.EnableRootCoord {
		rootCoord = mr.runRootCoord(ctx, local, &wg)
	}

	if mr.EnableDataCoord {
		dataCoord = mr.runDataCoord(ctx, local, &wg)
	}

	if mr.EnableIndexCoord {
		indexCoord = mr.runIndexCoord(ctx, local, &wg)
	}

	if mr.EnableQueryCoord {
		queryCoord = mr.runQueryCoord(ctx, local, &wg)
	}

	if mr.EnableQueryNode {
		queryNode = mr.runQueryNode(ctx, local, &wg)
	}

	if mr.EnableDataNode {
		dataNode = mr.runDataNode(ctx, local, &wg)
	}
	if mr.EnableIndexNode {
		indexNode = mr.runIndexNode(ctx, local, &wg)
	}

	if mr.EnableProxy {
		proxy = mr.runProxy(ctx, local, &wg)
	}

	wg.Wait()

	mr.setupLogger()
	tracer.Init()

	paramtable.SetCreateTime(time.Now())
	paramtable.SetUpdateTime(time.Now())

	<-mr.closed

	// stop coordinators first
	coordinators := []component{rootCoord, dataCoord, indexCoord, queryCoord}
	for idx, coord := range coordinators {
		log.Warn("stop processing")
		if coord != nil {
			log.Info("stop coord", zap.Int("idx", idx), zap.Any("coord", coord))
			coord.Stop()
		}
	}
	log.Info("All coordinators have stopped")

	// stop nodes
	nodes := []component{queryNode, indexNode, dataNode}
	for idx, node := range nodes {
		if node != nil {
			log.Info("stop node", zap.Int("idx", idx), zap.Any("node", node))
			node.Stop()
		}
	}
	log.Info("All nodes have stopped")

	if proxy != nil {
		proxy.Stop()
		log.Info("proxy stopped!")
	}

	log.Info("Milvus components graceful stop done")
}

func (mr *MilvusRoles) GetRoles() []string {
	roles := make([]string, 0)
	if mr.EnableRootCoord {
		roles = append(roles, typeutil.RootCoordRole)
	}
	if mr.EnableProxy {
		roles = append(roles, typeutil.ProxyRole)
	}
	if mr.EnableQueryCoord {
		roles = append(roles, typeutil.QueryCoordRole)
	}
	if mr.EnableQueryNode {
		roles = append(roles, typeutil.QueryNodeRole)
	}
	if mr.EnableDataCoord {
		roles = append(roles, typeutil.DataCoordRole)
	}
	if mr.EnableDataNode {
		roles = append(roles, typeutil.DataNodeRole)
	}
	if mr.EnableIndexCoord {
		roles = append(roles, typeutil.IndexCoordRole)
	}
	if mr.EnableIndexNode {
		roles = append(roles, typeutil.IndexNodeRole)
	}
	return roles
}
