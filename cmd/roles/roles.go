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
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/cmd/components"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/http/healthz"
	"github.com/milvus-io/milvus/internal/util/dependency"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/initcore"
	internalmetrics "github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	rocksmqimpl "github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper/nmq"
	"github.com/milvus-io/milvus/pkg/tracer"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/expr"
	"github.com/milvus-io/milvus/pkg/util/gc"
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

const (
	TmpInvertedIndexPrefix = "/tmp/milvus/inverted-index/"
)

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
	EnableRootCoord     bool `env:"ENABLE_ROOT_COORD"`
	EnableProxy         bool `env:"ENABLE_PROXY"`
	EnableQueryCoord    bool `env:"ENABLE_QUERY_COORD"`
	EnableQueryNode     bool `env:"ENABLE_QUERY_NODE"`
	EnableDataCoord     bool `env:"ENABLE_DATA_COORD"`
	EnableDataNode      bool `env:"ENABLE_DATA_NODE"`
	EnableIndexCoord    bool `env:"ENABLE_INDEX_COORD"`
	EnableIndexNode     bool `env:"ENABLE_INDEX_NODE"`
	EnableStreamingNode bool `env:"ENABLE_STREAMING_NODE"`

	Local    bool
	Alias    string
	Embedded bool

	ServerType string

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
	cleanLocalDir(TmpInvertedIndexPrefix)

	return runComponent(ctx, localMsg, wg, components.NewQueryNode, metrics.RegisterQueryNode)
}

func (mr *MilvusRoles) runDataCoord(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewDataCoord, metrics.RegisterDataCoord)
}

func (mr *MilvusRoles) runStreamingNode(ctx context.Context, localMsg bool, wg *sync.WaitGroup) component {
	wg.Add(1)
	return runComponent(ctx, localMsg, wg, components.NewStreamingNode, metrics.RegisterStreamingNode)
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
	cleanLocalDir(TmpInvertedIndexPrefix)

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
	params.Watch(params.LogCfg.Level.Key, config.NewHandler("log.level", func(event *config.Event) {
		if !event.HasUpdated || event.EventType == config.DeleteType {
			return
		}
		logLevel, err := zapcore.ParseLevel(event.Value)
		if err != nil {
			log.Warn("failed to parse log level", zap.Error(err))
			return
		}
		log.SetLevel(logLevel)
		log.Info("log level changed", zap.String("level", event.Value))
	}))
}

// Register serves prometheus http service
func setupPrometheusHTTPServer(r *internalmetrics.MilvusRegistry) {
	log.Info("setupPrometheusHTTPServer")
	http.Register(&http.Handler{
		Path:    http.MetricsPath,
		Handler: promhttp.HandlerFor(r, promhttp.HandlerOpts{}),
	})
	http.Register(&http.Handler{
		Path:    http.MetricsDefaultPath,
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
		if paramtable.Get().RocksmqEnable() {
			defer stopRocksmq()
		} else if paramtable.Get().NatsmqEnable() {
			defer nmq.CloseNatsMQ()
		} else {
			panic("only support Rocksmq and Natsmq in standalone mode")
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
		paramtable.SetRole(typeutil.StandaloneRole)
	} else {
		if err := os.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.ClusterDeployMode); err != nil {
			log.Error("Failed to set deploy mode: ", zap.Error(err))
		}
		paramtable.Init()
		paramtable.SetRole(mr.ServerType)
	}

	// Initialize streaming service if enabled.
	if streamingutil.IsStreamingServiceEnabled() {
		streaming.Init()
		defer streaming.Release()
	}

	expr.Init()
	expr.Register("param", paramtable.Get())
	mr.setupLogger()
	http.ServeHTTP()
	setupPrometheusHTTPServer(Registry)

	if paramtable.Get().CommonCfg.GCEnabled.GetAsBool() {
		if paramtable.Get().CommonCfg.GCHelperEnabled.GetAsBool() {
			action := func(GOGC uint32) {
				debug.SetGCPercent(int(GOGC))
			}
			gc.NewTuner(paramtable.Get().CommonCfg.OverloadedMemoryThresholdPercentage.GetAsFloat(), uint32(paramtable.Get().QueryNodeCfg.MinimumGOGCConfig.GetAsInt()), uint32(paramtable.Get().QueryNodeCfg.MaximumGOGCConfig.GetAsInt()), action)
		} else {
			action := func(uint32) {}
			gc.NewTuner(paramtable.Get().CommonCfg.OverloadedMemoryThresholdPercentage.GetAsFloat(), uint32(paramtable.Get().QueryNodeCfg.MinimumGOGCConfig.GetAsInt()), uint32(paramtable.Get().QueryNodeCfg.MaximumGOGCConfig.GetAsInt()), action)
		}
	}

	var wg sync.WaitGroup
	local := mr.Local

	componentMap := make(map[string]component)
	var rootCoord, queryCoord, indexCoord, dataCoord component
	var proxy, dataNode, indexNode, queryNode, streamingNode component
	if mr.EnableRootCoord {
		rootCoord = mr.runRootCoord(ctx, local, &wg)
		componentMap[typeutil.RootCoordRole] = rootCoord
	}

	if mr.EnableDataCoord {
		dataCoord = mr.runDataCoord(ctx, local, &wg)
		componentMap[typeutil.DataCoordRole] = dataCoord
	}

	if mr.EnableIndexCoord {
		indexCoord = mr.runIndexCoord(ctx, local, &wg)
		componentMap[typeutil.IndexCoordRole] = indexCoord
	}

	if mr.EnableQueryCoord {
		queryCoord = mr.runQueryCoord(ctx, local, &wg)
		componentMap[typeutil.QueryCoordRole] = queryCoord
	}

	waitCoordBecomeHealthy := func() {
		for {
			select {
			case <-ctx.Done():
				log.Info("wait all coord become healthy loop quit")
				return
			default:
				rcState := rootCoord.Health(ctx)
				dcState := dataCoord.Health(ctx)
				icState := indexCoord.Health(ctx)
				qcState := queryCoord.Health(ctx)

				if rcState == commonpb.StateCode_Healthy && dcState == commonpb.StateCode_Healthy && icState == commonpb.StateCode_Healthy && qcState == commonpb.StateCode_Healthy {
					log.Info("all coord become healthy")
					return
				}
				log.Info("wait all coord become healthy", zap.String("rootCoord", rcState.String()), zap.String("dataCoord", dcState.String()), zap.String("indexCoord", icState.String()), zap.String("queryCoord", qcState.String()))
				time.Sleep(time.Second)
			}
		}
	}

	// In standalone mode, block the start process until the new coordinator is active to avoid the coexistence of the old coordinator and the new node/proxy
	// 1. In the start/restart process, the new coordinator will become active immediately and will not be blocked
	// 2. In the rolling upgrade process, the new coordinator will not be active until the old coordinator is down, and it will be blocked
	if mr.Local {
		waitCoordBecomeHealthy()
	}

	if mr.EnableQueryNode {
		queryNode = mr.runQueryNode(ctx, local, &wg)
		componentMap[typeutil.QueryNodeRole] = queryNode
	}

	if mr.EnableDataNode {
		dataNode = mr.runDataNode(ctx, local, &wg)
		componentMap[typeutil.DataNodeRole] = dataNode
	}
	if mr.EnableIndexNode {
		indexNode = mr.runIndexNode(ctx, local, &wg)
		componentMap[typeutil.IndexNodeRole] = indexNode
	}

	if mr.EnableProxy {
		proxy = mr.runProxy(ctx, local, &wg)
		componentMap[typeutil.ProxyRole] = proxy
	}

	if mr.EnableStreamingNode {
		streamingNode = mr.runStreamingNode(ctx, local, &wg)
		componentMap[typeutil.StreamingNodeRole] = streamingNode
	}

	wg.Wait()

	http.RegisterStopComponent(func(role string) error {
		if len(role) == 0 || componentMap[role] == nil {
			return fmt.Errorf("stop component [%s] in [%s] is not supported", role, mr.ServerType)
		}

		log.Info("unregister component before stop", zap.String("role", role))
		healthz.UnRegister(role)
		return componentMap[role].Stop()
	})

	http.RegisterCheckComponentReady(func(role string) error {
		if len(role) == 0 || componentMap[role] == nil {
			return fmt.Errorf("check component state for [%s] in [%s] is not supported", role, mr.ServerType)
		}

		// for coord component, if it's in standby state, it will return StateCode_StandBy
		code := componentMap[role].Health(context.TODO())
		if code != commonpb.StateCode_Healthy {
			return fmt.Errorf("component [%s] in [%s] is not healthy", role, mr.ServerType)
		}

		return nil
	})

	tracer.Init()
	paramtable.Get().WatchKeyPrefix("trace", config.NewHandler("tracing handler", func(e *config.Event) {
		params := paramtable.Get()

		exp, err := tracer.CreateTracerExporter(params)
		if err != nil {
			log.Warn("Init tracer faield", zap.Error(err))
			return
		}

		// close old provider
		err = tracer.CloseTracerProvider(context.Background())
		if err != nil {
			log.Warn("Close old provider failed, stop reset", zap.Error(err))
			return
		}

		tracer.SetTracerProvider(exp, params.TraceCfg.SampleFraction.GetAsFloat())
		log.Info("Reset tracer finished", zap.String("Exporter", params.TraceCfg.Exporter.GetValue()), zap.Float64("SampleFraction", params.TraceCfg.SampleFraction.GetAsFloat()))

		if paramtable.GetRole() == typeutil.QueryNodeRole || paramtable.GetRole() == typeutil.StandaloneRole {
			initcore.InitTraceConfig(params)
			log.Info("Reset segcore tracer finished", zap.String("Exporter", params.TraceCfg.Exporter.GetValue()))
		}
	}))

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
	nodes := []component{queryNode, indexNode, dataNode, streamingNode}
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

	// close reused etcd client
	kvfactory.CloseEtcdClient()

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
