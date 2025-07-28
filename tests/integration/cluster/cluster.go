package cluster

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration/cluster/process"
)

const (
	MilvusWorkDirEnvKey = "MILVUS_WORK_DIR"
)

type (
	MiniClusterV3Option func(*MiniClusterV3)
	ClusterOperationOpt func(*clusterOperationOpt)
)

// clusterOperationOpt is the option for the mini cluster v3.
type clusterOperationOpt struct {
	waitReady bool
}

// WithoutWaitForReady sets the wait ready option for the cluster operation.
func WithoutWaitForReady() ClusterOperationOpt {
	return func(opt *clusterOperationOpt) {
		opt.waitReady = false
	}
}

func WithRootPath(rootPath string) MiniClusterV3Option {
	return func(c *MiniClusterV3) {
		c.rootPath = rootPath
	}
}

func WithWorkDir(workDir string) MiniClusterV3Option {
	return func(c *MiniClusterV3) {
		c.workDir = workDir
	}
}

func WithEtcdCli(etcdCli *clientv3.Client) MiniClusterV3Option {
	return func(c *MiniClusterV3) {
		c.EtcdCli = etcdCli
	}
}

func WithExtraEnv(env map[string]string) MiniClusterV3Option {
	return func(c *MiniClusterV3) {
		c.extraEnv = env
	}
}

// NewMiniClusterV3 creates a new mini cluster v3.
func NewMiniClusterV3(
	ctx context.Context,
	opts ...MiniClusterV3Option,
) *MiniClusterV3 {
	client := &MiniClusterV3{
		ctx:                   ctx,
		mu:                    sync.Mutex{},
		nodeID:                0,
		workDir:               os.Getenv(MilvusWorkDirEnvKey),
		configRefreshInterval: 100 * time.Millisecond,
		extraEnv:              make(map[string]string),
		mixcoord:              make(map[int64]*process.MixcoordProcess),
		proxy:                 make(map[int64]*process.ProxyProcess),
		datanode:              make(map[int64]*process.DataNodeProcess),
		querynode:             make(map[int64]*process.QueryNodeProcess),
		streamingnode:         make(map[int64]*process.StreamingNodeProcess),
	}
	for _, opt := range opts {
		opt(client)
	}
	client.init()
	client.Logger().Info("init mini cluster v3 done")
	return client
}

type MiniClusterV3 struct {
	log.Binder

	ctx                   context.Context
	mu                    sync.Mutex
	rootPath              string
	configRefreshInterval time.Duration
	extraEnv              map[string]string

	defaultMixCoord      *process.MixcoordProcess
	defaultProxy         *process.ProxyProcess
	defaultDataNode      *process.DataNodeProcess
	defaultQueryNode     *process.QueryNodeProcess
	defaultStreamingNode *process.StreamingNodeProcess

	nodeID        int64
	workDir       string
	mixcoord      map[int64]*process.MixcoordProcess
	proxy         map[int64]*process.ProxyProcess
	datanode      map[int64]*process.DataNodeProcess
	querynode     map[int64]*process.QueryNodeProcess
	streamingnode map[int64]*process.StreamingNodeProcess

	metaWatcher      MetaWatcher
	milvusClientConn *grpc.ClientConn

	EtcdCli             *clientv3.Client
	ChunkManager        storage.ChunkManager
	MilvusClient        milvuspb.MilvusServiceClient
	MixCoordClient      types.MixCoordClient
	ProxyClient         types.ProxyClient
	DataNodeClient      types.DataNodeClient
	QueryNodeClient     types.QueryNodeClient
	StreamingNodeClient types.QueryNodeClient
}

func (c *MiniClusterV3) init() {
	if c.workDir == "" {
		panic("work dir is not set")
	}
	if c.rootPath == "" {
		c.rootPath = fmt.Sprintf("integration-%s", uuid.New())
	}
	if c.EtcdCli == nil {
		c.EtcdCli, _ = kvfactory.GetEtcdAndPath()
	}
	c.SetLogger(c.Logger().With(
		log.FieldComponent(process.MilvusClusterComponent),
		zap.String("rootPath", c.rootPath)))

	logger := c.Logger().With(zap.String("operation", "Init"))
	logger.Info("init mini cluster v3...", zap.Any("extraEnv", c.extraEnv))
	now := time.Now()

	c.defaultMixCoord = c.AddMixCoord(WithoutWaitForReady())
	c.defaultProxy = c.AddProxy(WithoutWaitForReady())
	c.defaultDataNode = c.AddDataNode(WithoutWaitForReady())
	c.defaultQueryNode = c.AddQueryNode(WithoutWaitForReady())
	c.defaultStreamingNode = c.AddStreamingNode(WithoutWaitForReady())
	c.Logger().Info("set default node for mini cluster v3 done", zap.Duration("cost", time.Since(now)))
	now = time.Now()

	c.initClients()
	logger.Info("wait for all client ready", zap.Duration("cost", time.Since(now)))
	now = time.Now()

	c.metaWatcher = &EtcdMetaWatcher{
		rootPath: c.rootPath,
		etcdCli:  c.EtcdCli,
	}

	// init chunk manager
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	cli, err := chunkManagerFactory.NewPersistentStorageChunkManager(c.ctx)
	if err != nil {
		panic(err)
	}
	c.ChunkManager = cli
	logger.Info("init mini cluster v3 done", zap.Duration("cost", time.Since(now)))
}

// initClients initializes the clients.
func (c *MiniClusterV3) initClients() {
	c.initMilvusClient()
	c.initInternalClient()
}

// initMilvusClient initializes the milvus client.
func (c *MiniClusterV3) initMilvusClient() {
	if c.milvusClientConn != nil {
		return
	}

	_, err := c.defaultProxy.GetAddress(context.Background())
	if err != nil {
		panic(err)
	}

	// wait for proxy ready.
	clientConn, err := grpc.DialContext(context.Background(), "localhost:19530", getGrpcDialOpt()...)
	if err != nil {
		panic(err)
	}
	c.MilvusClient = milvuspb.NewMilvusServiceClient(clientConn)
	c.milvusClientConn = clientConn
}

// initInternalClient initializes the internal client.
func (c *MiniClusterV3) initInternalClient() {
	if c.MixCoordClient == nil {
		c.MixCoordClient = c.defaultMixCoord.MustGetClient(c.ctx)
	}
	if c.DataNodeClient == nil {
		c.DataNodeClient = c.defaultDataNode.MustGetClient(c.ctx)
	}
	if c.StreamingNodeClient == nil {
		c.StreamingNodeClient = c.defaultStreamingNode.MustGetClient(c.ctx)
	}
	if c.QueryNodeClient == nil {
		c.QueryNodeClient = c.defaultQueryNode.MustGetClient(c.ctx)
	}
	if c.ProxyClient == nil {
		c.ProxyClient = c.defaultProxy.MustGetClient(c.ctx)
	}
}

// ModifyMilvusConfig modifies the milvus config.
// Meanwhile it will return a guard function to revert the config into default.
// It doesn't promise that the configuration will be applied immediately,
// milvus may not support the dynamic configuration change for some configurations or some configration may be applied slowly.
// If you want to ensure the config is applied, you should restart the target component or wait it to be ready.
func (c *MiniClusterV3) MustModifyMilvusConfig(kvs map[string]string) func() {
	keys := make([]string, 0, len(kvs))
	for key, value := range kvs {
		key = path.Join(c.rootPath, "config", strings.ToUpper(strings.ReplaceAll(key, ".", "_")))
		if _, err := c.EtcdCli.Put(c.ctx, key, value); err != nil {
			panic(fmt.Sprintf("failed to modify milvus config: %v", err))
		}
		c.Logger().Info("modify milvus config done", zap.String("key", key), zap.String("value", value))
		keys = append(keys, key)
	}
	// wait for the config to be refreshed.
	time.Sleep(c.configRefreshInterval * 2)

	return func() {
		for _, key := range keys {
			if _, err := c.EtcdCli.Delete(c.ctx, key); err != nil {
				panic(fmt.Sprintf("failed to revert milvus config: %v", err))
			}
			c.Logger().Info("revert milvus config done", zap.String("key", key))
		}
		// wait for the config to be reverted.
		time.Sleep(c.configRefreshInterval * 2)
	}
}

func (c *MiniClusterV3) RootPath() string {
	return c.rootPath
}

func (c *MiniClusterV3) ShowSessions() ([]*sessionutil.SessionRaw, error) {
	return c.metaWatcher.ShowSessions()
}

func (c *MiniClusterV3) ShowReplicas() ([]*querypb.Replica, error) {
	return c.metaWatcher.ShowReplicas()
}

// ShowSegments shows the segments of a collection.
func (c *MiniClusterV3) ShowSegments(collectionName string) ([]*datapb.SegmentInfo, error) {
	resp, err := c.MilvusClient.ShowCollections(c.ctx, &milvuspb.ShowCollectionsRequest{
		CollectionNames: []string{collectionName},
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, err
	}
	return c.metaWatcher.ShowSegments(resp.CollectionIds[0])
}

func (c *MiniClusterV3) GetContext() context.Context {
	return c.ctx
}

func (c *MiniClusterV3) Reset() {
	logger := c.Logger().With(zap.String("operation", "Reset"))

	logger.Info("reset mini cluster v3...")
	now := time.Now()

	c.clearRedundantNodes()
	logger.Info("clear redundant nodes done", zap.Duration("cost", time.Since(now)))
	now = time.Now()

	c.resetDefaultNodes()
	logger.Info("reset default nodes done", zap.Duration("cost", time.Since(now)))
	now = time.Now()

	c.initClients()
	// wait for all client ready.
	logger.Info("wait for all client ready", zap.Duration("cost", time.Since(now)))
}

// clearRedundantNodes clears redundant nodes, only keep one working node for each role.
func (c *MiniClusterV3) clearRedundantNodes() {
	wg := sync.WaitGroup{}
	clearNodes := func(defaultNode *process.MilvusProcess, nodes []*process.MilvusProcess, new func()) {
		defer wg.Done()

		workingNode := int64(-1)
		// clear redundant nodes, only keep one working node for each role.
		c.mu.Lock()
		if defaultNode.IsWorking() {
			// use the old default node as the working node at highest priority.
			workingNode = defaultNode.GetNodeID()
		}
		for _, node := range nodes {
			if node.IsWorking() {
				if workingNode == -1 {
					// if there's no default working node, use the first working node as the working node.
					workingNode = node.GetNodeID()
					continue
				}

				// stop all redundant nodes.
				if workingNode != node.GetNodeID() {
					wg.Add(1)
					go func() {
						defer wg.Done()
						node.Stop(time.Second * 10)
					}()
				}
			}
		}
		c.mu.Unlock()

		// if no working node, add a new node.
		if workingNode == -1 {
			new()
		}
	}

	wg.Add(5)
	go clearNodes(c.defaultMixCoord.MilvusProcess, lo.MapToSlice(c.mixcoord, func(_ int64, node *process.MixcoordProcess) *process.MilvusProcess {
		return node.MilvusProcess
	}), func() { c.AddMixCoord(WithoutWaitForReady()) })
	go clearNodes(c.defaultDataNode.MilvusProcess, lo.MapToSlice(c.datanode, func(_ int64, node *process.DataNodeProcess) *process.MilvusProcess {
		return node.MilvusProcess
	}), func() { c.AddDataNode(WithoutWaitForReady()) })
	go clearNodes(c.defaultQueryNode.MilvusProcess, lo.MapToSlice(c.querynode, func(_ int64, node *process.QueryNodeProcess) *process.MilvusProcess {
		return node.MilvusProcess
	}), func() { c.AddQueryNode(WithoutWaitForReady()) })
	go clearNodes(c.defaultStreamingNode.MilvusProcess, lo.MapToSlice(c.streamingnode, func(_ int64, node *process.StreamingNodeProcess) *process.MilvusProcess {
		return node.MilvusProcess
	}), func() { c.AddStreamingNode(WithoutWaitForReady()) })
	go clearNodes(c.defaultProxy.MilvusProcess, lo.MapToSlice(c.proxy, func(_ int64, node *process.ProxyProcess) *process.MilvusProcess {
		return node.MilvusProcess
	}), func() { c.AddProxy(WithoutWaitForReady()) })
	wg.Wait()
}

func (c *MiniClusterV3) resetDefaultNodes() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.defaultMixCoord.IsWorking() {
		for _, node := range c.mixcoord {
			if node.IsWorking() {
				c.defaultMixCoord = node
				break
			}
		}
		c.MixCoordClient = nil
	}

	if !c.defaultDataNode.IsWorking() {
		for _, node := range c.datanode {
			if node.IsWorking() {
				c.defaultDataNode = node
				break
			}
		}
		c.DataNodeClient = nil
	}

	if !c.defaultQueryNode.IsWorking() {
		for _, node := range c.querynode {
			if node.IsWorking() {
				c.defaultQueryNode = node
				break
			}
		}
		c.QueryNodeClient = nil
	}

	if !c.defaultStreamingNode.IsWorking() {
		for _, node := range c.streamingnode {
			if node.IsWorking() {
				c.defaultStreamingNode = node
				break
			}
		}
		c.StreamingNodeClient = nil
	}

	if !c.defaultProxy.IsWorking() {
		for _, node := range c.proxy {
			if node.IsWorking() {
				c.defaultProxy = node
				break
			}
		}
		c.milvusClientConn.Close()
		c.MilvusClient = nil
		c.milvusClientConn = nil
		c.ProxyClient = nil
	}
}

func (c *MiniClusterV3) DefaultMixCoord() *process.MixcoordProcess {
	return c.defaultMixCoord
}

func (c *MiniClusterV3) DefaultProxy() *process.ProxyProcess {
	return c.defaultProxy
}

func (c *MiniClusterV3) DefaultDataNode() *process.DataNodeProcess {
	return c.defaultDataNode
}

func (c *MiniClusterV3) DefaultQueryNode() *process.QueryNodeProcess {
	return c.defaultQueryNode
}

func (c *MiniClusterV3) DefaultStreamingNode() *process.StreamingNodeProcess {
	return c.defaultStreamingNode
}

func (c *MiniClusterV3) GetAllStreamingAndQueryNodesClient() []types.QueryNodeClient {
	qns := c.GetAllQueryNodes()
	sns := c.GetAllStreamingNodes()
	clients := make([]types.QueryNodeClient, 0, len(qns)+len(sns))
	for _, qn := range qns {
		clients = append(clients, qn.MustGetClient(c.ctx))
	}
	for _, sn := range sns {
		clients = append(clients, sn.MustGetClient(c.ctx))
	}
	return clients
}

func (c *MiniClusterV3) GetAllStreamingNodes() []*process.StreamingNodeProcess {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodes := make([]*process.StreamingNodeProcess, 0, len(c.streamingnode))
	for _, sn := range c.streamingnode {
		if sn.IsWorking() {
			nodes = append(nodes, sn)
		}
	}
	return nodes
}

func (c *MiniClusterV3) GetAllQueryNodes() []*process.QueryNodeProcess {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodes := make([]*process.QueryNodeProcess, 0, len(c.querynode))
	for _, qn := range c.querynode {
		if qn.IsWorking() {
			nodes = append(nodes, qn)
		}
	}
	return nodes
}

func (c *MiniClusterV3) GetAllDataNodes() []*process.DataNodeProcess {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodes := make([]*process.DataNodeProcess, 0, len(c.datanode))
	for _, dn := range c.datanode {
		if dn.IsWorking() {
			nodes = append(nodes, dn)
		}
	}
	return nodes
}

// AddMixCoord adds a mixcoord to the cluster.
// Use WithoutWaitForReady to avoid waiting for the node to be ready.
func (c *MiniClusterV3) AddMixCoord(opts ...ClusterOperationOpt) (mp *process.MixcoordProcess) {
	c.Logger().Info("add mixcoord to the cluster")

	opt := c.getClusterOperationOpt(opts...)
	defer func() {
		if opt.waitReady {
			mp.MustWaitForReady(c.ctx)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	mc := process.NewMixCoordProcess(c.getOptions()...)
	c.mixcoord[mc.GetNodeID()] = mc
	return mc
}

// AddQueryNodes adds multiple query nodes to the cluster.
// Use WithoutWaitForReady to avoid waiting for the node to be ready.
func (c *MiniClusterV3) AddQueryNodes(num int, opts ...ClusterOperationOpt) (mps []*process.QueryNodeProcess) {
	c.Logger().Info("add query nodes to the cluster", zap.Int("num", num))

	opt := c.getClusterOperationOpt(opts...)
	defer func() {
		if opt.waitReady {
			for _, mp := range mps {
				mp.MustWaitForReady(c.ctx)
			}
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	qns := make([]*process.QueryNodeProcess, 0, num)
	for i := 0; i < num; i++ {
		qn := process.NewQueryNodeProcess(c.getOptions()...)
		c.querynode[qn.GetNodeID()] = qn
		qns = append(qns, qn)
	}
	return qns
}

// AddQueryNode adds a query node to the cluster.
// Use WithoutWaitForReady to avoid waiting for the node to be ready.
func (c *MiniClusterV3) AddQueryNode(opts ...ClusterOperationOpt) (mp *process.QueryNodeProcess) {
	c.Logger().Info("add query node to the cluster")

	opt := c.getClusterOperationOpt(opts...)
	defer func() {
		if opt.waitReady {
			mp.MustWaitForReady(c.ctx)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	qn := process.NewQueryNodeProcess(c.getOptions()...)
	c.querynode[qn.GetNodeID()] = qn
	return qn
}

// StopAllQueryNode stops all query nodes.
func (c *MiniClusterV3) StopAllQueryNode(timeout ...time.Duration) {
	wg := sync.WaitGroup{}
	for _, qn := range c.GetAllQueryNodes() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qn.Stop(timeout...)
		}()
	}
	wg.Wait()
}

// AddDataNode adds a data node to the cluster.
// Use WithoutWaitForReady to avoid waiting for the node to be ready.
func (c *MiniClusterV3) AddDataNode(opts ...ClusterOperationOpt) (mp *process.DataNodeProcess) {
	c.Logger().Info("add data node to the cluster")

	opt := c.getClusterOperationOpt(opts...)
	defer func() {
		if opt.waitReady {
			mp.MustWaitForReady(c.ctx)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	dn := process.NewDataNodeProcess(c.getOptions()...)
	c.datanode[dn.GetNodeID()] = dn
	return dn
}

// AddStreamingNode adds a streaming node to the cluster.
// Use WithoutWaitForReady to avoid waiting for the node to be ready.
func (c *MiniClusterV3) AddStreamingNode(opts ...ClusterOperationOpt) (mp *process.StreamingNodeProcess) {
	c.Logger().Info("add streaming node to the cluster")

	opt := c.getClusterOperationOpt(opts...)
	defer func() {
		if opt.waitReady {
			mp.MustWaitForReady(c.ctx)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	sn := process.NewStreamingNodeProcess(c.getOptions()...)
	c.streamingnode[sn.GetNodeID()] = sn
	return sn
}

// AddProxy adds a proxy to the cluster.
// Use WithoutWaitForReady to avoid waiting for the node to be ready.
func (c *MiniClusterV3) AddProxy(opts ...ClusterOperationOpt) (mp *process.ProxyProcess) {
	c.Logger().Info("add proxy to the cluster")

	opt := c.getClusterOperationOpt(opts...)
	defer func() {
		if opt.waitReady {
			mp.MustWaitForReady(c.ctx)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	pn := process.NewProxyProcess(c.getOptions()...)
	c.proxy[pn.GetNodeID()] = pn
	return pn
}

// getClusterOperationOpt gets the cluster operation option.
func (c *MiniClusterV3) getClusterOperationOpt(opts ...ClusterOperationOpt) *clusterOperationOpt {
	coo := &clusterOperationOpt{
		waitReady: true,
	}
	for _, opt := range opts {
		opt(coo)
	}
	return coo
}

func (c *MiniClusterV3) getOptions() []process.Option {
	c.nodeID++
	env := map[string]string{
		"QUOTAANDLIMITS_ENABLED":  "false", // disable the quota limits by default to avoid rpc failure.
		"PROXY_IP":                "localhost",
		"STREAMINGNODE_IP":        "localhost",
		"QUERYNODE_IP":            "localhost",
		"DATANODE_IP":             "localhost",
		"ROOTCOORD_IP":            "localhost",
		"PROXY_PORT":              "19530",
		"ROOTCOORD_DMLCHANNELNUM": "2", // set smaller dml channel num to speed up the test.
		paramtable.MilvusConfigRefreshIntervalEnvKey: c.configRefreshInterval.String(), // set smaller config refresh interval to speed up the test.
	}
	for k, v := range c.extraEnv {
		env[k] = v
	}
	return []process.Option{
		process.WithWorkDir(c.workDir),
		process.WithServerID(c.nodeID),
		process.WithRootPath(c.rootPath),
		process.WithETCDClient(c.EtcdCli),
		process.WithEnvironment(env),
		process.WithStopCallback(func(p *process.MilvusProcess) {
			c.clearProcess(p)
		}),
	}
}

func (c *MiniClusterV3) clearProcess(process *process.MilvusProcess) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, mc := range c.mixcoord {
		if mc.GetNodeID() == process.GetNodeID() {
			delete(c.mixcoord, process.GetNodeID())
		}
	}
	for _, dn := range c.datanode {
		if dn.GetNodeID() == process.GetNodeID() {
			delete(c.datanode, process.GetNodeID())
		}
	}
	for _, qn := range c.querynode {
		if qn.GetNodeID() == process.GetNodeID() {
			delete(c.querynode, process.GetNodeID())
		}
	}
	for _, sn := range c.streamingnode {
		if sn.GetNodeID() == process.GetNodeID() {
			delete(c.streamingnode, process.GetNodeID())
		}
	}
	for _, pn := range c.proxy {
		if pn.GetNodeID() == process.GetNodeID() {
			delete(c.proxy, process.GetNodeID())
		}
	}
}

// Stop stops the cluster.
func (c *MiniClusterV3) Stop() {
	wg := sync.WaitGroup{}
	timeout := time.Second * 10

	type stoppable interface {
		Stop(timeout ...time.Duration) error
	}

	stop := func(p stoppable) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Stop(timeout)
		}()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, dn := range c.datanode {
		stop(dn)
	}
	for _, sn := range c.streamingnode {
		stop(sn)
	}
	for _, qn := range c.querynode {
		stop(qn)
	}
	for _, pn := range c.proxy {
		stop(pn)
	}
	wg.Wait()

	for _, mc := range c.mixcoord {
		stop(mc)
	}
	wg.Wait()
}

func getGrpcDialOpt() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithMax(6),
			grpc_retry.WithBackoff(func(attempt uint) time.Duration {
				return 60 * time.Millisecond * time.Duration(math.Pow(3, float64(attempt)))
			}),
			grpc_retry.WithCodes(codes.Unavailable, codes.ResourceExhausted)),
		),
	}
}
