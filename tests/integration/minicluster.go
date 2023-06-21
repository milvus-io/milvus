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

package integration

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/indexnode"
	proxy2 "github.com/milvus-io/milvus/internal/proxy"
	querycoord "github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/querynodev2"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type Cluster interface {
	Start() error
	Stop() error

	// node add/remove interfaces
	AddRootCoord(types.RootCoordComponent) error
	AddDataCoord(types.DataCoordComponent) error
	AddQueryCoord(types.QueryCoordComponent) error
	//AddIndexCoord(types.IndexCoordComponent) error
	AddDataNode(types.DataNodeComponent) error
	AddQueryNode(types.QueryNodeComponent) error
	AddIndexNode(types.IndexNodeComponent) error

	RemoveRootCoord(types.RootCoordComponent) error
	RemoveDataCoord(types.DataCoordComponent) error
	RemoveQueryCoord(types.QueryCoordComponent) error
	//RemoveIndexCoord(types.IndexCoordComponent) error
	RemoveDataNode(types.DataNodeComponent) error
	RemoveQueryNode(types.QueryNodeComponent) error
	RemoveIndexNode(types.IndexNodeComponent) error

	// UpdateClusterSize change the cluster size, will add/remove nodes to reach given config
	UpdateClusterSize(ClusterConfig) error

	// GetMetaWatcher to observe meta data
	GetMetaWatcher() MetaWatcher
	// todo
	// GetStorageWatcher() StorageWatcher
}

type ClusterConfig struct {
	//ProxyNum int
	// todo coord num can be more than 1 if enable Active-Standby
	//RootCoordNum int
	//DataCoordNum int
	//IndexCoordNum int
	//QueryCoordNum int
	QueryNodeNum int
	DataNodeNum  int
	IndexNodeNum int
}

const (
	EtcdRootPath  = "etcd.rootPath"
	MinioRootPath = "minio.rootPath"
)

type MiniCluster struct {
	ctx context.Context

	mu sync.RWMutex

	params        map[string]string
	clusterConfig ClusterConfig

	factory      dependency.Factory
	ChunkManager storage.ChunkManager

	EtcdCli *clientv3.Client

	Proxy      types.ProxyComponent
	DataCoord  types.DataCoordComponent
	RootCoord  types.RootCoordComponent
	QueryCoord types.QueryCoordComponent

	QueryNodes []types.QueryNodeComponent
	DataNodes  []types.DataNodeComponent
	IndexNodes []types.IndexNodeComponent

	MetaWatcher MetaWatcher
}

var params *paramtable.ComponentParam = paramtable.Get()

type Option func(cluster *MiniCluster)

func StartMiniCluster(ctx context.Context, opts ...Option) (cluster *MiniCluster, err error) {
	cluster = &MiniCluster{
		ctx: ctx,
	}
	params.Init()
	cluster.params = DefaultParams()
	cluster.clusterConfig = DefaultClusterConfig()
	for _, opt := range opts {
		opt(cluster)
	}
	for k, v := range cluster.params {
		params.Save(k, v)
	}
	params.UpdateSourceOptions(config.WithEtcdSource(&config.EtcdInfo{
		KeyPrefix:       cluster.params[EtcdRootPath],
		RefreshInterval: 2 * time.Second,
	}))

	if cluster.factory == nil {
		params.Save(params.LocalStorageCfg.Path.Key, "/tmp/milvus/")
		params.Save(params.CommonCfg.StorageType.Key, "local")
		params.Save(params.MinioCfg.RootPath.Key, "/tmp/milvus/")
		cluster.factory = dependency.NewDefaultFactory(true)
		chunkManager, err := cluster.factory.NewPersistentStorageChunkManager(cluster.ctx)
		if err != nil {
			return nil, err
		}
		cluster.ChunkManager = chunkManager
	}

	if cluster.EtcdCli == nil {
		var etcdCli *clientv3.Client
		etcdCli, err = etcd.GetEtcdClient(
			params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
			params.EtcdCfg.EtcdUseSSL.GetAsBool(),
			params.EtcdCfg.Endpoints.GetAsStrings(),
			params.EtcdCfg.EtcdTLSCert.GetValue(),
			params.EtcdCfg.EtcdTLSKey.GetValue(),
			params.EtcdCfg.EtcdTLSCACert.GetValue(),
			params.EtcdCfg.EtcdTLSMinVersion.GetValue())
		if err != nil {
			return nil, err
		}
		cluster.EtcdCli = etcdCli
	}

	cluster.MetaWatcher = &EtcdMetaWatcher{
		rootPath: cluster.params[EtcdRootPath],
		etcdCli:  cluster.EtcdCli,
	}

	if cluster.RootCoord == nil {
		var rootCoord types.RootCoordComponent
		rootCoord, err = cluster.CreateDefaultRootCoord()
		if err != nil {
			return nil, err
		}
		cluster.RootCoord = rootCoord
	}

	if cluster.DataCoord == nil {
		var dataCoord types.DataCoordComponent
		dataCoord, err = cluster.CreateDefaultDataCoord()
		if err != nil {
			return nil, err
		}
		cluster.DataCoord = dataCoord
	}

	if cluster.QueryCoord == nil {
		var queryCoord types.QueryCoordComponent
		queryCoord, err = cluster.CreateDefaultQueryCoord()
		if err != nil {
			return nil, err
		}
		cluster.QueryCoord = queryCoord
	}

	//if cluster.indexCoord == nil {
	//	var indexCoord types.IndexCoordComponent
	//	indexCoord, err = cluster.CreateDefaultIndexCoord()
	//	if err != nil {
	//		return nil, err
	//	}
	//	cluster.indexCoord = indexCoord
	//}

	if cluster.DataNodes == nil {
		dataNodes := make([]types.DataNodeComponent, 0)
		for i := 0; i < cluster.clusterConfig.DataNodeNum; i++ {
			var dataNode types.DataNodeComponent
			dataNode, err = cluster.CreateDefaultDataNode()
			if err != nil {
				return nil, err
			}
			dataNodes = append(dataNodes, dataNode)
		}
		cluster.DataNodes = dataNodes
	}

	if cluster.QueryNodes == nil {
		queryNodes := make([]types.QueryNodeComponent, 0)
		for i := 0; i < cluster.clusterConfig.QueryNodeNum; i++ {
			var queryNode types.QueryNodeComponent
			queryNode, err = cluster.CreateDefaultQueryNode()
			if err != nil {
				return nil, err
			}
			queryNodes = append(queryNodes, queryNode)
		}
		cluster.QueryNodes = queryNodes
	}

	if cluster.IndexNodes == nil {
		indexNodes := make([]types.IndexNodeComponent, 0)
		for i := 0; i < cluster.clusterConfig.IndexNodeNum; i++ {
			var indexNode types.IndexNodeComponent
			indexNode, err = cluster.CreateDefaultIndexNode()
			if err != nil {
				return
			}
			indexNodes = append(indexNodes, indexNode)
		}
		cluster.IndexNodes = indexNodes
	}

	if cluster.Proxy == nil {
		var proxy types.ProxyComponent
		proxy, err = cluster.CreateDefaultProxy()
		if err != nil {
			return
		}
		cluster.Proxy = proxy
	}

	//cluster.dataCoord.SetIndexCoord(cluster.indexCoord)
	cluster.DataCoord.SetRootCoord(cluster.RootCoord)

	err = cluster.RootCoord.SetDataCoord(cluster.DataCoord)
	if err != nil {
		return
	}
	//err = cluster.rootCoord.SetIndexCoord(cluster.indexCoord)
	//if err != nil {
	//	return
	//}
	err = cluster.RootCoord.SetQueryCoord(cluster.QueryCoord)
	if err != nil {
		return
	}

	//err = cluster.queryCoord.SetIndexCoord(cluster.indexCoord)
	if err != nil {
		return
	}
	err = cluster.QueryCoord.SetDataCoord(cluster.DataCoord)
	if err != nil {
		return
	}
	err = cluster.QueryCoord.SetRootCoord(cluster.RootCoord)
	if err != nil {
		return
	}

	//err = cluster.indexCoord.SetDataCoord(cluster.dataCoord)
	//if err != nil {
	//	return
	//}
	//err = cluster.indexCoord.SetRootCoord(cluster.rootCoord)
	//if err != nil {
	//	return
	//}

	for _, dataNode := range cluster.DataNodes {
		err = dataNode.SetDataCoord(cluster.DataCoord)
		if err != nil {
			return
		}
		err = dataNode.SetRootCoord(cluster.RootCoord)
		if err != nil {
			return
		}
	}

	cluster.Proxy.SetDataCoordClient(cluster.DataCoord)
	//cluster.proxy.SetIndexCoordClient(cluster.indexCoord)
	cluster.Proxy.SetQueryCoordClient(cluster.QueryCoord)
	cluster.Proxy.SetRootCoordClient(cluster.RootCoord)

	return cluster, nil
}

func (cluster *MiniCluster) GetContext() context.Context {
	return cluster.ctx
}

func (cluster *MiniCluster) Start() error {
	log.Info("mini cluster start")
	err := cluster.RootCoord.Init()
	if err != nil {
		return err
	}
	err = cluster.RootCoord.Start()
	if err != nil {
		return err
	}
	err = cluster.RootCoord.Register()
	if err != nil {
		return err
	}

	err = cluster.DataCoord.Init()
	if err != nil {
		return err
	}
	err = cluster.DataCoord.Start()
	if err != nil {
		return err
	}
	err = cluster.DataCoord.Register()
	if err != nil {
		return err
	}

	err = cluster.QueryCoord.Init()
	if err != nil {
		return err
	}
	err = cluster.QueryCoord.Start()
	if err != nil {
		return err
	}
	err = cluster.QueryCoord.Register()
	if err != nil {
		return err
	}

	//err = cluster.indexCoord.Init()
	//if err != nil {
	//	return err
	//}
	//err = cluster.indexCoord.Start()
	//if err != nil {
	//	return err
	//}
	//err = cluster.indexCoord.Register()
	//if err != nil {
	//	return err
	//}

	for _, dataNode := range cluster.DataNodes {
		err = dataNode.Init()
		if err != nil {
			return err
		}
		err = dataNode.Start()
		if err != nil {
			return err
		}
		err = dataNode.Register()
		if err != nil {
			return err
		}
	}

	for _, queryNode := range cluster.QueryNodes {
		err = queryNode.Init()
		if err != nil {
			return err
		}
		err = queryNode.Start()
		if err != nil {
			return err
		}
		err = queryNode.Register()
		if err != nil {
			return err
		}
	}

	for _, indexNode := range cluster.IndexNodes {
		err = indexNode.Init()
		if err != nil {
			return err
		}
		err = indexNode.Start()
		if err != nil {
			return err
		}
		err = indexNode.Register()
		if err != nil {
			return err
		}
	}

	err = cluster.Proxy.Init()
	if err != nil {
		return err
	}
	err = cluster.Proxy.Start()
	if err != nil {
		return err
	}
	err = cluster.Proxy.Register()
	if err != nil {
		return err
	}

	return nil
}

func (cluster *MiniCluster) Stop() error {
	log.Info("mini cluster stop")
	cluster.RootCoord.Stop()
	log.Info("mini cluster rootCoord stopped")
	cluster.DataCoord.Stop()
	log.Info("mini cluster dataCoord stopped")
	//cluster.indexCoord.Stop()
	cluster.QueryCoord.Stop()
	log.Info("mini cluster queryCoord stopped")
	cluster.Proxy.Stop()
	log.Info("mini cluster proxy stopped")

	for _, dataNode := range cluster.DataNodes {
		dataNode.Stop()
	}
	log.Info("mini cluster datanodes stopped")

	for _, queryNode := range cluster.QueryNodes {
		queryNode.Stop()
	}
	log.Info("mini cluster querynodes stopped")

	for _, indexNode := range cluster.IndexNodes {
		indexNode.Stop()
	}
	log.Info("mini cluster indexnodes stopped")

	cluster.EtcdCli.KV.Delete(cluster.ctx, params.EtcdCfg.RootPath.GetValue(), clientv3.WithPrefix())
	defer cluster.EtcdCli.Close()

	if cluster.ChunkManager == nil {
		chunkManager, err := cluster.factory.NewPersistentStorageChunkManager(cluster.ctx)
		if err != nil {
			log.Warn("fail to create chunk manager to clean test data", zap.Error(err))
		} else {
			cluster.ChunkManager = chunkManager
		}
	}
	cluster.ChunkManager.RemoveWithPrefix(cluster.ctx, cluster.ChunkManager.RootPath())
	return nil
}

func DefaultParams() map[string]string {
	testPath := fmt.Sprintf("integration-test-%d", time.Now().Unix())
	return map[string]string{
		EtcdRootPath:  testPath,
		MinioRootPath: testPath,
		//"runtime.role": typeutil.StandaloneRole,
		params.IntegrationTestCfg.IntegrationMode.Key: "true",
		params.LocalStorageCfg.Path.Key:               path.Join("/tmp", testPath),
		params.CommonCfg.StorageType.Key:              "local",
		params.DataNodeCfg.MemoryForceSyncEnable.Key:  "false", // local execution will print too many logs
	}
}

func WithParam(k, v string) Option {
	return func(cluster *MiniCluster) {
		cluster.params[k] = v
	}
}

func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		QueryNodeNum: 1,
		DataNodeNum:  1,
		IndexNodeNum: 1,
	}
}

func WithClusterSize(clusterConfig ClusterConfig) Option {
	return func(cluster *MiniCluster) {
		cluster.clusterConfig = clusterConfig
	}
}

func WithEtcdClient(etcdCli *clientv3.Client) Option {
	return func(cluster *MiniCluster) {
		cluster.EtcdCli = etcdCli
	}
}

func WithFactory(factory dependency.Factory) Option {
	return func(cluster *MiniCluster) {
		cluster.factory = factory
	}
}

func WithRootCoord(rootCoord types.RootCoordComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.RootCoord = rootCoord
	}
}

func WithDataCoord(dataCoord types.DataCoordComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.DataCoord = dataCoord
	}
}

func WithQueryCoord(queryCoord types.QueryCoordComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.QueryCoord = queryCoord
	}
}

//func WithIndexCoord(indexCoord types.IndexCoordComponent) Option {
//	return func(cluster *MiniCluster) {
//		cluster.indexCoord = indexCoord
//	}
//}

func WithDataNodes(datanodes []types.DataNodeComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.DataNodes = datanodes
	}
}

func WithQueryNodes(queryNodes []types.QueryNodeComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.QueryNodes = queryNodes
	}
}

func WithIndexNodes(indexNodes []types.IndexNodeComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.IndexNodes = indexNodes
	}
}

func WithProxy(proxy types.ProxyComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.Proxy = proxy
	}
}

func (cluster *MiniCluster) CreateDefaultRootCoord() (types.RootCoordComponent, error) {
	rootCoord, err := rootcoord.NewCore(cluster.ctx, cluster.factory)
	if err != nil {
		return nil, err
	}
	port := funcutil.GetAvailablePort()
	rootCoord.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	rootCoord.SetProxyCreator(cluster.GetProxy)
	rootCoord.SetEtcdClient(cluster.EtcdCli)
	return rootCoord, nil
}

func (cluster *MiniCluster) CreateDefaultDataCoord() (types.DataCoordComponent, error) {
	dataCoord := datacoord.CreateServer(cluster.ctx, cluster.factory)
	port := funcutil.GetAvailablePort()
	dataCoord.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	dataCoord.SetDataNodeCreator(cluster.GetDataNode)
	dataCoord.SetIndexNodeCreator(cluster.GetIndexNode)
	dataCoord.SetEtcdClient(cluster.EtcdCli)
	return dataCoord, nil
}

func (cluster *MiniCluster) CreateDefaultQueryCoord() (types.QueryCoordComponent, error) {
	queryCoord, err := querycoord.NewQueryCoord(cluster.ctx)
	if err != nil {
		return nil, err
	}
	port := funcutil.GetAvailablePort()
	queryCoord.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	queryCoord.SetQueryNodeCreator(cluster.GetQueryNode)
	queryCoord.SetEtcdClient(cluster.EtcdCli)
	return queryCoord, nil
}

//func (cluster *MiniCluster) CreateDefaultIndexCoord() (types.IndexCoordComponent, error) {
//	indexCoord, err := indexcoord.NewIndexCoord(cluster.ctx, cluster.factory)
//	if err != nil {
//		return nil, err
//	}
//	port := funcutil.GetAvailablePort()
//	indexCoord.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
//	indexCoord.SetIndexNodeCreator(cluster.GetIndexNode)
//	indexCoord.SetEtcdClient(cluster.etcdCli)
//	return indexCoord, nil
//}

func (cluster *MiniCluster) CreateDefaultDataNode() (types.DataNodeComponent, error) {
	log.Debug("mini cluster CreateDefaultDataNode")
	dataNode := datanode.NewDataNode(cluster.ctx, cluster.factory)
	dataNode.SetEtcdClient(cluster.EtcdCli)
	port := funcutil.GetAvailablePort()
	dataNode.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	return dataNode, nil
}

func (cluster *MiniCluster) CreateDefaultQueryNode() (types.QueryNodeComponent, error) {
	log.Debug("mini cluster CreateDefaultQueryNode")
	queryNode := querynodev2.NewQueryNode(cluster.ctx, cluster.factory)
	queryNode.SetEtcdClient(cluster.EtcdCli)
	port := funcutil.GetAvailablePort()
	queryNode.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	return queryNode, nil
}

func (cluster *MiniCluster) CreateDefaultIndexNode() (types.IndexNodeComponent, error) {
	log.Debug("mini cluster CreateDefaultIndexNode")
	indexNode := indexnode.NewIndexNode(cluster.ctx, cluster.factory)
	indexNode.SetEtcdClient(cluster.EtcdCli)
	port := funcutil.GetAvailablePort()
	indexNode.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	return indexNode, nil
}

func (cluster *MiniCluster) CreateDefaultProxy() (types.ProxyComponent, error) {
	log.Debug("mini cluster CreateDefaultProxy")
	proxy, err := proxy2.NewProxy(cluster.ctx, cluster.factory)
	proxy.SetEtcdClient(cluster.EtcdCli)
	if err != nil {
		return nil, err
	}
	port := funcutil.GetAvailablePort()
	proxy.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	proxy.SetQueryNodeCreator(cluster.GetQueryNode)
	return proxy, nil
}

// AddRootCoord to the cluster, you can use your own node for some specific purpose or
// pass nil to create a default one with cluster's setting.
func (cluster *MiniCluster) AddRootCoord(rootCoord types.RootCoordComponent) error {
	log.Debug("mini cluster AddRootCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	var err error
	if cluster.RootCoord != nil {
		return errors.New("rootCoord already exist, maybe you need to remove it first")
	}
	if rootCoord == nil {
		rootCoord, err = cluster.CreateDefaultRootCoord()
		if err != nil {
			return err
		}
	}

	// link
	rootCoord.SetDataCoord(cluster.DataCoord)
	rootCoord.SetQueryCoord(cluster.QueryCoord)
	//rootCoord.SetIndexCoord(cluster.indexCoord)
	cluster.DataCoord.SetRootCoord(rootCoord)
	cluster.QueryCoord.SetRootCoord(rootCoord)
	//cluster.indexCoord.SetRootCoord(rootCoord)
	cluster.Proxy.SetRootCoordClient(rootCoord)
	for _, dataNode := range cluster.DataNodes {
		err = dataNode.SetRootCoord(rootCoord)
		if err != nil {
			return err
		}
	}

	// start
	err = rootCoord.Init()
	if err != nil {
		return err
	}
	err = rootCoord.Start()
	if err != nil {
		return err
	}
	err = rootCoord.Register()
	if err != nil {
		return err
	}

	cluster.RootCoord = rootCoord
	log.Debug("mini cluster AddRootCoord succeed")
	return nil
}

// RemoveRootCoord from the cluster
func (cluster *MiniCluster) RemoveRootCoord(rootCoord types.RootCoordComponent) error {
	log.Debug("mini cluster RemoveRootCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if cluster.RootCoord == nil {
		log.Info("mini cluster has no rootCoord, no need to remove")
		return nil
	}

	cluster.RootCoord.Stop()
	cluster.RootCoord = nil
	log.Debug("mini cluster RemoveRootCoord succeed")
	return nil
}

// AddDataCoord to the cluster, you can use your own node for some specific purpose or
// pass nil to create a default one with cluster's setting.
func (cluster *MiniCluster) AddDataCoord(dataCoord types.DataCoordComponent) error {
	log.Debug("mini cluster AddDataCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	var err error
	if cluster.DataCoord != nil {
		return errors.New("dataCoord already exist, maybe you need to remove it first")
	}
	if dataCoord == nil {
		dataCoord, err = cluster.CreateDefaultDataCoord()
		if err != nil {
			return err
		}
	}

	// link
	//dataCoord.SetIndexCoord(cluster.indexCoord)
	dataCoord.SetRootCoord(cluster.RootCoord)
	err = cluster.RootCoord.SetDataCoord(cluster.DataCoord)
	if err != nil {
		return err
	}
	err = cluster.QueryCoord.SetDataCoord(cluster.DataCoord)
	if err != nil {
		return err
	}
	//err = cluster.indexCoord.SetDataCoord(cluster.dataCoord)
	//if err != nil {
	//	return err
	//}
	cluster.Proxy.SetDataCoordClient(dataCoord)
	for _, dataNode := range cluster.DataNodes {
		err = dataNode.SetDataCoord(dataCoord)
		if err != nil {
			return err
		}
	}

	// start
	err = dataCoord.Init()
	if err != nil {
		return err
	}
	err = dataCoord.Start()
	if err != nil {
		return err
	}
	err = dataCoord.Register()
	if err != nil {
		return err
	}

	cluster.DataCoord = dataCoord
	log.Debug("mini cluster AddDataCoord succeed")
	return nil
}

// RemoveDataCoord from the cluster
func (cluster *MiniCluster) RemoveDataCoord(dataCoord types.DataCoordComponent) error {
	log.Debug("mini cluster RemoveDataCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if cluster.DataCoord == nil {
		log.Info("mini cluster has no dataCoord, no need to remove")
		return nil
	}

	cluster.DataCoord.Stop()
	cluster.DataCoord = nil
	log.Debug("mini cluster RemoveDataCoord succeed")
	return nil
}

// AddQueryCoord to the cluster, you can use your own node for some specific purpose or
// pass nil to create a default one with cluster's setting.
func (cluster *MiniCluster) AddQueryCoord(queryCoord types.QueryCoordComponent) error {
	log.Debug("mini cluster AddQueryCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	var err error
	if cluster.QueryCoord != nil {
		return errors.New("queryCoord already exist, maybe you need to remove it first")
	}
	if queryCoord == nil {
		queryCoord, err = cluster.CreateDefaultQueryCoord()
		if err != nil {
			return err
		}
	}

	// link
	queryCoord.SetRootCoord(cluster.RootCoord)
	queryCoord.SetDataCoord(cluster.DataCoord)
	//queryCoord.SetIndexCoord(cluster.indexCoord)
	cluster.RootCoord.SetQueryCoord(queryCoord)
	cluster.Proxy.SetQueryCoordClient(queryCoord)

	// start
	err = queryCoord.Init()
	if err != nil {
		return err
	}
	err = queryCoord.Start()
	if err != nil {
		return err
	}
	err = queryCoord.Register()
	if err != nil {
		return err
	}

	cluster.QueryCoord = queryCoord
	log.Debug("mini cluster AddQueryCoord succeed")
	return nil
}

// RemoveQueryCoord from the cluster
func (cluster *MiniCluster) RemoveQueryCoord(queryCoord types.QueryCoordComponent) error {
	log.Debug("mini cluster RemoveQueryCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if cluster.QueryCoord == nil {
		log.Info("mini cluster has no queryCoord, no need to remove")
		return nil
	}

	cluster.QueryCoord.Stop()
	cluster.QueryCoord = nil
	log.Debug("mini cluster RemoveQueryCoord succeed")
	return nil
}

// AddIndexCoord to the cluster, you can use your own node for some specific purpose or
// pass nil to create a default one with cluster's setting.
//func (cluster *MiniCluster) AddIndexCoord(indexCoord types.IndexCoordComponent) error {
//	log.Debug("mini cluster AddIndexCoord start")
//	cluster.mu.Lock()
//	defer cluster.mu.Unlock()
//	var err error
//	if cluster.indexCoord != nil {
//		return errors.New("indexCoord already exist, maybe you need to remove it first")
//	}
//	if indexCoord == nil {
//		indexCoord, err = cluster.CreateDefaultIndexCoord()
//		if err != nil {
//			return err
//		}
//	}
//
//	// link
//	indexCoord.SetDataCoord(cluster.dataCoord)
//	indexCoord.SetRootCoord(cluster.rootCoord)
//	//cluster.dataCoord.SetIndexCoord(indexCoord)
//	cluster.queryCoord.SetIndexCoord(indexCoord)
//	//cluster.rootCoord.SetIndexCoord(indexCoord)
//	//cluster.proxy.SetIndexCoordClient(indexCoord)
//
//	// start
//	err = indexCoord.Init()
//	if err != nil {
//		return err
//	}
//	err = indexCoord.Start()
//	if err != nil {
//		return err
//	}
//	err = indexCoord.Register()
//	if err != nil {
//		return err
//	}
//
//	cluster.indexCoord = indexCoord
//	log.Debug("mini cluster AddIndexCoord succeed")
//	return nil
//}

// RemoveIndexCoord from the cluster
//func (cluster *MiniCluster) RemoveIndexCoord(indexCoord types.IndexCoordComponent) error {
//	log.Debug("mini cluster RemoveIndexCoord start")
//	cluster.mu.Lock()
//	defer cluster.mu.Unlock()
//
//	if cluster.indexCoord == nil {
//		log.Info("mini cluster has no indexCoord, no need to remove")
//		return nil
//	}
//
//	cluster.indexCoord.Stop()
//	cluster.indexCoord = nil
//	log.Debug("mini cluster RemoveIndexCoord succeed")
//	return nil
//}

// AddDataNode to the cluster, you can use your own node for some specific purpose or
// pass nil to create a default one with cluster's setting.
func (cluster *MiniCluster) AddDataNode(dataNode types.DataNodeComponent) error {
	log.Debug("mini cluster AddDataNode start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	var err error
	if dataNode == nil {
		dataNode, err = cluster.CreateDefaultDataNode()
		if err != nil {
			return err
		}
	}
	err = dataNode.SetDataCoord(cluster.DataCoord)
	if err != nil {
		return err
	}
	err = dataNode.SetRootCoord(cluster.RootCoord)
	if err != nil {
		return err
	}
	err = dataNode.Init()
	if err != nil {
		return err
	}
	err = dataNode.Start()
	if err != nil {
		return err
	}
	err = dataNode.Register()
	if err != nil {
		return err
	}
	cluster.DataNodes = append(cluster.DataNodes, dataNode)
	cluster.clusterConfig.DataNodeNum = cluster.clusterConfig.DataNodeNum + 1
	log.Debug("mini cluster AddDataNode succeed")
	return nil
}

// RemoveDataNode from the cluster, if pass nil, remove a node randomly
func (cluster *MiniCluster) RemoveDataNode(dataNode types.DataNodeComponent) error {
	log.Debug("mini cluster RemoveDataNode start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if dataNode == nil {
		// choose a node randomly
		if len(cluster.DataNodes) > 0 {
			randIndex := rand.Intn(len(cluster.DataNodes))
			dataNode = cluster.DataNodes[randIndex]
		} else {
			log.Debug("mini cluster has no dataNodes")
			return nil
		}
	}

	err := dataNode.Stop()
	if err != nil {
		return err
	}

	newDataNodes := make([]types.DataNodeComponent, 0)
	for _, dn := range cluster.DataNodes {
		if dn == dataNode {
			continue
		}
		newDataNodes = append(newDataNodes, dn)
	}
	cluster.DataNodes = newDataNodes
	cluster.clusterConfig.DataNodeNum = cluster.clusterConfig.DataNodeNum - 1
	log.Debug("mini cluster RemoveDataNode succeed")
	return nil
}

// AddQueryNode to the cluster, you can use your own node for some specific purpose or
// pass nil to create a default one with cluster's setting.
func (cluster *MiniCluster) AddQueryNode(queryNode types.QueryNodeComponent) error {
	log.Debug("mini cluster AddQueryNode start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	var err error
	if queryNode == nil {
		queryNode, err = cluster.CreateDefaultQueryNode()
		if err != nil {
			return err
		}
	}
	err = queryNode.Init()
	if err != nil {
		return err
	}
	err = queryNode.Start()
	if err != nil {
		return err
	}
	err = queryNode.Register()
	if err != nil {
		return err
	}
	cluster.QueryNodes = append(cluster.QueryNodes, queryNode)
	cluster.clusterConfig.QueryNodeNum = cluster.clusterConfig.QueryNodeNum + 1
	log.Debug("mini cluster AddQueryNode succeed")
	return nil
}

// RemoveQueryNode from the cluster, if pass nil, remove a node randomly
func (cluster *MiniCluster) RemoveQueryNode(queryNode types.QueryNodeComponent) error {
	log.Debug("mini cluster RemoveQueryNode start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if queryNode == nil {
		// choose a node randomly
		if len(cluster.QueryNodes) > 0 {
			randIndex := rand.Intn(len(cluster.QueryNodes))
			queryNode = cluster.QueryNodes[randIndex]
		} else {
			log.Debug("mini cluster has no queryNodes")
			return nil
		}
	}

	err := queryNode.Stop()
	if err != nil {
		return err
	}

	newQueryNodes := make([]types.QueryNodeComponent, 0)
	for _, qn := range cluster.QueryNodes {
		if qn == queryNode {
			continue
		}
		newQueryNodes = append(newQueryNodes, qn)
	}
	cluster.QueryNodes = newQueryNodes
	cluster.clusterConfig.QueryNodeNum = cluster.clusterConfig.QueryNodeNum - 1
	log.Debug("mini cluster RemoveQueryNode succeed")
	return nil
}

// AddIndexNode to the cluster, you can use your own node for some specific purpose or
// pass nil to create a default one with cluster's setting.
func (cluster *MiniCluster) AddIndexNode(indexNode types.IndexNodeComponent) error {
	log.Debug("mini cluster AddIndexNode start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	var err error
	if indexNode == nil {
		indexNode, err = cluster.CreateDefaultIndexNode()
		if err != nil {
			return err
		}
	}
	err = indexNode.Init()
	if err != nil {
		return err
	}
	err = indexNode.Start()
	if err != nil {
		return err
	}
	err = indexNode.Register()
	if err != nil {
		return err
	}
	cluster.IndexNodes = append(cluster.IndexNodes, indexNode)
	cluster.clusterConfig.IndexNodeNum = cluster.clusterConfig.IndexNodeNum + 1
	log.Debug("mini cluster AddIndexNode succeed")
	return nil
}

// RemoveIndexNode from the cluster, if pass nil, remove a node randomly
func (cluster *MiniCluster) RemoveIndexNode(indexNode types.IndexNodeComponent) error {
	log.Debug("mini cluster RemoveIndexNode start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if indexNode == nil {
		// choose a node randomly
		if len(cluster.IndexNodes) > 0 {
			randIndex := rand.Intn(len(cluster.IndexNodes))
			indexNode = cluster.IndexNodes[randIndex]
		} else {
			log.Debug("mini cluster has no queryNodes")
			return nil
		}
	}

	err := indexNode.Stop()
	if err != nil {
		return err
	}

	newIndexNodes := make([]types.IndexNodeComponent, 0)
	for _, in := range cluster.IndexNodes {
		if in == indexNode {
			continue
		}
		newIndexNodes = append(newIndexNodes, in)
	}
	cluster.IndexNodes = newIndexNodes
	cluster.clusterConfig.IndexNodeNum = cluster.clusterConfig.IndexNodeNum - 1
	log.Debug("mini cluster RemoveIndexNode succeed")
	return nil
}

func (cluster *MiniCluster) UpdateClusterSize(clusterConfig ClusterConfig) error {
	log.Debug("mini cluster UpdateClusterSize start")
	if clusterConfig.DataNodeNum < 0 || clusterConfig.QueryNodeNum < 0 || clusterConfig.IndexNodeNum < 0 {
		return errors.New("Illegal cluster size config")
	}
	// todo concurrent concerns
	//cluster.mu.Lock()
	//defer cluster.mu.Unlock()
	if clusterConfig.DataNodeNum > len(cluster.DataNodes) {
		needAdd := clusterConfig.DataNodeNum - len(cluster.DataNodes)
		for i := 0; i < needAdd; i++ {
			cluster.AddDataNode(nil)
		}
	} else if clusterConfig.DataNodeNum < len(cluster.DataNodes) {
		needRemove := len(cluster.DataNodes) - clusterConfig.DataNodeNum
		for i := 0; i < needRemove; i++ {
			cluster.RemoveDataNode(nil)
		}
	}

	if clusterConfig.QueryNodeNum > len(cluster.QueryNodes) {
		needAdd := clusterConfig.QueryNodeNum - len(cluster.QueryNodes)
		for i := 0; i < needAdd; i++ {
			cluster.AddQueryNode(nil)
		}
	} else if clusterConfig.QueryNodeNum < len(cluster.QueryNodes) {
		needRemove := len(cluster.QueryNodes) - clusterConfig.QueryNodeNum
		for i := 0; i < needRemove; i++ {
			cluster.RemoveQueryNode(nil)
		}
	}

	if clusterConfig.IndexNodeNum > len(cluster.IndexNodes) {
		needAdd := clusterConfig.IndexNodeNum - len(cluster.IndexNodes)
		for i := 0; i < needAdd; i++ {
			cluster.AddIndexNode(nil)
		}
	} else if clusterConfig.IndexNodeNum < len(cluster.IndexNodes) {
		needRemove := len(cluster.IndexNodes) - clusterConfig.IndexNodeNum
		for i := 0; i < needRemove; i++ {
			cluster.RemoveIndexNode(nil)
		}
	}

	// validate
	if clusterConfig.DataNodeNum != len(cluster.DataNodes) ||
		clusterConfig.QueryNodeNum != len(cluster.QueryNodes) ||
		clusterConfig.IndexNodeNum != len(cluster.IndexNodes) {
		return errors.New("Fail to update cluster size to target size")
	}

	log.Debug("mini cluster UpdateClusterSize succeed")
	return nil
}

func (cluster *MiniCluster) GetProxy(ctx context.Context, addr string) (types.Proxy, error) {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	if cluster.Proxy.GetAddress() == addr {
		return cluster.Proxy, nil
	}
	return nil, nil
}

func (cluster *MiniCluster) GetQueryNode(ctx context.Context, addr string) (types.QueryNode, error) {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	for _, queryNode := range cluster.QueryNodes {
		if queryNode.GetAddress() == addr {
			return queryNode, nil
		}
	}
	return nil, errors.New("no related queryNode found")
}

func (cluster *MiniCluster) GetDataNode(ctx context.Context, addr string) (types.DataNode, error) {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	for _, dataNode := range cluster.DataNodes {
		if dataNode.GetAddress() == addr {
			return dataNode, nil
		}
	}
	return nil, errors.New("no related dataNode found")
}

func (cluster *MiniCluster) GetIndexNode(ctx context.Context, addr string) (types.IndexNode, error) {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	for _, indexNode := range cluster.IndexNodes {
		if indexNode.GetAddress() == addr {
			return indexNode, nil
		}
	}
	return nil, errors.New("no related indexNode found")
}

func (cluster *MiniCluster) GetMetaWatcher() MetaWatcher {
	return cluster.MetaWatcher
}
