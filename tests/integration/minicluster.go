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
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/log"
	proxy2 "github.com/milvus-io/milvus/internal/proxy"
	querycoord "github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/querynode"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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

	mu sync.Mutex

	params        map[string]string
	clusterConfig ClusterConfig

	factory      dependency.Factory
	chunkManager storage.ChunkManager

	etcdCli *clientv3.Client

	proxy     types.ProxyComponent
	dataCoord types.DataCoordComponent
	rootCoord types.RootCoordComponent
	//indexCoord types.IndexCoordComponent

	queryCoord types.QueryCoordComponent
	queryNodes []types.QueryNodeComponent
	dataNodes  []types.DataNodeComponent
	indexNodes []types.IndexNodeComponent

	metaWatcher MetaWatcher
}

var Params *paramtable.ComponentParam = paramtable.Get()

type Option func(cluster *MiniCluster)

func StartMiniCluster(ctx context.Context, opts ...Option) (cluster *MiniCluster, err error) {
	cluster = &MiniCluster{
		ctx: ctx,
	}
	//Params.InitOnce()
	Params.Init()
	cluster.params = DefaultParams()
	cluster.clusterConfig = DefaultClusterConfig()
	for _, opt := range opts {
		opt(cluster)
	}
	for k, v := range cluster.params {
		Params.Save(k, v)
	}

	if cluster.factory == nil {
		cluster.factory = dependency.NewDefaultFactory(true)
		chunkManager, err := cluster.factory.NewPersistentStorageChunkManager(cluster.ctx)
		if err != nil {
			return nil, err
		}
		cluster.chunkManager = chunkManager
	}

	if cluster.etcdCli == nil {
		var etcdCli *clientv3.Client
		etcdCli, err = etcd.GetEtcdClient(
			Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
			Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
			Params.EtcdCfg.Endpoints.GetAsStrings(),
			Params.EtcdCfg.EtcdTLSCert.GetValue(),
			Params.EtcdCfg.EtcdTLSKey.GetValue(),
			Params.EtcdCfg.EtcdTLSCACert.GetValue(),
			Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
		if err != nil {
			return nil, err
		}
		cluster.etcdCli = etcdCli
	}

	cluster.metaWatcher = &EtcdMetaWatcher{
		rootPath: cluster.params[EtcdRootPath],
		etcdCli:  cluster.etcdCli,
	}

	if cluster.rootCoord == nil {
		var rootCoord types.RootCoordComponent
		rootCoord, err = cluster.CreateDefaultRootCoord()
		if err != nil {
			return nil, err
		}
		cluster.rootCoord = rootCoord
	}

	if cluster.dataCoord == nil {
		var dataCoord types.DataCoordComponent
		dataCoord, err = cluster.CreateDefaultDataCoord()
		if err != nil {
			return nil, err
		}
		cluster.dataCoord = dataCoord
	}

	if cluster.queryCoord == nil {
		var queryCoord types.QueryCoordComponent
		queryCoord, err = cluster.CreateDefaultQueryCoord()
		if err != nil {
			return nil, err
		}
		cluster.queryCoord = queryCoord
	}

	//if cluster.indexCoord == nil {
	//	var indexCoord types.IndexCoordComponent
	//	indexCoord, err = cluster.CreateDefaultIndexCoord()
	//	if err != nil {
	//		return nil, err
	//	}
	//	cluster.indexCoord = indexCoord
	//}

	if cluster.dataNodes == nil {
		dataNodes := make([]types.DataNodeComponent, 0)
		for i := 0; i < cluster.clusterConfig.DataNodeNum; i++ {
			var dataNode types.DataNodeComponent
			dataNode, err = cluster.CreateDefaultDataNode()
			if err != nil {
				return nil, err
			}
			dataNodes = append(dataNodes, dataNode)
		}
		cluster.dataNodes = dataNodes
	}

	if cluster.queryNodes == nil {
		queryNodes := make([]types.QueryNodeComponent, 0)
		for i := 0; i < cluster.clusterConfig.QueryNodeNum; i++ {
			var queryNode types.QueryNodeComponent
			queryNode, err = cluster.CreateDefaultQueryNode()
			if err != nil {
				return nil, err
			}
			queryNodes = append(queryNodes, queryNode)
		}
		cluster.queryNodes = queryNodes
	}

	if cluster.indexNodes == nil {
		indexNodes := make([]types.IndexNodeComponent, 0)
		for i := 0; i < cluster.clusterConfig.IndexNodeNum; i++ {
			var indexNode types.IndexNodeComponent
			indexNode, err = cluster.CreateDefaultIndexNode()
			if err != nil {
				return
			}
			indexNodes = append(indexNodes, indexNode)
		}
		cluster.indexNodes = indexNodes
	}

	if cluster.proxy == nil {
		var proxy types.ProxyComponent
		proxy, err = cluster.CreateDefaultProxy()
		if err != nil {
			return
		}
		cluster.proxy = proxy
	}

	//cluster.dataCoord.SetIndexCoord(cluster.indexCoord)
	cluster.dataCoord.SetRootCoord(cluster.rootCoord)

	err = cluster.rootCoord.SetDataCoord(cluster.dataCoord)
	if err != nil {
		return
	}
	//err = cluster.rootCoord.SetIndexCoord(cluster.indexCoord)
	//if err != nil {
	//	return
	//}
	err = cluster.rootCoord.SetQueryCoord(cluster.queryCoord)
	if err != nil {
		return
	}

	//err = cluster.queryCoord.SetIndexCoord(cluster.indexCoord)
	if err != nil {
		return
	}
	err = cluster.queryCoord.SetDataCoord(cluster.dataCoord)
	if err != nil {
		return
	}
	err = cluster.queryCoord.SetRootCoord(cluster.rootCoord)
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

	for _, dataNode := range cluster.dataNodes {
		err = dataNode.SetDataCoord(cluster.dataCoord)
		if err != nil {
			return
		}
		err = dataNode.SetRootCoord(cluster.rootCoord)
		if err != nil {
			return
		}
	}

	cluster.proxy.SetDataCoordClient(cluster.dataCoord)
	//cluster.proxy.SetIndexCoordClient(cluster.indexCoord)
	cluster.proxy.SetQueryCoordClient(cluster.queryCoord)
	cluster.proxy.SetRootCoordClient(cluster.rootCoord)

	return cluster, nil
}

func (cluster *MiniCluster) Start() error {
	log.Info("mini cluster start")
	err := cluster.rootCoord.Init()
	if err != nil {
		return err
	}
	err = cluster.rootCoord.Start()
	if err != nil {
		return err
	}
	err = cluster.rootCoord.Register()
	if err != nil {
		return err
	}

	err = cluster.dataCoord.Init()
	if err != nil {
		return err
	}
	err = cluster.dataCoord.Start()
	if err != nil {
		return err
	}
	err = cluster.dataCoord.Register()
	if err != nil {
		return err
	}

	err = cluster.queryCoord.Init()
	if err != nil {
		return err
	}
	err = cluster.queryCoord.Start()
	if err != nil {
		return err
	}
	err = cluster.queryCoord.Register()
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

	for _, dataNode := range cluster.dataNodes {
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

	for _, queryNode := range cluster.queryNodes {
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

	for _, indexNode := range cluster.indexNodes {
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

	err = cluster.proxy.Init()
	if err != nil {
		return err
	}
	err = cluster.proxy.Start()
	if err != nil {
		return err
	}
	err = cluster.proxy.Register()
	if err != nil {
		return err
	}

	return nil
}

func (cluster *MiniCluster) Stop() error {
	log.Info("mini cluster stop")
	cluster.proxy.Stop()
	log.Info("mini cluster proxy stopped")
	cluster.rootCoord.Stop()
	log.Info("mini cluster rootCoord stopped")
	cluster.dataCoord.Stop()
	log.Info("mini cluster dataCoord stopped")
	//cluster.indexCoord.Stop()
	cluster.queryCoord.Stop()
	log.Info("mini cluster queryCoord stopped")

	for _, dataNode := range cluster.dataNodes {
		dataNode.Stop()
	}
	log.Info("mini cluster datanodes stopped")
	for _, queryNode := range cluster.queryNodes {
		queryNode.Stop()
	}
	log.Info("mini cluster indexnodes stopped")
	for _, indexNode := range cluster.indexNodes {
		indexNode.Stop()
	}
	log.Info("mini cluster querynodes stopped")

	cluster.etcdCli.KV.Delete(cluster.ctx, Params.EtcdCfg.RootPath.GetValue(), clientv3.WithPrefix())
	defer cluster.etcdCli.Close()

	if cluster.chunkManager == nil {
		chunkManager, err := cluster.factory.NewPersistentStorageChunkManager(cluster.ctx)
		if err != nil {
			log.Warn("fail to create chunk manager to clean test data", zap.Error(err))
		} else {
			cluster.chunkManager = chunkManager
		}
	}
	cluster.chunkManager.RemoveWithPrefix(cluster.ctx, cluster.chunkManager.RootPath())
	return nil
}

func DefaultParams() map[string]string {
	testPath := fmt.Sprintf("integration-test-%d", time.Now().Unix())
	return map[string]string{
		EtcdRootPath:  testPath,
		MinioRootPath: testPath,
		//"runtime.role": typeutil.StandaloneRole,
		Params.IntegrationTestCfg.IntegrationMode.Key: "true",
		Params.CommonCfg.StorageType.Key:              "local",
		Params.DataNodeCfg.MemoryForceSyncEnable.Key:  "false", // local execution will print too many logs
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
		cluster.etcdCli = etcdCli
	}
}

func WithFactory(factory dependency.Factory) Option {
	return func(cluster *MiniCluster) {
		cluster.factory = factory
	}
}

func WithRootCoord(rootCoord types.RootCoordComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.rootCoord = rootCoord
	}
}

func WithDataCoord(dataCoord types.DataCoordComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.dataCoord = dataCoord
	}
}

func WithQueryCoord(queryCoord types.QueryCoordComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.queryCoord = queryCoord
	}
}

//func WithIndexCoord(indexCoord types.IndexCoordComponent) Option {
//	return func(cluster *MiniCluster) {
//		cluster.indexCoord = indexCoord
//	}
//}

func WithDataNodes(datanodes []types.DataNodeComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.dataNodes = datanodes
	}
}

func WithQueryNodes(queryNodes []types.QueryNodeComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.queryNodes = queryNodes
	}
}

func WithIndexNodes(indexNodes []types.IndexNodeComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.indexNodes = indexNodes
	}
}

func WithProxy(proxy types.ProxyComponent) Option {
	return func(cluster *MiniCluster) {
		cluster.proxy = proxy
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
	rootCoord.SetEtcdClient(cluster.etcdCli)
	return rootCoord, nil
}

func (cluster *MiniCluster) CreateDefaultDataCoord() (types.DataCoordComponent, error) {
	dataCoord := datacoord.CreateServer(cluster.ctx, cluster.factory)
	port := funcutil.GetAvailablePort()
	dataCoord.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	dataCoord.SetDataNodeCreator(cluster.GetDataNode)
	dataCoord.SetIndexNodeCreator(cluster.GetIndexNode)
	dataCoord.SetEtcdClient(cluster.etcdCli)
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
	queryCoord.SetEtcdClient(cluster.etcdCli)
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
	dataNode.SetEtcdClient(cluster.etcdCli)
	port := funcutil.GetAvailablePort()
	dataNode.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	return dataNode, nil
}

func (cluster *MiniCluster) CreateDefaultQueryNode() (types.QueryNodeComponent, error) {
	log.Debug("mini cluster CreateDefaultQueryNode")
	queryNode := querynode.NewQueryNode(cluster.ctx, cluster.factory)
	queryNode.SetEtcdClient(cluster.etcdCli)
	port := funcutil.GetAvailablePort()
	queryNode.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	return queryNode, nil
}

func (cluster *MiniCluster) CreateDefaultIndexNode() (types.IndexNodeComponent, error) {
	log.Debug("mini cluster CreateDefaultIndexNode")
	indexNode := indexnode.NewIndexNode(cluster.ctx, cluster.factory)
	indexNode.SetEtcdClient(cluster.etcdCli)
	port := funcutil.GetAvailablePort()
	indexNode.SetAddress(funcutil.GetLocalIP() + ":" + fmt.Sprint(port))
	return indexNode, nil
}

func (cluster *MiniCluster) CreateDefaultProxy() (types.ProxyComponent, error) {
	log.Debug("mini cluster CreateDefaultProxy")
	proxy, err := proxy2.NewProxy(cluster.ctx, cluster.factory)
	proxy.SetEtcdClient(cluster.etcdCli)
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
	if cluster.rootCoord != nil {
		return errors.New("rootCoord already exist, maybe you need to remove it first")
	}
	if rootCoord == nil {
		rootCoord, err = cluster.CreateDefaultRootCoord()
		if err != nil {
			return err
		}
	}

	// link
	rootCoord.SetDataCoord(cluster.dataCoord)
	rootCoord.SetQueryCoord(cluster.queryCoord)
	//rootCoord.SetIndexCoord(cluster.indexCoord)
	cluster.dataCoord.SetRootCoord(rootCoord)
	cluster.queryCoord.SetRootCoord(rootCoord)
	//cluster.indexCoord.SetRootCoord(rootCoord)
	cluster.proxy.SetRootCoordClient(rootCoord)
	for _, dataNode := range cluster.dataNodes {
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

	cluster.rootCoord = rootCoord
	log.Debug("mini cluster AddRootCoord succeed")
	return nil
}

// RemoveRootCoord from the cluster
func (cluster *MiniCluster) RemoveRootCoord(rootCoord types.RootCoordComponent) error {
	log.Debug("mini cluster RemoveRootCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if cluster.rootCoord == nil {
		log.Info("mini cluster has no rootCoord, no need to remove")
		return nil
	}

	cluster.rootCoord.Stop()
	cluster.rootCoord = nil
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
	if cluster.dataCoord != nil {
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
	dataCoord.SetRootCoord(cluster.rootCoord)
	err = cluster.rootCoord.SetDataCoord(cluster.dataCoord)
	if err != nil {
		return err
	}
	err = cluster.queryCoord.SetDataCoord(cluster.dataCoord)
	if err != nil {
		return err
	}
	//err = cluster.indexCoord.SetDataCoord(cluster.dataCoord)
	//if err != nil {
	//	return err
	//}
	cluster.proxy.SetDataCoordClient(dataCoord)
	for _, dataNode := range cluster.dataNodes {
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

	cluster.dataCoord = dataCoord
	log.Debug("mini cluster AddDataCoord succeed")
	return nil
}

// RemoveDataCoord from the cluster
func (cluster *MiniCluster) RemoveDataCoord(dataCoord types.DataCoordComponent) error {
	log.Debug("mini cluster RemoveDataCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if cluster.dataCoord == nil {
		log.Info("mini cluster has no dataCoord, no need to remove")
		return nil
	}

	cluster.dataCoord.Stop()
	cluster.dataCoord = nil
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
	if cluster.queryCoord != nil {
		return errors.New("queryCoord already exist, maybe you need to remove it first")
	}
	if queryCoord == nil {
		queryCoord, err = cluster.CreateDefaultQueryCoord()
		if err != nil {
			return err
		}
	}

	// link
	queryCoord.SetRootCoord(cluster.rootCoord)
	queryCoord.SetDataCoord(cluster.dataCoord)
	//queryCoord.SetIndexCoord(cluster.indexCoord)
	cluster.rootCoord.SetQueryCoord(queryCoord)
	cluster.proxy.SetQueryCoordClient(queryCoord)

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

	cluster.queryCoord = queryCoord
	log.Debug("mini cluster AddQueryCoord succeed")
	return nil
}

// RemoveQueryCoord from the cluster
func (cluster *MiniCluster) RemoveQueryCoord(queryCoord types.QueryCoordComponent) error {
	log.Debug("mini cluster RemoveQueryCoord start")
	cluster.mu.Lock()
	defer cluster.mu.Unlock()

	if cluster.queryCoord == nil {
		log.Info("mini cluster has no queryCoord, no need to remove")
		return nil
	}

	cluster.queryCoord.Stop()
	cluster.queryCoord = nil
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
	err = dataNode.SetDataCoord(cluster.dataCoord)
	if err != nil {
		return err
	}
	err = dataNode.SetRootCoord(cluster.rootCoord)
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
	cluster.dataNodes = append(cluster.dataNodes, dataNode)
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
		if len(cluster.dataNodes) > 0 {
			randIndex := rand.Intn(len(cluster.dataNodes))
			dataNode = cluster.dataNodes[randIndex]
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
	for _, dn := range cluster.dataNodes {
		if dn == dataNode {
			continue
		}
		newDataNodes = append(newDataNodes, dn)
	}
	cluster.dataNodes = newDataNodes
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
	cluster.queryNodes = append(cluster.queryNodes, queryNode)
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
		if len(cluster.queryNodes) > 0 {
			randIndex := rand.Intn(len(cluster.queryNodes))
			queryNode = cluster.queryNodes[randIndex]
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
	for _, qn := range cluster.queryNodes {
		if qn == queryNode {
			continue
		}
		newQueryNodes = append(newQueryNodes, qn)
	}
	cluster.queryNodes = newQueryNodes
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
	cluster.indexNodes = append(cluster.indexNodes, indexNode)
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
		if len(cluster.indexNodes) > 0 {
			randIndex := rand.Intn(len(cluster.indexNodes))
			indexNode = cluster.indexNodes[randIndex]
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
	for _, in := range cluster.indexNodes {
		if in == indexNode {
			continue
		}
		newIndexNodes = append(newIndexNodes, in)
	}
	cluster.indexNodes = newIndexNodes
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
	if clusterConfig.DataNodeNum > len(cluster.dataNodes) {
		needAdd := clusterConfig.DataNodeNum - len(cluster.dataNodes)
		for i := 0; i < needAdd; i++ {
			cluster.AddDataNode(nil)
		}
	} else if clusterConfig.DataNodeNum < len(cluster.dataNodes) {
		needRemove := len(cluster.dataNodes) - clusterConfig.DataNodeNum
		for i := 0; i < needRemove; i++ {
			cluster.RemoveDataNode(nil)
		}
	}

	if clusterConfig.QueryNodeNum > len(cluster.queryNodes) {
		needAdd := clusterConfig.QueryNodeNum - len(cluster.queryNodes)
		for i := 0; i < needAdd; i++ {
			cluster.AddQueryNode(nil)
		}
	} else if clusterConfig.QueryNodeNum < len(cluster.queryNodes) {
		needRemove := len(cluster.queryNodes) - clusterConfig.QueryNodeNum
		for i := 0; i < needRemove; i++ {
			cluster.RemoveQueryNode(nil)
		}
	}

	if clusterConfig.IndexNodeNum > len(cluster.indexNodes) {
		needAdd := clusterConfig.IndexNodeNum - len(cluster.indexNodes)
		for i := 0; i < needAdd; i++ {
			cluster.AddIndexNode(nil)
		}
	} else if clusterConfig.IndexNodeNum < len(cluster.indexNodes) {
		needRemove := len(cluster.indexNodes) - clusterConfig.IndexNodeNum
		for i := 0; i < needRemove; i++ {
			cluster.RemoveIndexNode(nil)
		}
	}

	// validate
	if clusterConfig.DataNodeNum != len(cluster.dataNodes) ||
		clusterConfig.QueryNodeNum != len(cluster.queryNodes) ||
		clusterConfig.IndexNodeNum != len(cluster.indexNodes) {
		return errors.New("Fail to update cluster size to target size")
	}

	log.Debug("mini cluster UpdateClusterSize succeed")
	return nil
}

func (cluster *MiniCluster) GetProxy(ctx context.Context, addr string) (types.Proxy, error) {
	if cluster.proxy.GetAddress() == addr {
		return cluster.proxy, nil
	}
	return nil, nil
}

func (cluster *MiniCluster) GetQueryNode(ctx context.Context, addr string) (types.QueryNode, error) {
	for _, queryNode := range cluster.queryNodes {
		if queryNode.GetAddress() == addr {
			return queryNode, nil
		}
	}
	return nil, errors.New("no related queryNode found")
}

func (cluster *MiniCluster) GetDataNode(ctx context.Context, addr string) (types.DataNode, error) {
	for _, dataNode := range cluster.dataNodes {
		if dataNode.GetAddress() == addr {
			return dataNode, nil
		}
	}
	return nil, errors.New("no related dataNode found")
}

func (cluster *MiniCluster) GetIndexNode(ctx context.Context, addr string) (types.IndexNode, error) {
	for _, indexNode := range cluster.indexNodes {
		if indexNode.GetAddress() == addr {
			return indexNode, nil
		}
	}
	return nil, errors.New("no related indexNode found")
}

func (cluster *MiniCluster) GetMetaWatcher() MetaWatcher {
	return cluster.metaWatcher
}
