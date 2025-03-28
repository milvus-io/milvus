package coordinator

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/txnkv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/querycoordv2"
	"github.com/milvus-io/milvus/internal/rootcoord"
	streamingcoord "github.com/milvus-io/milvus/internal/streamingcoord/server"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var Params *paramtable.ComponentParam = paramtable.Get()

type mixCoordImpl struct {
	rootcoordServer  *rootcoord.Core
	queryCoordServer *querycoordv2.Server
	datacoordServer  *datacoord.Server
	streamingCoord   *streamingcoord.Server

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	etcdCli *clientv3.Client
	tikvCli *txnkv.Client
	address string

	proxyCreator       proxyutil.ProxyCreator
	proxyWatcher       *proxyutil.ProxyWatcher
	proxyClientManager proxyutil.ProxyClientManagerInterface

	metricsCacheManager *metricsinfo.MetricsCacheManager
	stateCode           atomic.Int32
	initOnce            sync.Once
	startOnce           sync.Once
	session             *sessionutil.Session

	factory dependency.Factory

	enableActiveStandBy bool
	activateFunc        func() error

	metricsRequest *metricsinfo.MetricsRequest

	metaKVCreator  func() kv.MetaKv
	mixCoordClient types.MixCoordClient
}

func NewMixCoordServer(c context.Context, factory dependency.Factory) (types.MixCoordComponent, error) {
	ctx, cancel := context.WithCancel(c)
	rootCoordServer, _ := rootcoord.NewCore(ctx, factory)
	queryCoordServer, _ := querycoordv2.NewQueryCoord(c)
	dataCoordServer := datacoord.CreateServer(c, factory)

	return &mixCoordImpl{
		ctx:              ctx,
		cancel:           cancel,
		rootcoordServer:  rootCoordServer,
		queryCoordServer: queryCoordServer,
		datacoordServer:  dataCoordServer,
	}, nil
}

// Register register mixcoord at etcd
func (s *mixCoordImpl) Register() error {
	log := log.Ctx(s.ctx)
	s.session.Register()
	afterRegister := func() {
		metrics.NumNodes.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), typeutil.MixCoordRole).Inc()
		s.session.LivenessCheck(s.ctx, func() {
			log.Error("MixCoord disconnected from etcd, process will exit", zap.Int64("serverID", s.session.GetServerID()))
			os.Exit(1)
		})
	}
	if s.enableActiveStandBy {
		go func() {
			if err := s.session.ProcessActiveStandBy(s.activateFunc); err != nil {
				log.Error("failed to activate standby server", zap.Error(err))
				panic(err)
			}
			afterRegister()
		}()
	} else {
		afterRegister()
	}
	return nil
}

func (s *mixCoordImpl) Init() error {
	log := log.Ctx(s.ctx)
	var initErr error
	if err := s.initSession(); err != nil {
		initErr = err
		return initErr
	}
	s.initKVCreator()
	if s.enableActiveStandBy {
		s.activateFunc = func() error {
			log.Info("mixCoord switch from standby to active, activating")

			var err error
			s.initOnce.Do(func() {
				if err = s.initInternal(); err != nil {
					log.Error("mixCoord init failed", zap.Error(err))
				}
			})
			if err != nil {
				return err
			}
			log.Info("mixCoord startup success", zap.String("address", s.session.GetAddress()))
			return err
		}
		s.UpdateStateCode(commonpb.StateCode_StandBy)
		log.Info("MixCoord enter standby mode successfully")
	} else {
		var err error
		s.initOnce.Do(func() {
			if err = s.initInternal(); err != nil {
				log.Error("mixCoord init failed", zap.Error(err))
			}
		})
	}
	return initErr
}

func (s *mixCoordImpl) initInternal() error {
	log := log.Ctx(s.ctx)
	s.initStreamingCoord()
	s.rootcoordServer.SetMixCoord(s)
	s.datacoordServer.SetMixCoord(s)
	s.queryCoordServer.SetMixCoord(s)

	if err := s.rootcoordServer.Init(); err != nil {
		log.Error("rootCoord init failed", zap.Error(err))
		return err
	}

	if err := s.rootcoordServer.Start(); err != nil {
		log.Error("rootCoord start failed", zap.Error(err))
		return err
	}

	if err := s.streamingCoord.Start(s.ctx); err != nil {
		log.Error("streamCoord init failed", zap.Error(err))
		return err
	}

	if err := s.datacoordServer.Init(); err != nil {
		log.Error("dataCoord init failed", zap.Error(err))
		return err
	}

	if err := s.datacoordServer.Start(); err != nil {
		log.Error("dataCoord init failed", zap.Error(err))
		return err
	}

	if err := s.queryCoordServer.Init(); err != nil {
		log.Error("queryCoord init failed", zap.Error(err))
		return err
	}

	if err := s.queryCoordServer.Start(); err != nil {
		log.Error("queryCoord init failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *mixCoordImpl) initKVCreator() {
	if s.metaKVCreator == nil {
		if Params.MetaStoreCfg.MetaStoreType.GetValue() == util.MetaStoreTypeTiKV {
			s.metaKVCreator = func() kv.MetaKv {
				return tikv.NewTiKV(s.tikvCli, Params.TiKVCfg.MetaRootPath.GetValue(),
					tikv.WithRequestTimeout(paramtable.Get().ServiceParam.TiKVCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
			}
		} else {
			s.metaKVCreator = func() kv.MetaKv {
				return etcdkv.NewEtcdKV(s.etcdCli, Params.EtcdCfg.MetaRootPath.GetValue(),
					etcdkv.WithRequestTimeout(paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond)))
			}
		}
	}
}

func (s *mixCoordImpl) Start() error {
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	var startErr error
	return startErr
}

func (s *mixCoordImpl) Stop() error {
	log.Info("graceful stop")
	s.GracefulStop()
	log.Info("graceful stop done")

	if err := s.queryCoordServer.Stop(); err != nil {
		log.Error("Failed to stop queryCoord", zap.Error(err))
	}

	if err := s.datacoordServer.Stop(); err != nil {
		log.Error("Failed to stop dataCoord", zap.Error(err))
	}

	if err := s.rootcoordServer.Stop(); err != nil {
		log.Error("Failed to stop rootCoord", zap.Error(err))
	}
	s.cancel()
	return nil
}

func (s *mixCoordImpl) initStreamingCoord() {
	fMixcoord := syncutil.NewFuture[types.MixCoordClient]()
	fMixcoord.Set(s.mixCoordClient)

	s.streamingCoord = streamingcoord.NewServerBuilder().
		WithETCD(s.etcdCli).
		WithMetaKV(s.metaKVCreator()).
		WithSession(s.session).
		WithMixCoordClient(fMixcoord).
		Build()
}

func (s *mixCoordImpl) initSession() error {
	s.session = sessionutil.NewSession(s.ctx)
	s.session.Init(typeutil.MixCoordRole, s.address, true, true)
	s.enableActiveStandBy = Params.RootCoordCfg.EnableActiveStandby.GetAsBool()
	s.session.SetEnableActiveStandBy(s.enableActiveStandBy)
	s.rootcoordServer.SetSession(s.session)
	s.datacoordServer.SetSession(s.session)
	s.queryCoordServer.SetSession(s.session)

	return nil
}

func (s *mixCoordImpl) startHealthCheck() {
}

func (s *mixCoordImpl) SetAddress(address string) {
	s.address = address
	s.rootcoordServer.SetAddress(address)
	s.datacoordServer.SetAddress(address)
	s.queryCoordServer.SetAddress(address)
}

func (s *mixCoordImpl) SetEtcdClient(client *clientv3.Client) {
	s.etcdCli = client
	s.rootcoordServer.SetEtcdClient(client)
	s.datacoordServer.SetEtcdClient(client)
	s.queryCoordServer.SetEtcdClient(client)
}

func (s *mixCoordImpl) SetTiKVClient(client *txnkv.Client) {
	s.tikvCli = client
	s.rootcoordServer.SetTiKVClient(client)
	s.datacoordServer.SetTiKVClient(client)
	s.queryCoordServer.SetTiKVClient(client)
}

func (s *mixCoordImpl) SetMixCoordClient(client types.MixCoordClient) {
	s.mixCoordClient = client
}

func (s *mixCoordImpl) GetServerID() int64 {
	return paramtable.GetNodeID()
}

func (s *mixCoordImpl) UpdateStateCode(code commonpb.StateCode) {
	s.stateCode.Store(int32(code))
}

func (s *mixCoordImpl) GetStateCode() commonpb.StateCode {
	return commonpb.StateCode(s.stateCode.Load())
}

func (s *mixCoordImpl) GracefulStop() {
	if s.streamingCoord != nil {
		s.streamingCoord.Stop()
	}
}

// RootCoordServer
func (s *mixCoordImpl) CreateCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.CreateCollection(ctx, req)
}

func (s *mixCoordImpl) DropCollection(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.DropCollection(ctx, req)
}

func (s *mixCoordImpl) HasCollection(ctx context.Context, req *milvuspb.HasCollectionRequest) (*milvuspb.BoolResponse, error) {
	return s.rootcoordServer.HasCollection(ctx, req)
}

func (s *mixCoordImpl) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.rootcoordServer.DescribeCollection(ctx, req)
}

func (s *mixCoordImpl) ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
	return s.rootcoordServer.ShowCollections(ctx, req)
}

func (s *mixCoordImpl) ShowCollectionIDs(ctx context.Context, req *rootcoordpb.ShowCollectionIDsRequest) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	return s.rootcoordServer.ShowCollectionIDs(ctx, req)
}

func (s *mixCoordImpl) AlterCollection(ctx context.Context, req *milvuspb.AlterCollectionRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.AlterCollection(ctx, req)
}

func (s *mixCoordImpl) AlterCollectionField(ctx context.Context, req *milvuspb.AlterCollectionFieldRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.AlterCollectionField(ctx, req)
}

func (s *mixCoordImpl) CreatePartition(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.CreatePartition(ctx, req)
}

func (s *mixCoordImpl) DropPartition(ctx context.Context, req *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.DropPartition(ctx, req)
}

func (s *mixCoordImpl) HasPartition(ctx context.Context, req *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
	return s.rootcoordServer.HasPartition(ctx, req)
}

func (s *mixCoordImpl) ShowPartitions(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.rootcoordServer.ShowPartitions(ctx, req)
}

func (s *mixCoordImpl) ShowPartitionsInternal(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	return s.rootcoordServer.ShowPartitionsInternal(ctx, req)
}

func (s *mixCoordImpl) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	return s.rootcoordServer.AllocTimestamp(ctx, req)
}

func (s *mixCoordImpl) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	return s.rootcoordServer.AllocID(ctx, req)
}

func (s *mixCoordImpl) UpdateChannelTimeTick(ctx context.Context, req *internalpb.ChannelTimeTickMsg) (*commonpb.Status, error) {
	return s.rootcoordServer.UpdateChannelTimeTick(ctx, req)
}

func (s *mixCoordImpl) ShowSegments(ctx context.Context, req *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	return s.rootcoordServer.ShowSegments(ctx, req)
}

func (s *mixCoordImpl) GetPChannelInfo(ctx context.Context, req *rootcoordpb.GetPChannelInfoRequest) (*rootcoordpb.GetPChannelInfoResponse, error) {
	return s.rootcoordServer.GetPChannelInfo(ctx, req)
}

func (s *mixCoordImpl) InvalidateCollectionMetaCache(ctx context.Context, req *proxypb.InvalidateCollMetaCacheRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.InvalidateCollectionMetaCache(ctx, req)
}

func (s *mixCoordImpl) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	return s.rootcoordServer.ShowConfigurations(ctx, req)
}

func (s *mixCoordImpl) CreateAlias(ctx context.Context, in *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.CreateAlias(ctx, in)
}

func (s *mixCoordImpl) DescribeCollectionInternal(ctx context.Context, in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return s.rootcoordServer.DescribeCollectionInternal(ctx, in)
}

// DropAlias drop collection alias
func (c *mixCoordImpl) DropAlias(ctx context.Context, in *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
	return c.rootcoordServer.DropAlias(ctx, in)
}

// AlterAlias alter collection alias
func (c *mixCoordImpl) AlterAlias(ctx context.Context, in *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
	return c.rootcoordServer.AlterAlias(ctx, in)
}

// DescribeAlias describe collection alias
func (c *mixCoordImpl) DescribeAlias(ctx context.Context, in *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
	return c.rootcoordServer.DescribeAlias(ctx, in)
}

// ListAliases list aliases
func (c *mixCoordImpl) ListAliases(ctx context.Context, in *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
	return c.rootcoordServer.ListAliases(ctx, in)
}

func (c *mixCoordImpl) AddCollectionField(ctx context.Context, in *milvuspb.AddCollectionFieldRequest) (*commonpb.Status, error) {
	return c.rootcoordServer.AddCollectionField(ctx, in)
}

func (s *mixCoordImpl) CreateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return s.rootcoordServer.CreateCredential(ctx, req)
}

func (s *mixCoordImpl) GetCredential(ctx context.Context, req *rootcoordpb.GetCredentialRequest) (*rootcoordpb.GetCredentialResponse, error) {
	return s.rootcoordServer.GetCredential(ctx, req)
}

func (s *mixCoordImpl) UpdateCredential(ctx context.Context, req *internalpb.CredentialInfo) (*commonpb.Status, error) {
	return s.rootcoordServer.UpdateCredential(ctx, req)
}

func (s *mixCoordImpl) DeleteCredential(ctx context.Context, req *milvuspb.DeleteCredentialRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.DeleteCredential(ctx, req)
}

func (s *mixCoordImpl) ListCredUsers(ctx context.Context, req *milvuspb.ListCredUsersRequest) (*milvuspb.ListCredUsersResponse, error) {
	return s.rootcoordServer.ListCredUsers(ctx, req)
}

func (s *mixCoordImpl) CreateRole(ctx context.Context, req *milvuspb.CreateRoleRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.CreateRole(ctx, req)
}

func (s *mixCoordImpl) DropRole(ctx context.Context, req *milvuspb.DropRoleRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.DropRole(ctx, req)
}

func (s *mixCoordImpl) OperateUserRole(ctx context.Context, req *milvuspb.OperateUserRoleRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.OperateUserRole(ctx, req)
}

func (s *mixCoordImpl) SelectRole(ctx context.Context, req *milvuspb.SelectRoleRequest) (*milvuspb.SelectRoleResponse, error) {
	return s.rootcoordServer.SelectRole(ctx, req)
}

func (s *mixCoordImpl) SelectUser(ctx context.Context, req *milvuspb.SelectUserRequest) (*milvuspb.SelectUserResponse, error) {
	return s.rootcoordServer.SelectUser(ctx, req)
}

func (s *mixCoordImpl) OperatePrivilege(ctx context.Context, req *milvuspb.OperatePrivilegeRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.OperatePrivilege(ctx, req)
}

func (s *mixCoordImpl) SelectGrant(ctx context.Context, req *milvuspb.SelectGrantRequest) (*milvuspb.SelectGrantResponse, error) {
	return s.rootcoordServer.SelectGrant(ctx, req)
}

func (s *mixCoordImpl) ListPolicy(ctx context.Context, req *internalpb.ListPolicyRequest) (*internalpb.ListPolicyResponse, error) {
	return s.rootcoordServer.ListPolicy(ctx, req)
}

func (s *mixCoordImpl) CheckHealth(ctx context.Context, req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
	return s.rootcoordServer.CheckHealth(ctx, req)
}

func (s *mixCoordImpl) RenameCollection(ctx context.Context, req *milvuspb.RenameCollectionRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.RenameCollection(ctx, req)
}

func (s *mixCoordImpl) CreateDatabase(ctx context.Context, req *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.CreateDatabase(ctx, req)
}

func (s *mixCoordImpl) DropDatabase(ctx context.Context, req *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.DropDatabase(ctx, req)
}

func (s *mixCoordImpl) ListDatabases(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
	return s.rootcoordServer.ListDatabases(ctx, req)
}

func (s *mixCoordImpl) DescribeDatabase(ctx context.Context, req *rootcoordpb.DescribeDatabaseRequest) (*rootcoordpb.DescribeDatabaseResponse, error) {
	return s.rootcoordServer.DescribeDatabase(ctx, req)
}

func (s *mixCoordImpl) AlterDatabase(ctx context.Context, req *rootcoordpb.AlterDatabaseRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.AlterDatabase(ctx, req)
}

func (s *mixCoordImpl) BackupRBAC(ctx context.Context, req *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
	return s.rootcoordServer.BackupRBAC(ctx, req)
}

func (s *mixCoordImpl) RestoreRBAC(ctx context.Context, req *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.RestoreRBAC(ctx, req)
}

func (s *mixCoordImpl) CreatePrivilegeGroup(ctx context.Context, req *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.CreatePrivilegeGroup(ctx, req)
}

func (s *mixCoordImpl) DropPrivilegeGroup(ctx context.Context, req *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.DropPrivilegeGroup(ctx, req)
}

func (s *mixCoordImpl) ListPrivilegeGroups(ctx context.Context, req *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
	return s.rootcoordServer.ListPrivilegeGroups(ctx, req)
}

func (s *mixCoordImpl) OperatePrivilegeGroup(ctx context.Context, req *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
	return s.rootcoordServer.OperatePrivilegeGroup(ctx, req)
}

// GetComponentStates get states of components
func (s *mixCoordImpl) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	code := s.GetStateCode()
	log.Ctx(ctx).Debug("Mix coord current state", zap.String("StateCode", code.String()))

	nodeID := common.NotRegisteredID
	if s.session != nil && s.session.Registered() {
		nodeID = s.session.ServerID
	}

	return &milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			NodeID:    nodeID,
			Role:      typeutil.MixCoordRole,
			StateCode: code,
			ExtraInfo: nil,
		},
		Status: merr.Success(),
		SubcomponentStates: []*milvuspb.ComponentInfo{
			{
				NodeID:    nodeID,
				Role:      typeutil.MixCoordRole,
				StateCode: code,
				ExtraInfo: nil,
			},
		},
	}, nil
}

// GetTimeTickChannel get timetick channel name
func (sc *mixCoordImpl) GetTimeTickChannel(ctx context.Context, req *internalpb.GetTimeTickChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  Params.CommonCfg.RootCoordTimeTick.GetValue(),
	}, nil
}

// GetStatisticsChannel get statistics channel name
func (s *mixCoordImpl) GetStatisticsChannel(ctx context.Context, req *internalpb.GetStatisticsChannelRequest) (*milvuspb.StringResponse, error) {
	return &milvuspb.StringResponse{
		Status: merr.Success(),
		Value:  Params.CommonCfg.RootCoordStatistics.GetValue(),
	}, nil
}

// GetMetrics get metrics
func (s *mixCoordImpl) GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return s.rootcoordServer.GetMetrics(ctx, in)
}

// QueryCoordServer
func (s *mixCoordImpl) ActivateChecker(ctx context.Context, req *querypb.ActivateCheckerRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.ActivateChecker(ctx, req)
}

func (s *mixCoordImpl) DeactivateChecker(ctx context.Context, req *querypb.DeactivateCheckerRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.DeactivateChecker(ctx, req)
}

func (s *mixCoordImpl) ListCheckers(ctx context.Context, req *querypb.ListCheckersRequest) (*querypb.ListCheckersResponse, error) {
	return s.queryCoordServer.ListCheckers(ctx, req)
}

func (s *mixCoordImpl) ShowLoadCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error) {
	return s.queryCoordServer.ShowLoadCollections(ctx, req)
}

func (s *mixCoordImpl) LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.LoadCollection(ctx, req)
}

func (s *mixCoordImpl) ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.ReleaseCollection(ctx, req)
}

func (s *mixCoordImpl) ShowLoadPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error) {
	return s.queryCoordServer.ShowLoadPartitions(ctx, req)
}

func (s *mixCoordImpl) LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.LoadPartitions(ctx, req)
}

func (s *mixCoordImpl) ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.ReleasePartitions(ctx, req)
}

func (s *mixCoordImpl) SyncNewCreatedPartition(ctx context.Context, req *querypb.SyncNewCreatedPartitionRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.SyncNewCreatedPartition(ctx, req)
}

func (s *mixCoordImpl) GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error) {
	return s.queryCoordServer.GetPartitionStates(ctx, req)
}

func (s *mixCoordImpl) GetLoadSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error) {
	return s.queryCoordServer.GetLoadSegmentInfo(ctx, req)
}

func (s *mixCoordImpl) LoadBalance(ctx context.Context, req *querypb.LoadBalanceRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.LoadBalance(ctx, req)
}

func (s *mixCoordImpl) GetReplicas(ctx context.Context, req *milvuspb.GetReplicasRequest) (*milvuspb.GetReplicasResponse, error) {
	return s.queryCoordServer.GetReplicas(ctx, req)
}

func (s *mixCoordImpl) GetShardLeaders(ctx context.Context, req *querypb.GetShardLeadersRequest) (*querypb.GetShardLeadersResponse, error) {
	return s.queryCoordServer.GetShardLeaders(ctx, req)
}

func (s *mixCoordImpl) CreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.CreateResourceGroup(ctx, req)
}

func (s *mixCoordImpl) UpdateResourceGroups(ctx context.Context, req *querypb.UpdateResourceGroupsRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.UpdateResourceGroups(ctx, req)
}

func (s *mixCoordImpl) DropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.DropResourceGroup(ctx, req)
}

func (s *mixCoordImpl) TransferNode(ctx context.Context, req *milvuspb.TransferNodeRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.TransferNode(ctx, req)
}

func (s *mixCoordImpl) TransferReplica(ctx context.Context, req *querypb.TransferReplicaRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.TransferReplica(ctx, req)
}

func (s *mixCoordImpl) ListResourceGroups(ctx context.Context, req *milvuspb.ListResourceGroupsRequest) (*milvuspb.ListResourceGroupsResponse, error) {
	return s.queryCoordServer.ListResourceGroups(ctx, req)
}

func (s *mixCoordImpl) DescribeResourceGroup(ctx context.Context, req *querypb.DescribeResourceGroupRequest) (*querypb.DescribeResourceGroupResponse, error) {
	return s.queryCoordServer.DescribeResourceGroup(ctx, req)
}

func (s *mixCoordImpl) ListQueryNode(ctx context.Context, req *querypb.ListQueryNodeRequest) (*querypb.ListQueryNodeResponse, error) {
	return s.queryCoordServer.ListQueryNode(ctx, req)
}

func (s *mixCoordImpl) GetQueryNodeDistribution(ctx context.Context, req *querypb.GetQueryNodeDistributionRequest) (*querypb.GetQueryNodeDistributionResponse, error) {
	return s.queryCoordServer.GetQueryNodeDistribution(ctx, req)
}

func (s *mixCoordImpl) SuspendBalance(ctx context.Context, req *querypb.SuspendBalanceRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.SuspendBalance(ctx, req)
}

func (s *mixCoordImpl) ResumeBalance(ctx context.Context, req *querypb.ResumeBalanceRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.ResumeBalance(ctx, req)
}

func (s *mixCoordImpl) CheckBalanceStatus(ctx context.Context, req *querypb.CheckBalanceStatusRequest) (*querypb.CheckBalanceStatusResponse, error) {
	return s.queryCoordServer.CheckBalanceStatus(ctx, req)
}

func (s *mixCoordImpl) SuspendNode(ctx context.Context, req *querypb.SuspendNodeRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.SuspendNode(ctx, req)
}

func (s *mixCoordImpl) ResumeNode(ctx context.Context, req *querypb.ResumeNodeRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.ResumeNode(ctx, req)
}

func (s *mixCoordImpl) TransferSegment(ctx context.Context, req *querypb.TransferSegmentRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.TransferSegment(ctx, req)
}

func (s *mixCoordImpl) TransferChannel(ctx context.Context, req *querypb.TransferChannelRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.TransferChannel(ctx, req)
}

func (s *mixCoordImpl) CheckQueryNodeDistribution(ctx context.Context, req *querypb.CheckQueryNodeDistributionRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.CheckQueryNodeDistribution(ctx, req)
}

func (s *mixCoordImpl) UpdateLoadConfig(ctx context.Context, req *querypb.UpdateLoadConfigRequest) (*commonpb.Status, error) {
	return s.queryCoordServer.UpdateLoadConfig(ctx, req)
}

// DataCoordServer
func (s *mixCoordImpl) GetSegmentInfo(ctx context.Context, req *datapb.GetSegmentInfoRequest) (*datapb.GetSegmentInfoResponse, error) {
	return s.datacoordServer.GetSegmentInfo(ctx, req)
}

func (s *mixCoordImpl) Flush(ctx context.Context, req *datapb.FlushRequest) (*datapb.FlushResponse, error) {
	return s.datacoordServer.Flush(ctx, req)
}

func (s *mixCoordImpl) AssignSegmentID(ctx context.Context, req *datapb.AssignSegmentIDRequest) (*datapb.AssignSegmentIDResponse, error) {
	return s.datacoordServer.AssignSegmentID(ctx, req)
}

func (s *mixCoordImpl) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	return s.datacoordServer.GetSegmentStates(ctx, req)
}

func (s *mixCoordImpl) GetInsertBinlogPaths(ctx context.Context, req *datapb.GetInsertBinlogPathsRequest) (*datapb.GetInsertBinlogPathsResponse, error) {
	return s.datacoordServer.GetInsertBinlogPaths(ctx, req)
}

func (s *mixCoordImpl) GetCollectionStatistics(ctx context.Context, req *datapb.GetCollectionStatisticsRequest) (*datapb.GetCollectionStatisticsResponse, error) {
	return s.datacoordServer.GetCollectionStatistics(ctx, req)
}

func (s *mixCoordImpl) GetPartitionStatistics(ctx context.Context, req *datapb.GetPartitionStatisticsRequest) (*datapb.GetPartitionStatisticsResponse, error) {
	return s.datacoordServer.GetPartitionStatistics(ctx, req)
}

func (s *mixCoordImpl) GetSegmentInfoChannel(ctx context.Context, req *datapb.GetSegmentInfoChannelRequest) (*milvuspb.StringResponse, error) {
	return s.datacoordServer.GetSegmentInfoChannel(ctx, req)
}

func (s *mixCoordImpl) SaveBinlogPaths(ctx context.Context, req *datapb.SaveBinlogPathsRequest) (*commonpb.Status, error) {
	return s.datacoordServer.SaveBinlogPaths(ctx, req)
}

func (s *mixCoordImpl) GetRecoveryInfo(ctx context.Context, req *datapb.GetRecoveryInfoRequest) (*datapb.GetRecoveryInfoResponse, error) {
	return s.datacoordServer.GetRecoveryInfo(ctx, req)
}

func (s *mixCoordImpl) GetRecoveryInfoV2(ctx context.Context, req *datapb.GetRecoveryInfoRequestV2) (*datapb.GetRecoveryInfoResponseV2, error) {
	return s.datacoordServer.GetRecoveryInfoV2(ctx, req)
}

func (s *mixCoordImpl) GetChannelRecoveryInfo(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest) (*datapb.GetChannelRecoveryInfoResponse, error) {
	return s.datacoordServer.GetChannelRecoveryInfo(ctx, req)
}

func (s *mixCoordImpl) GetFlushedSegments(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
	return s.datacoordServer.GetFlushedSegments(ctx, req)
}

func (s *mixCoordImpl) GetSegmentsByStates(ctx context.Context, req *datapb.GetSegmentsByStatesRequest) (*datapb.GetSegmentsByStatesResponse, error) {
	return s.datacoordServer.GetSegmentsByStates(ctx, req)
}

func (s *mixCoordImpl) ManualCompaction(ctx context.Context, req *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
	return s.datacoordServer.ManualCompaction(ctx, req)
}

func (s *mixCoordImpl) GetCompactionState(ctx context.Context, req *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
	return s.datacoordServer.GetCompactionState(ctx, req)
}

func (s *mixCoordImpl) GetCompactionStateWithPlans(ctx context.Context, req *milvuspb.GetCompactionPlansRequest) (*milvuspb.GetCompactionPlansResponse, error) {
	return s.datacoordServer.GetCompactionStateWithPlans(ctx, req)
}

func (s *mixCoordImpl) WatchChannels(ctx context.Context, req *datapb.WatchChannelsRequest) (*datapb.WatchChannelsResponse, error) {
	return s.datacoordServer.WatchChannels(ctx, req)
}

func (s *mixCoordImpl) GetFlushState(ctx context.Context, req *datapb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
	return s.datacoordServer.GetFlushState(ctx, req)
}

func (s *mixCoordImpl) GetFlushAllState(ctx context.Context, req *milvuspb.GetFlushAllStateRequest) (*milvuspb.GetFlushAllStateResponse, error) {
	return s.datacoordServer.GetFlushAllState(ctx, req)
}

func (s *mixCoordImpl) DropVirtualChannel(ctx context.Context, req *datapb.DropVirtualChannelRequest) (*datapb.DropVirtualChannelResponse, error) {
	return s.datacoordServer.DropVirtualChannel(ctx, req)
}

func (s *mixCoordImpl) SetSegmentState(ctx context.Context, req *datapb.SetSegmentStateRequest) (*datapb.SetSegmentStateResponse, error) {
	return s.datacoordServer.SetSegmentState(ctx, req)
}

func (s *mixCoordImpl) UpdateSegmentStatistics(ctx context.Context, req *datapb.UpdateSegmentStatisticsRequest) (*commonpb.Status, error) {
	return s.datacoordServer.UpdateSegmentStatistics(ctx, req)
}

func (s *mixCoordImpl) UpdateChannelCheckpoint(ctx context.Context, req *datapb.UpdateChannelCheckpointRequest) (*commonpb.Status, error) {
	return s.datacoordServer.UpdateChannelCheckpoint(ctx, req)
}

func (s *mixCoordImpl) MarkSegmentsDropped(ctx context.Context, req *datapb.MarkSegmentsDroppedRequest) (*commonpb.Status, error) {
	return s.datacoordServer.MarkSegmentsDropped(ctx, req)
}

func (s *mixCoordImpl) BroadcastAlteredCollection(ctx context.Context, req *datapb.AlterCollectionRequest) (*commonpb.Status, error) {
	return s.datacoordServer.BroadcastAlteredCollection(ctx, req)
}

func (s *mixCoordImpl) GcConfirm(ctx context.Context, req *datapb.GcConfirmRequest) (*datapb.GcConfirmResponse, error) {
	return s.datacoordServer.GcConfirm(ctx, req)
}

func (s *mixCoordImpl) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return s.datacoordServer.CreateIndex(ctx, req)
}

func (s *mixCoordImpl) AlterIndex(ctx context.Context, req *indexpb.AlterIndexRequest) (*commonpb.Status, error) {
	return s.datacoordServer.AlterIndex(ctx, req)
}

func (s *mixCoordImpl) GetIndexState(ctx context.Context, req *indexpb.GetIndexStateRequest) (*indexpb.GetIndexStateResponse, error) {
	return s.datacoordServer.GetIndexState(ctx, req)
}

func (s *mixCoordImpl) GetSegmentIndexState(ctx context.Context, req *indexpb.GetSegmentIndexStateRequest) (*indexpb.GetSegmentIndexStateResponse, error) {
	return s.datacoordServer.GetSegmentIndexState(ctx, req)
}

func (s *mixCoordImpl) GetIndexInfos(ctx context.Context, req *indexpb.GetIndexInfoRequest) (*indexpb.GetIndexInfoResponse, error) {
	return s.datacoordServer.GetIndexInfos(ctx, req)
}

func (s *mixCoordImpl) DescribeIndex(ctx context.Context, req *indexpb.DescribeIndexRequest) (*indexpb.DescribeIndexResponse, error) {
	return s.datacoordServer.DescribeIndex(ctx, req)
}

func (s *mixCoordImpl) GetIndexStatistics(ctx context.Context, req *indexpb.GetIndexStatisticsRequest) (*indexpb.GetIndexStatisticsResponse, error) {
	return s.datacoordServer.GetIndexStatistics(ctx, req)
}

func (s *mixCoordImpl) DropIndex(ctx context.Context, req *indexpb.DropIndexRequest) (*commonpb.Status, error) {
	return s.datacoordServer.DropIndex(ctx, req)
}

func (s *mixCoordImpl) GetIndexBuildProgress(ctx context.Context, req *indexpb.GetIndexBuildProgressRequest) (*indexpb.GetIndexBuildProgressResponse, error) {
	return s.datacoordServer.GetIndexBuildProgress(ctx, req)
}

func (s *mixCoordImpl) ReportDataNodeTtMsgs(ctx context.Context, req *datapb.ReportDataNodeTtMsgsRequest) (*commonpb.Status, error) {
	return s.datacoordServer.ReportDataNodeTtMsgs(ctx, req)
}

func (s *mixCoordImpl) GcControl(ctx context.Context, req *datapb.GcControlRequest) (*commonpb.Status, error) {
	return s.datacoordServer.GcControl(ctx, req)
}

func (s *mixCoordImpl) ImportV2(ctx context.Context, req *internalpb.ImportRequestInternal) (*internalpb.ImportResponse, error) {
	return s.datacoordServer.ImportV2(ctx, req)
}

func (s *mixCoordImpl) GetImportProgress(ctx context.Context, req *internalpb.GetImportProgressRequest) (*internalpb.GetImportProgressResponse, error) {
	return s.datacoordServer.GetImportProgress(ctx, req)
}

func (s *mixCoordImpl) ListImports(ctx context.Context, req *internalpb.ListImportsRequestInternal) (*internalpb.ListImportsResponse, error) {
	return s.datacoordServer.ListImports(ctx, req)
}

func (s *mixCoordImpl) ListIndexes(ctx context.Context, req *indexpb.ListIndexesRequest) (*indexpb.ListIndexesResponse, error) {
	return s.datacoordServer.ListIndexes(ctx, req)
}

func (s *mixCoordImpl) AllocSegment(ctx context.Context, req *datapb.AllocSegmentRequest) (*datapb.AllocSegmentResponse, error) {
	return s.datacoordServer.AllocSegment(ctx, req)
}

// RegisterStreamingCoordGRPCService registers the grpc service of streaming coordinator.
func (s *mixCoordImpl) RegisterStreamingCoordGRPCService(server *grpc.Server) {
	s.streamingCoord.RegisterGRPCService(server)
}
