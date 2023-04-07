package etcd

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/registry/options"
	milvuscommon "github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"google.golang.org/grpc"
)

type EtcdSDSuite struct {
	// suite fields
	etcd      *embed.Etcd
	tmpFolder string

	// test fields
	prefix   string
	etcdCli  *clientv3.Client
	sd       *etcdServiceDiscovery
	sessions []registry.Session

	suite.Suite
}

func (s *EtcdSDSuite) SetupSuite() {
	paramtable.Init()
	server, tmpFolder, err := etcd.StartTestEmbedEtcdServer()
	s.Require().NoError(err)
	s.etcd = server
	s.tmpFolder = tmpFolder

}

func (s *EtcdSDSuite) TearDownSuite() {
	s.etcd.Server.Stop()
	os.RemoveAll(s.tmpFolder)
}

func (s *EtcdSDSuite) SetupTest() {
	s.prefix = funcutil.RandomString(6)
	s.etcdCli = v3client.New(s.etcd.Server)
	raw := NewEtcdServiceDiscovery(s.etcdCli, s.prefix)
	var ok bool
	s.sd, ok = raw.(*etcdServiceDiscovery)
	s.Require().True(ok)
}

func (s *EtcdSDSuite) TearDownTest() {
	s.etcdCli.Delete(context.Background(), s.prefix, clientv3.WithPrefix())
	s.etcdCli.Close()
}

func (s *EtcdSDSuite) TestGetServices() {
	sessions := s.prepareDataset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		for _, serverType := range typeutil.ServerTypeList() {
			target := lo.Filter(sessions, func(session registry.Session, _ int) bool {
				return session.ComponentType() == serverType
			})
			m := lo.SliceToMap(target, func(session registry.Session) (int64, registry.Session) {
				return session.ID(), session
			})

			s.Run(serverType, func() {
				entries, err := s.sd.GetServices(ctx, serverType)
				s.NoError(err)
				s.Equal(len(target), len(entries))
				for _, entry := range entries {
					s.Equal(serverType, entry.ComponentType())
					targetEntry, ok := m[entry.ID()]
					if s.True(ok) {
						s.Equal(targetEntry.Addr(), entry.Addr())
					}
				}
			})
		}
	})

	s.Run("ctx_canceld", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		for _, serverType := range typeutil.ServerTypeList() {
			s.Run(serverType, func() {
				_, err := s.sd.GetServices(ctx, serverType)
				s.Error(err)
			})
		}
	})

}

type MockRCServer struct {
	mock.Mock
	*rootcoordpb.UnimplementedRootCoordServer
}

func (s *MockRCServer) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	ret := s.Called(ctx, req)

	var r0 *milvuspb.ComponentStates
	if rf, ok := ret.Get(0).(func(ctx context.Context, req *milvuspb.GetComponentStatesRequest) *milvuspb.ComponentStates); ok {
		r0 = rf(ctx, req)
	} else {
		r0 = ret.Get(0).(*milvuspb.ComponentStates)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(ctx context.Context, req *milvuspb.GetComponentStatesRequest) error); ok {
		r1 = rf(ctx, req)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type MockDCServer struct {
	mock.Mock
	*datapb.UnimplementedDataCoordServer
}

func (s *MockDCServer) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	ret := s.Called(ctx, req)

	var r0 *milvuspb.ComponentStates
	if rf, ok := ret.Get(0).(func(ctx context.Context, req *milvuspb.GetComponentStatesRequest) *milvuspb.ComponentStates); ok {
		r0 = rf(ctx, req)
	} else {
		r0 = ret.Get(0).(*milvuspb.ComponentStates)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(ctx context.Context, req *milvuspb.GetComponentStatesRequest) error); ok {
		r1 = rf(ctx, req)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type MockQCServer struct {
	mock.Mock
	*querypb.UnimplementedQueryCoordServer
}

func (s *MockQCServer) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	ret := s.Called(ctx, req)

	var r0 *milvuspb.ComponentStates
	if rf, ok := ret.Get(0).(func(ctx context.Context, req *milvuspb.GetComponentStatesRequest) *milvuspb.ComponentStates); ok {
		r0 = rf(ctx, req)
	} else {
		r0 = ret.Get(0).(*milvuspb.ComponentStates)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(ctx context.Context, req *milvuspb.GetComponentStatesRequest) error); ok {
		r1 = rf(ctx, req)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (s *EtcdSDSuite) TestGetRootCoord() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("not_available", func() {
		rc, err := s.sd.GetRootCoord(ctx)
		s.NoError(err)

		_, err = rc.GetComponentStates(ctx)
		s.Error(err)
	})

	// start grpc server
	port := funcutil.GetAvailablePort()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	s.Require().NoError(err)
	server := grpc.NewServer()
	mockRC := &MockRCServer{}
	rootcoordpb.RegisterRootCoordServer(server, mockRC)

	go func() {
		server.Serve(lis)
	}()

	session, err := s.sd.RegisterRootCoord(ctx, nil, fmt.Sprintf("localhost:%d", port))
	s.Require().NoError(err)
	s.setupSession(ctx, session)

	s.Run("success", func() {
		mockRC.On("GetComponentStates", mock.Anything, mock.AnythingOfType("*milvuspb.GetComponentStatesRequest")).Return(&milvuspb.ComponentStates{
			Status: &commonpb.Status{},
		}, nil)
		rc, err := s.sd.GetRootCoord(ctx)
		s.NoError(err)

		_, err = rc.GetComponentStates(ctx)
		s.NoError(err)

		mockRC.AssertCalled(s.T(), "GetComponentStates", mock.Anything, mock.AnythingOfType("*milvuspb.GetComponentStatesRequest"))
	})

	s.Run("getEntry_ContextCancelled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rc, err := s.sd.GetRootCoord(ctx)
		s.NoError(err)

		_, err = rc.GetComponentStates(ctx)
		s.Error(err)
	})
}

func (s *EtcdSDSuite) TestGetDataCoord() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := funcutil.GetAvailablePort()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	s.Require().NoError(err)
	server := grpc.NewServer()
	mockDC := &MockDCServer{}
	datapb.RegisterDataCoordServer(server, mockDC)

	go func() {
		server.Serve(lis)
	}()

	session, err := s.sd.RegisterDataCoord(ctx, nil, fmt.Sprintf("localhost:%d", port))
	s.Require().NoError(err)
	err = session.Init(ctx)
	s.Require().NoError(err)
	err = session.Register(ctx)
	s.Require().NoError(err)

	mockDC.On("GetComponentStates", mock.Anything, mock.AnythingOfType("*milvuspb.GetComponentStatesRequest")).Return(&milvuspb.ComponentStates{
		Status: &commonpb.Status{},
	}, nil)
	dc, err := s.sd.GetDataCoord(ctx)
	s.NoError(err)

	_, err = dc.GetComponentStates(ctx)
	s.NoError(err)

	mockDC.AssertCalled(s.T(), "GetComponentStates", mock.Anything, mock.AnythingOfType("*milvuspb.GetComponentStatesRequest"))
}

func (s *EtcdSDSuite) TestGetQueryCoord() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port := funcutil.GetAvailablePort()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	s.Require().NoError(err)
	server := grpc.NewServer()
	mockQC := &MockQCServer{}
	querypb.RegisterQueryCoordServer(server, mockQC)

	go func() {
		server.Serve(lis)
	}()

	session, err := s.sd.RegisterQueryCoord(ctx, nil, fmt.Sprintf("localhost:%d", port))
	s.Require().NoError(err)
	s.setupSession(ctx, session)

	mockQC.On("GetComponentStates", mock.Anything, mock.AnythingOfType("*milvuspb.GetComponentStatesRequest")).Return(&milvuspb.ComponentStates{
		Status: &commonpb.Status{},
	}, nil)
	qc, err := s.sd.GetQueryCoord(ctx)
	s.NoError(err)

	_, err = qc.GetComponentStates(ctx)
	s.NoError(err)

	mockQC.AssertCalled(s.T(), "GetComponentStates", mock.Anything, mock.AnythingOfType("*milvuspb.GetComponentStatesRequest"))
}

func (s *EtcdSDSuite) TestWatchService() {
	sessions := s.prepareDataset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, serverType := range []string{typeutil.ProxyRole, typeutil.DataNodeRole, typeutil.IndexNodeRole, typeutil.QueryNodeRole} {

		target := lo.Filter(sessions, func(session registry.Session, _ int) bool {
			return session.ComponentType() == serverType
		})
		m := lo.SliceToMap(target, func(session registry.Session) (int64, registry.Session) {
			return session.ID(), session
		})

		s.Run(serverType, func() {
			current, watcher, err := s.sd.WatchServices(ctx, serverType)
			s.NoError(err)
			s.Equal(len(target), len(current))
			for _, entry := range current {
				s.Equal(serverType, entry.ComponentType())
				targetEntry, ok := m[entry.ID()]
				if s.True(ok) {
					s.Equal(targetEntry.Addr(), entry.Addr())
				}
			}

			session, err := s.sd.registerService(ctx, serverType, "addr")
			s.Require().NoError(err)
			s.setupSession(ctx, session)

			select {
			case evt := <-watcher.Watch():
				s.Equal(registry.SessionAddEvent, evt.EventType)
				s.Equal(session.Addr(), evt.Entry.Addr())
				s.Equal(session.ID(), evt.Entry.ID())
			case <-time.After(time.Second):
				s.FailNow("no watch event after 1 second")
			}
		})
	}
}

func (s *EtcdSDSuite) TestListWatch() {
	sessions := s.prepareDataset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		for _, serverType := range []string{typeutil.ProxyRole, typeutil.DataNodeRole, typeutil.IndexNodeRole, typeutil.QueryNodeRole} {

			target := lo.Filter(sessions, func(session registry.Session, _ int) bool {
				return session.ComponentType() == serverType
			})
			m := lo.SliceToMap(target, func(session registry.Session) (int64, registry.Session) {
				return session.ID(), session
			})

			s.Run(serverType, func() {
				//current, watcher, err := s.sd.WatchServices(ctx, serverType)
				current, watcher, err := listWatch(ctx, s.sd, serverType, func(ctx context.Context, entry registry.ServiceEntry) (registry.ServiceEntry, error) {
					return entry, nil
				})
				s.NoError(err)
				s.Equal(len(target), len(current))
				for _, entry := range current {
					s.Equal(serverType, entry.ComponentType())
					targetEntry, ok := m[entry.ID()]
					if s.True(ok) {
						s.Equal(targetEntry.Addr(), entry.Addr())
					}
				}

				session, err := s.sd.registerService(ctx, serverType, "addr")
				s.Require().NoError(err)
				s.setupSession(ctx, session)

				select {
				case evt := <-watcher.Watch():
					s.Equal(registry.SessionAddEvent, evt.EventType)
					s.Equal(session.Addr(), evt.Entry.Addr())
					s.Equal(session.ID(), evt.Entry.ID())
				case <-time.After(time.Second):
					s.FailNow("no watch event after 1 second")
				}
			})
		}
	})

	s.Run("ctx_canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, serverType := range []string{typeutil.ProxyRole, typeutil.DataNodeRole, typeutil.IndexNodeRole, typeutil.QueryNodeRole} {
			s.Run(serverType, func() {
				_, _, err := listWatch(ctx, s.sd, serverType, func(ctx context.Context, entry registry.ServiceEntry) (registry.ServiceEntry, error) {
					return entry, nil
				})
				s.Error(err)
			})
		}
	})

	s.Run("converter_error", func() {
		for _, serverType := range []string{typeutil.ProxyRole, typeutil.DataNodeRole, typeutil.IndexNodeRole, typeutil.QueryNodeRole} {
			s.Run(serverType, func() {
				current, _, err := listWatch(ctx, s.sd, serverType, func(ctx context.Context, entry registry.ServiceEntry) (registry.ServiceEntry, error) {
					return nil, errors.New("mocked")
				})
				s.NoError(err)
				s.Equal(0, len(current))
			})
		}
	})
}

func (s *EtcdSDSuite) setupSession(ctx context.Context, session registry.Session) {
	err := session.Init(ctx)
	s.Require().NoError(err)
	err = session.Register(ctx)
	s.Require().NoError(err)
}

func (s *EtcdSDSuite) prepareDataset() []registry.Session {
	var sessions []registry.Session
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session, err := s.sd.RegisterProxy(ctx, nil, "proxy-addr", options.WithTriggerKill(false))
	s.Require().NoError(err)
	s.setupSession(ctx, session)
	sessions = append(sessions, session)

	session, err = s.sd.RegisterRootCoord(ctx, nil, "rc-addr", options.WithTriggerKill(false))
	s.Require().NoError(err)
	s.setupSession(ctx, session)
	sessions = append(sessions, session)

	session, err = s.sd.RegisterQueryCoord(ctx, nil, "qc-addr", options.WithTriggerKill(false))
	s.Require().NoError(err)
	s.setupSession(ctx, session)
	sessions = append(sessions, session)

	session, err = s.sd.RegisterDataCoord(ctx, nil, "dc-addr", options.WithTriggerKill(false))
	s.Require().NoError(err)
	s.setupSession(ctx, session)
	sessions = append(sessions, session)

	session, err = s.sd.RegisterDataNode(ctx, nil, "dn-addr", options.WithTriggerKill(false))
	s.Require().NoError(err)
	s.setupSession(ctx, session)
	sessions = append(sessions, session)

	session, err = s.sd.RegisterQueryNode(ctx, nil, "qn-addr", options.WithTriggerKill(false))
	s.Require().NoError(err)
	s.setupSession(ctx, session)
	sessions = append(sessions, session)

	session, err = s.sd.RegisterIndexNode(ctx, nil, "in-addr", options.WithTriggerKill(false))
	s.Require().NoError(err)
	s.setupSession(ctx, session)
	sessions = append(sessions, session)

	// garbage data
	_, err = s.etcdCli.Put(ctx, path.Join(s.prefix, milvuscommon.DefaultServiceRoot, "proxy-9999"), "")
	s.Require().NoError(err)

	return sessions
}

func TestEtcdServiceDiscovery(t *testing.T) {
	suite.Run(t, new(EtcdSDSuite))
}
