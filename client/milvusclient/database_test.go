package milvusclient

import (
	"context"
	"fmt"
	"testing"

	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type DatabaseSuite struct {
	MockSuiteBase
}

func (s *DatabaseSuite) TestListDatabases() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"default", "db1"},
		}, nil).Once()

		names, err := s.client.ListDatabase(ctx, NewListDatabaseOption())
		s.NoError(err)
		s.ElementsMatch([]string{"default", "db1"}, names)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListDatabase(ctx, NewListDatabaseOption())
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestCreateDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		s.mock.EXPECT().CreateDatabase(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cdr *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
			s.Equal(dbName, cdr.GetDbName())
			return merr.Success(), nil
		}).Once()

		err := s.client.CreateDatabase(ctx, NewCreateDatabaseOption(dbName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		s.mock.EXPECT().CreateDatabase(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.CreateDatabase(ctx, NewCreateDatabaseOption(dbName))
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestDropDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		s.mock.EXPECT().DropDatabase(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ddr *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
			s.Equal(dbName, ddr.GetDbName())
			return merr.Success(), nil
		}).Once()

		err := s.client.DropDatabase(ctx, NewDropDatabaseOption(dbName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		s.mock.EXPECT().DropDatabase(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropDatabase(ctx, NewDropDatabaseOption(dbName))
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestUseDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		s.mock.EXPECT().Connect(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cr *milvuspb.ConnectRequest) (*milvuspb.ConnectResponse, error) {
			return &milvuspb.ConnectResponse{
				Status:     merr.Success(),
				ServerInfo: &commonpb.ServerInfo{},
			}, nil
		}).Once()

		err := s.client.UseDatabase(ctx, NewUseDatabaseOption(dbName))
		s.NoError(err)

		s.Equal(dbName, s.client.currentDB)
	})
}

func TestDatabase(t *testing.T) {
	suite.Run(t, new(DatabaseSuite))
}
