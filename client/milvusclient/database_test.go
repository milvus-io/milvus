package milvusclient

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type DatabaseSuite struct {
	MockSuiteBase
}

func (s *DatabaseSuite) TestListDatabases() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		mockListDatabases := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ListDatabases).Return(&milvuspb.ListDatabasesResponse{
			Status:  merr.Success(),
			DbNames: []string{"default", "db1"},
		}, nil).Build()
		defer mockListDatabases.UnPatch()

		names, err := s.client.ListDatabase(ctx, NewListDatabaseOption())
		s.NoError(err)
		s.ElementsMatch([]string{"default", "db1"}, names)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		mockListDatabases := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ListDatabases).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockListDatabases.UnPatch()

		_, err := s.client.ListDatabase(ctx, NewListDatabaseOption())
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestCreateDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		mockCreateDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).CreateDatabase).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, cdr *milvuspb.CreateDatabaseRequest) (*commonpb.Status, error) {
			s.Equal(dbName, cdr.GetDbName())
			return merr.Success(), nil
		}).Build()
		defer mockCreateDatabase.UnPatch()

		err := s.client.CreateDatabase(ctx, NewCreateDatabaseOption(dbName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		mockCreateDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).CreateDatabase).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockCreateDatabase.UnPatch()

		err := s.client.CreateDatabase(ctx, NewCreateDatabaseOption(dbName))
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestDropDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		mockDropDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DropDatabase).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, ddr *milvuspb.DropDatabaseRequest) (*commonpb.Status, error) {
			s.Equal(dbName, ddr.GetDbName())
			return merr.Success(), nil
		}).Build()
		defer mockDropDatabase.UnPatch()

		err := s.client.DropDatabase(ctx, NewDropDatabaseOption(dbName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		mockDropDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DropDatabase).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockDropDatabase.UnPatch()

		err := s.client.DropDatabase(ctx, NewDropDatabaseOption(dbName))
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestUseDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		// Note: Connect is already mocked in SetupSuite, so we don't need to mock it again.
		// The existing Connect mock returns a valid ConnectResponse.
		dbName := fmt.Sprintf("dt_%s", s.randString(6))

		err := s.client.UseDatabase(ctx, NewUseDatabaseOption(dbName))
		s.NoError(err)

		s.Equal(dbName, s.client.currentDB)
	})
}

func (s *DatabaseSuite) TestDescribeDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		key := fmt.Sprintf("key_%s", s.randString(4))
		value := s.randString(6)
		mockDescribeDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeDatabase).Return(&milvuspb.DescribeDatabaseResponse{
			Status: merr.Success(),
			DbName: dbName,
			Properties: []*commonpb.KeyValuePair{
				{Key: key, Value: value},
			},
		}, nil).Build()
		defer mockDescribeDatabase.UnPatch()

		db, err := s.client.DescribeDatabase(ctx, NewDescribeDatabaseOption(dbName))
		s.NoError(err)
		s.Equal(dbName, db.Name)
		s.Equal(value, db.Properties[key])
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		mockDescribeDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DescribeDatabase).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockDescribeDatabase.UnPatch()

		_, err := s.client.DescribeDatabase(ctx, NewDescribeDatabaseOption(dbName))
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestAlterDatabaseProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		key := fmt.Sprintf("key_%s", s.randString(4))
		value := s.randString(6)
		mockAlterDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterDatabase).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, adr *milvuspb.AlterDatabaseRequest) (*commonpb.Status, error) {
			s.Equal(dbName, adr.GetDbName())
			s.Len(adr.GetProperties(), 1)
			return merr.Success(), nil
		}).Build()
		defer mockAlterDatabase.UnPatch()

		err := s.client.AlterDatabaseProperties(ctx, NewAlterDatabasePropertiesOption(dbName).WithProperty(key, value))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		mockAlterDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterDatabase).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockAlterDatabase.UnPatch()

		err := s.client.AlterDatabaseProperties(ctx, NewAlterDatabasePropertiesOption(dbName).WithProperty("key", "value"))
		s.Error(err)
	})
}

func (s *DatabaseSuite) TestDropDatabaseProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		key := fmt.Sprintf("key_%s", s.randString(4))
		mockAlterDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterDatabase).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, adr *milvuspb.AlterDatabaseRequest) (*commonpb.Status, error) {
			s.Equal([]string{key}, adr.GetDeleteKeys())
			return merr.Success(), nil
		}).Build()
		defer mockAlterDatabase.UnPatch()

		err := s.client.DropDatabaseProperties(ctx, NewDropDatabasePropertiesOption(dbName, key))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		dbName := fmt.Sprintf("dt_%s", s.randString(6))
		mockAlterDatabase := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).AlterDatabase).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockAlterDatabase.UnPatch()

		err := s.client.DropDatabaseProperties(ctx, NewDropDatabasePropertiesOption(dbName, "key"))
		s.Error(err)
	})
}

func TestDatabase(t *testing.T) {
	suite.Run(t, new(DatabaseSuite))
}
