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

package milvusclient

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v2/internal/merr"
)

type SnapshotSuite struct {
	MockSuiteBase
}

func (s *SnapshotSuite) TestCreateSnapshot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		description := "test snapshot description"

		s.mock.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.CreateSnapshotRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, req.GetCollectionName())
			s.Equal(snapshotName, req.GetName())
			s.Equal(description, req.GetDescription())
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
		}).Once()

		opt := NewCreateSnapshotOption(snapshotName, collectionName).
			WithDescription(description)
		err := s.client.CreateSnapshot(ctx, opt)
		s.NoError(err)
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))

		s.mock.EXPECT().CreateSnapshot(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error")).Once()

		opt := NewCreateSnapshotOption(snapshotName, collectionName)
		err := s.client.CreateSnapshot(ctx, opt)
		s.Error(err)
	})

	s.Run("nil option", func() {
		err := s.client.CreateSnapshot(ctx, nil)
		s.Error(err)
	})
}

func (s *SnapshotSuite) TestDropSnapshot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))

		s.mock.EXPECT().DropSnapshot(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DropSnapshotRequest) (*commonpb.Status, error) {
			s.Equal(snapshotName, req.GetName())
			s.Equal(collectionName, req.GetCollectionName())
			return &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil
		}).Once()

		opt := NewDropSnapshotOption(snapshotName, collectionName)
		err := s.client.DropSnapshot(ctx, opt)
		s.NoError(err)
	})

	s.Run("failure", func() {
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))

		s.mock.EXPECT().DropSnapshot(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error")).Once()

		opt := NewDropSnapshotOption(snapshotName, collectionName)
		err := s.client.DropSnapshot(ctx, opt)
		s.Error(err)
	})

	s.Run("nil option", func() {
		err := s.client.DropSnapshot(ctx, nil)
		s.Error(err)
	})
}

func (s *SnapshotSuite) TestListSnapshots() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		expectedSnapshots := []string{"snapshot1", "snapshot2", "snapshot3"}
		dbName := "test_db"
		collectionName := "test_collection"

		s.mock.EXPECT().ListSnapshots(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ListSnapshotsRequest) (*milvuspb.ListSnapshotsResponse, error) {
			s.Equal(dbName, req.GetDbName())
			s.Equal(collectionName, req.GetCollectionName())
			return &milvuspb.ListSnapshotsResponse{
				Status:    merr.Success(),
				Snapshots: expectedSnapshots,
			}, nil
		}).Once()

		opt := NewListSnapshotsOption(collectionName).
			WithDbName(dbName)
		snapshots, err := s.client.ListSnapshots(ctx, opt)
		s.NoError(err)
		s.Equal(expectedSnapshots, snapshots)
	})

	s.Run("service error", func() {
		s.mock.EXPECT().ListSnapshots(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error")).Once()

		opt := NewListSnapshotsOption("test_collection")
		snapshots, err := s.client.ListSnapshots(ctx, opt)
		s.Error(err)
		s.Nil(snapshots)
	})

	s.Run("response error", func() {
		s.mock.EXPECT().ListSnapshots(mock.Anything, mock.Anything).Return(&milvuspb.ListSnapshotsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "test error",
			},
		}, nil).Once()

		opt := NewListSnapshotsOption("test_collection")
		snapshots, err := s.client.ListSnapshots(ctx, opt)
		s.Error(err)
		s.Nil(snapshots)
	})

	s.Run("nil option", func() {
		snapshots, err := s.client.ListSnapshots(ctx, nil)
		s.Error(err)
		s.Nil(snapshots)
	})
}

func (s *SnapshotSuite) TestDescribeSnapshot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))
		expectedResp := &milvuspb.DescribeSnapshotResponse{
			Status:         merr.Success(),
			Name:           snapshotName,
			Description:    "test description",
			CollectionName: collectionName,
			CreateTs:       1234567890,
			S3Location:     "s3://test-bucket/snapshot",
			PartitionNames: []string{"partition1", "partition2"},
		}

		s.mock.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeSnapshotRequest) (*milvuspb.DescribeSnapshotResponse, error) {
			s.Equal(snapshotName, req.GetName())
			s.Equal(collectionName, req.GetCollectionName())
			return expectedResp, nil
		}).Once()

		opt := NewDescribeSnapshotOption(snapshotName, collectionName)
		resp, err := s.client.DescribeSnapshot(ctx, opt)
		s.NoError(err)
		s.Equal(expectedResp.GetName(), resp.GetName())
		s.Equal(expectedResp.GetDescription(), resp.GetDescription())
		s.Equal(expectedResp.GetCollectionName(), resp.GetCollectionName())
		s.Equal(expectedResp.GetCreateTs(), resp.GetCreateTs())
		s.Equal(expectedResp.GetS3Location(), resp.GetS3Location())
		s.Equal(expectedResp.GetPartitionNames(), resp.GetPartitionNames())
	})

	s.Run("service error", func() {
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))

		s.mock.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).Return(nil, errors.New("mocked error")).Once()

		opt := NewDescribeSnapshotOption(snapshotName, collectionName)
		resp, err := s.client.DescribeSnapshot(ctx, opt)
		s.Error(err)
		s.Nil(resp)
	})

	s.Run("response error", func() {
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))

		s.mock.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).Return(&milvuspb.DescribeSnapshotResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "test error",
			},
		}, nil).Once()

		opt := NewDescribeSnapshotOption(snapshotName, collectionName)
		resp, err := s.client.DescribeSnapshot(ctx, opt)
		s.Error(err)
		// When there's a response error, the response object is still returned but with error status
		s.NotNil(resp)
		s.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	})

	s.Run("nil option", func() {
		resp, err := s.client.DescribeSnapshot(ctx, nil)
		s.Error(err)
		s.Nil(resp)
	})
}

func (s *SnapshotSuite) TestRestoreSnapshot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))
		targetCollectionName := fmt.Sprintf("restored_%s", s.randString(6))
		expectedJobID := int64(12345)

		s.mock.EXPECT().RestoreSnapshot(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.RestoreSnapshotRequest) (*milvuspb.RestoreSnapshotResponse, error) {
			s.Equal(snapshotName, req.GetName())
			s.Equal(collectionName, req.GetCollectionName())
			s.Equal(targetCollectionName, req.GetTargetCollectionName())
			return &milvuspb.RestoreSnapshotResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				JobId:  expectedJobID,
			}, nil
		}).Once()

		opt := NewRestoreSnapshotOption(snapshotName, collectionName, targetCollectionName)
		jobID, err := s.client.RestoreSnapshot(ctx, opt)
		s.NoError(err)
		s.Equal(expectedJobID, jobID)
	})

	s.Run("failure", func() {
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))
		targetCollectionName := fmt.Sprintf("restored_%s", s.randString(6))

		s.mock.EXPECT().RestoreSnapshot(mock.Anything, mock.Anything).Return((*milvuspb.RestoreSnapshotResponse)(nil), errors.New("mocked error")).Once()

		opt := NewRestoreSnapshotOption(snapshotName, collectionName, targetCollectionName)
		jobID, err := s.client.RestoreSnapshot(ctx, opt)
		s.Error(err)
		s.Equal(int64(0), jobID)
	})

	s.Run("nil option", func() {
		jobID, err := s.client.RestoreSnapshot(ctx, nil)
		s.Error(err)
		s.Equal(int64(0), jobID)
	})
}

func (s *SnapshotSuite) TestRestoreExternalSnapshot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		targetCollectionName := fmt.Sprintf("restored_%s", s.randString(6))
		metadataURI := "s3://bucket/files/snapshots/meta.json"
		expectedJobID := int64(2001)

		s.mock.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.RestoreExternalSnapshotRequest) (*milvuspb.RestoreExternalSnapshotResponse, error) {
			s.Equal(targetCollectionName, req.GetTargetCollectionName())
			s.Equal(metadataURI, req.GetSnapshotMetadataUri())
			return &milvuspb.RestoreExternalSnapshotResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				JobId:  expectedJobID,
			}, nil
		}).Once()

		jobID, err := s.client.RestoreExternalSnapshot(ctx,
			NewRestoreExternalSnapshotOption(targetCollectionName, metadataURI))
		s.NoError(err)
		s.Equal(expectedJobID, jobID)
	})

	s.Run("failure", func() {
		targetCollectionName := fmt.Sprintf("restored_%s", s.randString(6))
		metadataURI := "s3://bucket/files/snapshots/meta.json"

		s.mock.EXPECT().RestoreExternalSnapshot(mock.Anything, mock.Anything).Return((*milvuspb.RestoreExternalSnapshotResponse)(nil), errors.New("mocked error")).Once()

		jobID, err := s.client.RestoreExternalSnapshot(ctx,
			NewRestoreExternalSnapshotOption(targetCollectionName, metadataURI))
		s.Error(err)
		s.Equal(int64(0), jobID)
	})

	s.Run("nil option", func() {
		jobID, err := s.client.RestoreExternalSnapshot(ctx, nil)
		s.Error(err)
		s.Equal(int64(0), jobID)
	})
}

func (s *SnapshotSuite) TestExportSnapshot() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		dbName := "source_db"
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))
		targetS3Path := "s3://bucket/export-root"
		expectedURI := "s3://bucket/export-root/snapshots/100/metadata/1.json"

		s.mock.EXPECT().ExportSnapshot(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ExportSnapshotRequest) (*milvuspb.ExportSnapshotResponse, error) {
			s.Equal(snapshotName, req.GetName())
			s.Equal(dbName, req.GetDbName())
			s.Equal(collectionName, req.GetCollectionName())
			s.Equal(targetS3Path, req.GetTargetS3Path())
			return &milvuspb.ExportSnapshotResponse{
				Status:              &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				SnapshotMetadataUri: expectedURI,
			}, nil
		}).Once()

		metadataURI, err := s.client.ExportSnapshot(ctx,
			NewExportSnapshotOption(snapshotName, collectionName, targetS3Path).WithDbName(dbName))
		s.NoError(err)
		s.Equal(expectedURI, metadataURI)
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("collection_%s", s.randString(6))
		snapshotName := fmt.Sprintf("snapshot_%s", s.randString(6))

		s.mock.EXPECT().ExportSnapshot(mock.Anything, mock.Anything).Return((*milvuspb.ExportSnapshotResponse)(nil), errors.New("mocked error")).Once()

		metadataURI, err := s.client.ExportSnapshot(ctx,
			NewExportSnapshotOption(snapshotName, collectionName, "s3://bucket/export-root"))
		s.Error(err)
		s.Empty(metadataURI)
	})

	s.Run("nil option", func() {
		metadataURI, err := s.client.ExportSnapshot(ctx, nil)
		s.Error(err)
		s.Empty(metadataURI)
	})
}

func (s *SnapshotSuite) TestSnapshotOptions() {
	s.Run("CreateSnapshotOption", func() {
		collectionName := "test_collection"
		snapshotName := "test_snapshot"
		description := "test description"
		dbName := "test_db"

		opt := NewCreateSnapshotOption(snapshotName, collectionName).
			WithDescription(description).
			WithDbName(dbName)

		req := opt.Request()
		s.Equal(collectionName, req.GetCollectionName())
		s.Equal(snapshotName, req.GetName())
		s.Equal(description, req.GetDescription())
		s.Equal(dbName, req.GetDbName())
	})

	s.Run("DropSnapshotOption", func() {
		snapshotName := "test_snapshot"
		collectionName := "test_collection"
		dbName := "test_db"
		opt := NewDropSnapshotOption(snapshotName, collectionName).
			WithDbName(dbName)

		req := opt.Request()
		s.Equal(snapshotName, req.GetName())
		s.Equal(collectionName, req.GetCollectionName())
		s.Equal(dbName, req.GetDbName())
	})

	s.Run("ListSnapshotsOption", func() {
		dbName := "test_db"
		collectionName := "test_collection"

		opt := NewListSnapshotsOption(collectionName).
			WithDbName(dbName)

		req := opt.Request()
		s.Equal(dbName, req.GetDbName())
		s.Equal(collectionName, req.GetCollectionName())
	})

	s.Run("DescribeSnapshotOption", func() {
		snapshotName := "test_snapshot"
		collectionName := "test_collection"
		dbName := "test_db"
		opt := NewDescribeSnapshotOption(snapshotName, collectionName).
			WithDbName(dbName)

		req := opt.Request()
		s.Equal(snapshotName, req.GetName())
		s.Equal(collectionName, req.GetCollectionName())
		s.Equal(dbName, req.GetDbName())
	})

	s.Run("ListRestoreSnapshotJobsOption", func() {
		dbName := "test_db"
		collectionName := "test_collection"

		opt := NewListRestoreSnapshotJobsOption().
			WithDbName(dbName).
			WithCollectionName(collectionName)

		req := opt.Request()
		s.Equal(dbName, req.GetDbName())
		s.Equal(collectionName, req.GetCollectionName())
	})

	s.Run("RestoreSnapshotOption", func() {
		snapshotName := "test_snapshot"
		sourceCollection := "source_collection"
		sourceDb := "source_db"
		targetCollection := "restored_collection"
		targetDb := "target_db"

		opt := NewRestoreSnapshotOption(snapshotName, sourceCollection, targetCollection).
			WithDbName(sourceDb).
			WithTargetDbName(targetDb)

		req := opt.Request()
		s.Equal(snapshotName, req.GetName())
		s.Equal(sourceCollection, req.GetCollectionName())
		s.Equal(sourceDb, req.GetDbName())
		s.Equal(targetCollection, req.GetTargetCollectionName())
		s.Equal(targetDb, req.GetTargetDbName())
	})

	s.Run("RestoreExternalSnapshotOption", func() {
		dbName := "target_db"
		targetCollection := "restored_collection"
		metadataURI := "s3://bucket/files/snapshots/meta.json"
		externalSpec := `{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`

		opt := NewRestoreExternalSnapshotOption(targetCollection, metadataURI).
			WithDbName(dbName).
			WithExternalSpec(externalSpec)

		req := opt.Request()
		s.NotNil(req.GetBase())
		s.Equal(dbName, req.GetDbName())
		s.Equal(targetCollection, req.GetTargetCollectionName())
		s.Equal(metadataURI, req.GetSnapshotMetadataUri())
		s.Equal(externalSpec, req.GetExternalSpec())
	})

	s.Run("ExportSnapshotOption", func() {
		dbName := "source_db"
		collectionName := "source_collection"
		snapshotName := "test_snapshot"
		targetS3Path := "s3://bucket/export-root"
		externalSpec := `{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`

		opt := NewExportSnapshotOption(snapshotName, collectionName, targetS3Path).
			WithDbName(dbName).
			WithExternalSpec(externalSpec)

		req := opt.Request()
		s.Equal(snapshotName, req.GetName())
		s.Equal(dbName, req.GetDbName())
		s.Equal(collectionName, req.GetCollectionName())
		s.Equal(targetS3Path, req.GetTargetS3Path())
		s.Equal(externalSpec, req.GetExternalSpec())
	})
}

func TestSnapshot(t *testing.T) {
	suite.Run(t, new(SnapshotSuite))
}
