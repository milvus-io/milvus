// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importv3

import (
	"bytes"
	"context"
	"path"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// TestPublishReshardManifestWritesResultPath pins the manifest-last contract at
// the publish boundary: the run's result path is <root>/import_v3/<job>/reshard/
// <task>/manifests/<run>.pb and the bytes written are exactly the marshaled
// manifest, so the coordinator can unmarshal the descriptor list back.
func TestPublishReshardManifestWritesResultPath(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()

	manifest := &importv3pb.ReshardManifest{Fragments: []*importv3pb.FragmentDescriptor{
		{VchannelIndex: 0, PartitionId: 10, Seq: 0, Path: "fragments/0/10/3_0.parquet", Rows: 5},
		{VchannelIndex: 0, PartitionId: 10, Seq: 1, Path: "fragments/0/10/3_1.parquet", Rows: 4},
	}}
	req := datapbReshardRequest(7, 8, 9)

	var gotPath string
	var gotPayload []byte
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, filePath string, content []byte) error {
			gotPath = filePath
			gotPayload = append([]byte(nil), content...)
			return nil
		})

	require.NoError(t, publishReshardManifest(context.Background(), cm, req, manifest))

	wantPath := path.Join("root", metautil.BuildImportReshardResultPath(7, 8, 9))
	require.Equal(t, wantPath, gotPath)

	wantPayload, err := proto.Marshal(manifest)
	require.NoError(t, err)
	require.True(t, bytes.Equal(wantPayload, gotPayload), "the manifest bytes must round-trip")
	roundTrip := &importv3pb.ReshardManifest{}
	require.NoError(t, proto.Unmarshal(gotPayload, roundTrip))
	require.True(t, proto.Equal(manifest, roundTrip))
}

// TestPublishReshardManifestWriteFailurePropagates pins that a failed object
// write surfaces as a wrapped error, so the run reports the manifest as
// unpublished instead of a false success.
func TestPublishReshardManifestWriteFailurePropagates(t *testing.T) {
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("disk full"))

	err := publishReshardManifest(context.Background(), cm,
		datapbReshardRequest(7, 8, 9), &importv3pb.ReshardManifest{})
	require.Error(t, err)
	require.ErrorContains(t, err, "disk full")
}

// TestPublishReshardManifestMarshalFailureWritesNothing pins the manifest-last
// invariant: the manifest is fully marshaled before the object write, so a
// marshal failure leaves no partial manifest behind.
func TestPublishReshardManifestMarshalFailureWritesNothing(t *testing.T) {
	marshalMock := mockey.Mock(proto.Marshal).To(
		func(proto.Message) ([]byte, error) {
			return nil, errors.New("marshal exploded")
		}).Build()
	defer marshalMock.UnPatch()

	cm := mocks.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("root").Maybe()
	writeCalled := false
	cm.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string, []byte) error {
			writeCalled = true
			return nil
		}).Maybe()

	err := publishReshardManifest(context.Background(), cm,
		datapbReshardRequest(7, 8, 9), &importv3pb.ReshardManifest{})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrSerializationFailed)
	require.False(t, writeCalled, "nothing must be written until the manifest is fully marshaled")
}

func datapbReshardRequest(jobID, taskID, runID int64) *datapb.ReshardTaskRequest {
	return &datapb.ReshardTaskRequest{
		JobId: jobID, TaskId: taskID, RunId: runID,
		StorageConfig: &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"},
	}
}
