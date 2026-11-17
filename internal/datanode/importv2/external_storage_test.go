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

package importv2

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
)

func (s *SchedulerSuite) TestExternalSourcePreservesDestination() {
	data := []byte(`{"rows":[{"pk":"external-row","int64":42,"vec":[0.1,0.2,0.3,0.4]}]}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Contains(r.Header.Get("Authorization"), "source-key/")
		if r.Method != http.MethodHead && r.Method != http.MethodGet {
			s.T().Errorf("unexpected write to source: %s", r.Method)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/source-bucket/", "/source-bucket":
			w.WriteHeader(http.StatusOK)
		case "/source-bucket/external/data.json":
			http.ServeContent(w, r, "data.json", time.Unix(1, 0), bytes.NewReader(data))
		default:
			s.T().Errorf("unexpected source path: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	options := []*commonpb.KeyValuePair{
		{Key: "storage.type", Value: "remote"},
		{Key: "storage.endpoint", Value: strings.TrimPrefix(server.URL, "http://")},
		{Key: "storage.bucket", Value: "source-bucket"},
		{Key: "storage.accessKeyID", Value: "source-key"},
		{Key: "storage.secretAccessKey", Value: "source-secret"},
		{Key: "storage.useSSL", Value: "false"},
		{Key: "storage.region", Value: "us-east-1"},
	}
	destination := mocks.NewChunkManager(s.T())
	config := &indexpb.StorageConfig{
		StorageType: "remote", BucketName: "destination-bucket",
		RootPath: "milvus", AccessKeyID: "destination-key", SecretAccessKey: "destination-secret",
	}
	originalConfig := proto.Clone(config)
	files := []*internalpb.ImportFile{{Paths: []string{"external/data.json"}}}
	pre := NewPreImportTask(&datapb.PreImportRequest{
		JobID: 10, TaskID: 11, CollectionID: 12, PartitionIDs: []int64{13},
		Vchannels: []string{"v0"}, Schema: s.schema, ImportFiles: files,
		Options: options, StorageConfig: config,
	}, s.manager, destination)
	s.manager.Add(pre)
	s.Require().NoError(conc.AwaitAll(pre.Execute()...))
	s.Equal(int64(1), s.manager.Get(11).(*PreImportTask).GetFileStats()[0].GetTotalRows())

	// Any destination reads would fail the mock. All syncs must use this manager.
	s.syncMgr.EXPECT().SyncDataWithChunkManager(mock.Anything, mock.Anything, destination).
		RunAndReturn(func(ctx context.Context, task syncmgr.Task, cm storage.ChunkManager, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
			return conc.Go(func() (struct{}, error) { return struct{}{}, nil }), nil
		}).Once()
	task := NewImportTask(&datapb.ImportRequest{
		JobID: 10, TaskID: 14, CollectionID: 12, PartitionIDs: []int64{13},
		Vchannels: []string{"v0"}, Schema: s.schema, Files: files, Options: options,
		StorageConfig: config, Ts: 1000, IDRange: &datapb.IDRange{Begin: 1, End: 100},
		RequestSegments: []*datapb.ImportRequestSegment{{SegmentID: 15, PartitionID: 13, Vchannel: "v0"}},
	}, s.manager, s.syncMgr, destination)
	s.manager.Add(task)
	s.Require().NoError(conc.AwaitAll(task.Execute()...))
	s.True(proto.Equal(originalConfig, config))
	s.Same(destination, task.(*ImportTask).cm)
	s.Same(destination, task.Clone().(*ImportTask).cm)
}
