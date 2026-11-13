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

package testcases

import (
	"context"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Optional persisted-plan assertion for the BM25 E2E. Run the same fixture
// against an isolated instance with dataCoord.externalCollection.functionRowsPerTask
// set to 0 (expect 1 task) or 6000 (expect 5 tasks). Keep
// dataCoord.externalCollectionFilesPerTask=10000.
// Set MILVUS_TEST_EXTERNAL_BM25_FILES=64 and a 3000-row budget to also
// exercise 64 tasks across the metadata transaction batching boundary.
// Supply all three plan-assertion variables explicitly; ordinary SDK tests need no etcd access.
// This helper only reads the submitted job and does not alter server config.
func assertExternalRefreshTaskCount(t *testing.T, ctx context.Context, jobID, collectionID int64) {
	t.Helper()
	expected := os.Getenv("MILVUS_TEST_EXPECT_REFRESH_TASK_COUNT")
	if expected == "" {
		return
	}
	want, err := strconv.Atoi(expected)
	require.NoError(t, err)
	require.Positive(t, want)
	endpoints := os.Getenv("MILVUS_TEST_ETCD_ENDPOINTS")
	metaRoot := os.Getenv("MILVUS_TEST_ETCD_META_ROOT")
	require.NotEmpty(t, endpoints)
	require.NotEmpty(t, metaRoot)
	ec, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(endpoints, ","), DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer ec.Close()
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	key := path.Join(metaRoot, "datacoord-meta/external-collection-refresh-job", strconv.FormatInt(jobID, 10))
	result, err := ec.Get(readCtx, key)
	require.NoError(t, err)
	require.Len(t, result.Kvs, 1, "persisted refresh job %d", jobID)
	var job datapb.ExternalCollectionRefreshJob
	require.NoError(t, proto.Unmarshal(result.Kvs[0].Value, &job))
	require.Equal(t, collectionID, job.GetCollectionId())
	require.Len(t, job.GetTaskIds(), want, "persisted refresh plan for job %d", jobID)
	t.Logf("Verified persisted refresh plan: job=%d tasks=%d", jobID, len(job.GetTaskIds()))
}

func externalBM25FileCount(t *testing.T) int {
	t.Helper()
	value := os.Getenv("MILVUS_TEST_EXTERNAL_BM25_FILES")
	if value == "" {
		return 10
	}
	count, err := strconv.Atoi(value)
	require.NoError(t, err)
	require.Positive(t, count)
	return count
}
