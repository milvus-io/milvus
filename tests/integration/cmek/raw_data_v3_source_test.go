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

package cmek

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
	"github.com/milvus-io/milvus/tests/integration/cmek/testobserver"
)

func (s *RawDataV3Suite) assertCanonicalParquetFlush(ctx context.Context, collectionID int64, channels []string) {
	owners := make(map[int64]int)
	for _, node := range s.Cluster.GetAllStreamingNodes() {
		owners[node.GetNodeID()] = node.GetPID()
	}
	for _, node := range s.Cluster.GetAllDataNodes() {
		owners[node.GetNodeID()] = node.GetPID()
	}
	wantedChannels := make(map[string]bool, len(channels))
	for _, channel := range channels {
		wantedChannels[channel] = true
	}
	s.Require().NotEmpty(wantedChannels)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		records, err := testobserver.ReadRecords(s.observerDir, s.observerToken, owners)
		s.Require().NoError(err)
		flushes, complete, err := testobserver.CanonicalFlushes(records, collectionID, wantedChannels, rawDataRows)
		s.Require().NoError(err)
		if complete {
			for _, flush := range flushes {
				locator, err := inspector.ParseManifestLocatorV3(flush.Manifest)
				s.Require().NoError(err)
				manifestPath := locator.ObjectPath()
				manifest, err := s.Cluster.ChunkManager.Read(ctx, manifestPath)
				s.Require().NoError(err)
				s.inspectParquetManifest(ctx, locator.BasePath, manifest, collectionID, flush.SegmentID)
				s.T().Logf("stage=canonical-flush node=%d pid=%d task=%d segment=%d rows=%d manifest=%s commit=acknowledged", flush.NodeID, flush.PID, flush.TaskID, flush.SegmentID, flush.Rows, flush.Manifest)
			}
			return
		}
		select {
		case <-ctx.Done():
			s.T().Fatalf("canonical flush evidence incomplete: %v; records=%+v", ctx.Err(), records)
		case <-ticker.C:
		}
	}
}
