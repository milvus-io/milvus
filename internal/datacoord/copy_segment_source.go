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

package datacoord

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SnapshotSource is the snapshot metadata that copy-segment code reads and unpins.
type SnapshotSource interface {
	// ReadSnapshotData reads the snapshot data of a named snapshot of a collection from object storage.
	ReadSnapshotData(ctx context.Context, collectionID int64, snapshotName string, includeSegments bool) (*snapshotstorage.SnapshotData, error)
	// ReadExternalSnapshotDataWithChunkManager reads the snapshot data at an external location through the given chunk manager.
	ReadExternalSnapshotDataWithChunkManager(
		ctx context.Context,
		cm storage.ChunkManager,
		snapshotS3Location string,
		includeSegments bool,
	) (*snapshotstorage.SnapshotData, error)
	// UnpinSnapshot removes a pin ID from its snapshot and returns the collection ID, snapshot name and active pin count.
	UnpinSnapshot(ctx context.Context, pinID int64) (int64, string, int, error)
}

func isPermanentSnapshotError(err error) bool {
	return errors.Is(err, merr.ErrDataIntegrity) ||
		merr.GetErrorType(err) == merr.InputError ||
		merr.IsNonRetryableErr(err)
}

// setSnapshotActivePinsGauge publishes the active-pins gauge for a snapshot.
// When active falls to zero we drop the label series to keep Prometheus
// cardinality bounded — otherwise every dropped snapshot leaves behind a
// stale series until process restart.
func setSnapshotActivePinsGauge(collectionID int64, name string, active int) {
	label := []string{strconv.FormatInt(collectionID, 10), name}
	if active == 0 {
		metrics.DataCoordSnapshotActivePins.DeleteLabelValues(label...)
		return
	}
	metrics.DataCoordSnapshotActivePins.WithLabelValues(label...).Set(float64(active))
}
