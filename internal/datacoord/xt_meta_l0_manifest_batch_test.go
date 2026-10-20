// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/storagev2/packed"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// Existing operator tests supply a blocking fake. Only this test adapter uses
// a goroutine to model native callback delivery; production submits directly.
func mockL0ManifestSubmissions(commit func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig, []packed.DeltaLogEntry) (string, error)) *mockey.Mocker {
	return mockey.Mock(packed.SubmitManifestUpdates).To(func(ctx context.Context, io *packed.ManifestIOContext, base string, version int64, cfg *indexpb.StorageConfig, updates *packed.ManifestUpdates, complete func(packed.ManifestUpdateResult, error)) error {
		go func() {
			manifest, err := commit(ctx, io, packed.MarshalManifestPath(base, version), cfg, updates.DeltaLogs)
			complete(packed.ManifestUpdateResult{ManifestPath: manifest}, err)
		}()
		return nil
	}).Build()
}
