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

package packed

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func TestManifestTextSizeCanceledWaitingForFooterAdmission(t *testing.T) {
	cfg := manifestTestStorageConfig(t)
	base := filepath.Join(cfg.RootPath, "text-size-admission")
	schema := arrow.NewSchema([]arrow.Field{{Name: "101", Type: arrow.BinaryTypes.String,
		Metadata: arrow.NewMetadata([]string{ArrowFieldIdMetadataKey}, []string{"101"})}}, nil)
	writer, err := NewFFISegmentWriter(schema, &SegmentWriterConfig{
		SegmentPath: base, WriterFormat: "parquet",
		TextColumns: []TextColumnConfig{{FieldID: 101, LobBasePath: filepath.Join(cfg.RootPath, "lobs/101"),
			InlineThreshold: 64, MaxLobFileBytes: 1 << 20, FlushThresholdBytes: 1 << 20}},
	}, cfg, nil)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	builder.Field(0).(*array.StringBuilder).Append(strings.Repeat("text", 64))
	record := builder.NewRecord()
	builder.Release()
	require.NoError(t, writer.Write(record))
	record.Release()
	output, err := writer.Close()
	require.NoError(t, err)
	defer output.Destroy()
	manifest, err := CommitManifestUpdates(MarshalManifestPath(base, ManifestEarliest), cfg, &ManifestUpdates{NewFiles: output})
	require.NoError(t, err)

	previous := manifestFFI
	manifestFFI = &manifestFFILimit{slots: semaphore.NewWeighted(1)}
	defer func() { manifestFFI = previous }()
	io := NewManifestIOContext(1)
	defer io.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	var acquire func(context.Context) error
	// Deterministically let another coordinator operation take the only slot
	// after the real manifest read. Both admissions still use the real limiter;
	// only their interleaving is controlled, so cancellation must release the
	// waiting estimator without consuming a slot or starting a footer read.
	patch := mockey.Mock(acquireManifestFFI).Origin(&acquire).To(func(callCtx context.Context) error {
		if callCtx == ctx {
			if err := acquire(context.Background()); err != nil {
				return err
			}
			defer releaseManifestFFI()
			close(entered)
		}
		return acquire(callCtx)
	}).Build()
	defer patch.UnPatch()
	done := make(chan error, 1)
	go func() {
		_, _, err := EstimateManifestTextInputSize(ctx, io, manifest, 101, cfg, nil)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("footer admission did not follow the manifest read")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("canceled footer admission did not finish")
	}
	patch.UnPatch()
	// The canceled estimator released the admission budget and manifest state.
	size, found, err := EstimateManifestTextInputSize(context.Background(), io, manifest, 101, cfg, nil)
	require.NoError(t, err)
	require.True(t, found)
	require.Positive(t, size)
}
