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

package packed

import (
	"context"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func packedProfileContext(ctx context.Context, storageConfig *indexpb.StorageConfig) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return storageprofile.WithBackendKind(ctx, packedBackendKind(storageConfig))
}

func arrowRecordBytes(record arrow.Record) uint64 {
	if record == nil {
		return 0
	}
	var size uint64
	for _, column := range record.Columns() {
		if column != nil && column.Data() != nil {
			size += uint64(max(column.Data().SizeInBytes(), 0))
		}
	}
	return size
}

func packedBackendKind(storageConfig *indexpb.StorageConfig) storageprofile.BackendKind {
	if storageConfig == nil {
		return storageprofile.BackendKindUnknown
	}
	if strings.EqualFold(storageConfig.GetStorageType(), "local") {
		return storageprofile.BackendKindLocal
	}
	switch strings.ToLower(storageConfig.GetCloudProvider()) {
	case "azure":
		return storageprofile.BackendKindAzure
	case "gcp", "gcpnative":
		return storageprofile.BackendKindGCP
	default:
		return storageprofile.BackendKindS3Compatible
	}
}

func firstProfileContext(contexts []context.Context) context.Context {
	if len(contexts) > 0 && contexts[0] != nil {
		return contexts[0]
	}
	return context.Background()
}

func (pr *PackedReader) WithStorageProfileContext(ctx context.Context) *PackedReader {
	if pr != nil {
		pr.profileCtx = ctx
	}
	return pr
}

func (r *FFIPackedReader) WithStorageProfileContext(ctx context.Context) *FFIPackedReader {
	if r != nil {
		r.profileCtx = ctx
	}
	return r
}

func (pw *PackedWriter) WithStorageProfileContext(ctx context.Context) *PackedWriter {
	if pw != nil {
		pw.profileCtx = ctx
	}
	return pw
}

func (pw *FFIPackedWriter) WithStorageProfileContext(ctx context.Context) *FFIPackedWriter {
	if pw != nil {
		pw.profileCtx = ctx
	}
	return pw
}

func beginPackedOperation(ctx context.Context, operation storageprofile.StorageOperation, phase storageprofile.WorkloadPhase, bytes uint64, bytesKnown bool) storageprofile.OperationRecorder {
	return storageprofile.BeginOperation(ctx, storageprofile.OperationMeta{
		AccessLayer:         storageprofile.AccessLayerMilvus,
		Operation:           operation,
		Phase:               phase,
		CppBoundary:         true,
		BytesRequested:      bytes,
		RequestedBytesKnown: bytesKnown,
	})
}
