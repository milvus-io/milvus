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

package snapshotio

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/snapshotio/format"
	native "github.com/milvus-io/milvus/pkg/v3/snapshotio/internal/native"
	"github.com/milvus-io/milvus/pkg/v3/storagev2"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReadNativeManifestFiles reads the fixed revision recorded by a snapshot.
// It is shared by export and Restore inventory; callers need no native handle.
var ReadNativeManifestFiles = native.ReadManifestFiles

// DecodeManifest decodes one native manifest without performing storage I/O.
// basePath is the original segment base key, not the manifest object's directory.
// The decoder must resolve native relative paths (including partition LOBs) into
// complete keys and return Go-owned values. It must support the same historical
// formats as milvus-storage. ManifestFile is set by the reader, not the decoder.
// Decoding uses the backup/restore serde API; live reads and writes keep transactions.
func DecodeManifest(data []byte, basePath string) (*format.NativeManifestFiles, error) {
	return native.DecodeManifest(data, basePath)
}

// ManifestReader resolves one pinned native manifest into Go-owned references.
type ManifestReader func(context.Context, string) (*format.NativeManifestFiles, error)

// nativeManifestReader adapts the server transaction reader to shared inventory.
func nativeManifestReader(config *indexpb.StorageConfig) ManifestReader {
	return func(_ context.Context, descriptor string) (*format.NativeManifestFiles, error) {
		return ReadNativeManifestFiles(descriptor, config)
	}
}

func readManifestBytes(ctx context.Context, cm ChunkManager, bucket, descriptor string) (*format.NativeManifestFiles, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base, version, err := native.UnmarshalManifestPath(descriptor)
	if err != nil {
		return nil, err
	}
	if base == "" || version < 0 {
		return nil, merr.WrapErrDataIntegrityMsg("manifest inventory requires a base and pinned revision")
	}
	if err := ValidateSnapshotObjectPathForBucket(cm, "native manifest base", base, bucket); err != nil {
		return nil, err
	}
	if version == 0 {
		return &format.NativeManifestFiles{}, nil
	}
	base = NormalizeSnapshotObjectPath(base)
	key := storagev2.ManifestObjectPath(base, version)
	data, err := cm.Read(ctx, key)
	if err != nil {
		return nil, merr.Wrap(err, "read pinned native manifest")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	files, err := DecodeManifest(data, base)
	if err != nil {
		return nil, merr.Wrap(err, "decode pinned native manifest")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if files == nil {
		return nil, merr.WrapErrServiceInternalMsg("manifest decoder returned nil inventory")
	}
	files.ManifestFile = key
	return files, nil
}
