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

package importutilv2

import (
	"context"
	"strings"

	"google.golang.org/protobuf/proto"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// SnapshotSourcePlanMaxBytes bounds the replicated, per-file source inventory.
// Leave room for schema, options, IDs and encoding overhead in the WAL and
// catalog envelopes. This is not a claim about a backend's configured limit;
// the producer checks the complete encoded message separately.
const SnapshotSourcePlanMaxBytes = 256 * 1024

// ResolveSnapshotImportStorage only replaces reader dependencies, never the
// task's target storage config or chunk manager. A resolution/access failure
// must propagate: falling back could import a same-named object in the target.
func ResolveSnapshotImportStorage(ctx context.Context, cm storage.ChunkManager,
	cfg *indexpb.StorageConfig, metadataURI string, options Options,
) (storage.ChunkManager, *indexpb.StorageConfig, error) {
	if !HasExternalSource(options) {
		return cm, cfg, nil
	}
	spec, _ := funcutil.GetAttrByKeyFromRepeatedKV(ExternalSpec, options)
	resolved, err := snapshotstorage.ResolveSnapshotReadStorage(ctx,
		snapshotstorage.InstanceConfigFromParamtable(paramtable.Get()), metadataURI, spec)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to resolve snapshot import storage")
	}
	return resolved.ForeignCM, resolved.ForeignStorageConfig, nil
}

// ValidateSnapshotImportFiles runs on expanded/persisted input, not on the
// public metadata URI. Losing the descriptor must fail even on an old ACK path
// that preserves only the deliberately empty legacy paths.
func ValidateSnapshotImportFiles(files []*internalpb.ImportFile, options Options) error {
	typed := false
	for _, file := range files {
		typed = typed || file.GetSnapshotSource() != nil
	}
	if !typed && !IsSnapshotSource(options) {
		return nil
	}
	if err := ValidateSnapshotSourceOptions(options); err != nil {
		return err
	}
	if !IsSnapshotSource(options) || len(files) == 0 {
		return merr.WrapErrServiceInternalMsg("snapshot descriptors require a nonempty snapshot backup job")
	}
	size := 0
	for _, file := range files {
		source := file.GetSnapshotSource()
		if HasExternalSource(options) {
			uri, err := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options)
			if err != nil || strings.TrimSpace(uri) == "" || source.GetVersion() != 2 {
				return merr.WrapErrServiceInternalMsg("external snapshot import lost its source storage descriptor")
			}
		} else if source.GetVersion() == 2 {
			return merr.WrapErrServiceInternalMsg("external snapshot descriptor lost its credentials")
		}
		if !typed {
			if len(file.GetPaths()) != 1 || strings.TrimSpace(file.GetPaths()[0]) == "" {
				return merr.WrapErrServiceInternalMsg("snapshot import file lost its source descriptor or manifest")
			}
			continue
		}
		if source == nil || len(file.GetPaths()) != 0 {
			return merr.WrapErrServiceInternalMsg("mixed snapshot import source representations")
		}
		if source.GetVersion() != 1 && source.GetVersion() != 2 {
			return merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported snapshot import source version %d", source.GetVersion())
		}
		base, version, err := packed.UnmarshalManifestPath(source.GetManifestPath())
		if err != nil || base == "" || version == packed.ManifestLatest {
			return merr.WrapErrServiceInternalMsg("snapshot descriptor requires an exact data manifest")
		}
		seen := make(map[string]bool)
		for kind, paths := range [][]string{source.GetLegacyL0Deltalogs(), source.GetManifestL0Deltalogs()} {
			for _, path := range paths {
				if strings.TrimSpace(path) == "" {
					return merr.WrapErrServiceInternalMsg("snapshot descriptor contains an empty delete path")
				}
				if previous, ok := seen[path]; ok && previous != (kind == 1) {
					return merr.WrapErrServiceInternalMsg("snapshot delete path has conflicting decoder contracts")
				}
				seen[path] = kind == 1
			}
		}
		size += proto.Size(file)
		if size > SnapshotSourcePlanMaxBytes {
			return merr.WrapErrImportFailedMsg("snapshot source plan exceeds 256 KiB")
		}
	}
	return nil
}
