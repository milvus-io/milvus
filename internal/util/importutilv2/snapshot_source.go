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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// SnapshotSourcePlanMaxBytes bounds the replicated source inventory.
// Leave room for schema, options, IDs and encoding overhead in the WAL and
// catalog envelopes. This is not a claim about a backend's configured limit;
// the producer checks the complete encoded message separately.
const SnapshotSourcePlanMaxBytes = 256 * 1024

// SnapshotPreparationVersion is a Pending-only descriptor. It cannot be sent
// to workers; old consumers reject the version rather than losing semantics.
const SnapshotPreparationVersion = 9

func IsSnapshotPreparation(files []*internalpb.ImportFile) bool {
	return len(files) == 1 && files[0].GetSnapshotSource().GetVersion() == SnapshotPreparationVersion
}

// SnapshotPathValidator is pure: opening the exact manifest is sufficient to
// establish existence. Validate discovered object references before a reader
// can dereference them, using the source boundary captured at job creation.
func SnapshotPathValidator(options Options, cm storage.ChunkManager) (func(string) error, error) {
	layout, err := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotLayout, options)
	if err != nil {
		return nil, nil // Already-expanded jobs from before deferred validation.
	}
	if err := ValidateSnapshotSourceOptions(options); err != nil {
		return nil, err
	}
	uri, _ := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options)
	snapshot := &snapshotstorage.SnapshotData{Layout: datapb.SnapshotLayout_SnapshotLayoutReferenced}
	if layout == "self-contained" {
		snapshot.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
	}
	return func(path string) error {
		if err := snapshotstorage.ValidateSnapshotObjectPathForBucket(cm, "snapshot reference", path, ""); err != nil {
			return err
		}
		return snapshotstorage.ValidateExternalSnapshotPaths(uri, snapshot, []snapshotstorage.SnapshotFileRef{{
			Path: path, NormalizedPath: snapshotstorage.NormalizeSnapshotObjectPath(path),
		}})
	}, nil
}

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
	if IsSnapshotPreparation(files) {
		source := files[0].GetSnapshotSource()
		uri, _ := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options)
		layout, _ := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotLayout, options)
		if len(source.GetSnapshotMetadata()) == 0 || uri == "" || layout == "" ||
			len(files[0].GetPaths()) != 0 || source.GetManifestPath() != "" ||
			source.GetTargetPartitionId() != 0 {
			return merr.WrapErrServiceInternalMsg("invalid snapshot preparation descriptor")
		}
		if proto.Size(files[0]) > SnapshotSourcePlanMaxBytes {
			return merr.WrapErrImportFailedMsg("snapshot preparation input exceeds 256 KiB")
		}
		return nil
	}
	size := 0
	// The option's JSON contract was validated above; only presence matters
	// when checking the coordinator-produced descriptor.
	_, mappingErr := funcutil.GetAttrByKeyFromRepeatedKV(PartitionMapping, options)
	for _, file := range files {
		source := file.GetSnapshotSource()
		if source != nil && (source.GetVersion() < 1 || source.GetVersion() > 4) {
			return merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported snapshot import source version %d", source.GetVersion())
		}
		if len(source.GetSnapshotMetadata()) != 0 {
			return merr.WrapErrServiceInternalMsg("expanded snapshot source still contains preparation metadata")
		}
		contractVersion := source.GetVersion()
		external := contractVersion == 2 || contractVersion == 4
		if HasExternalSource(options) {
			uri, err := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options)
			if err != nil || strings.TrimSpace(uri) == "" || !external {
				return merr.WrapErrServiceInternalMsg("external snapshot import lost its source storage descriptor")
			}
		} else if external {
			return merr.WrapErrServiceInternalMsg("external snapshot descriptor lost its credentials")
		}
		mapped := contractVersion == 3 || contractVersion == 4
		if (mappingErr == nil) != mapped || (mapped && source.GetTargetPartitionId() <= 0) || (!mapped && source.GetTargetPartitionId() != 0) {
			return merr.WrapErrServiceInternalMsg("snapshot import lost its partition mapping descriptor")
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
		base, version, err := packed.UnmarshalManifestPath(source.GetManifestPath())
		if err != nil || base == "" || version == packed.ManifestLatest {
			return merr.WrapErrServiceInternalMsg("snapshot descriptor requires an exact data manifest")
		}
		size += proto.Size(file)
		if size > SnapshotSourcePlanMaxBytes {
			return merr.WrapErrImportFailedMsg("snapshot source plan exceeds 256 KiB")
		}
	}
	return nil
}

// ValidateSnapshotImportPlan bounds the complete durable file inventory.
func ValidateSnapshotImportPlan(files []*internalpb.ImportFile, options Options) error {
	if err := ValidateSnapshotImportFiles(files, options); err != nil {
		return err
	}
	if !IsSnapshotSource(options) {
		return nil
	}
	size := 0
	for _, file := range files {
		size += proto.Size(file)
	}
	if size > SnapshotSourcePlanMaxBytes {
		return merr.WrapErrImportFailedMsg("snapshot source plan exceeds 256 KiB")
	}
	return nil
}

func ValidateSnapshotImportTask(files []*internalpb.ImportFile, options Options) error {
	if IsSnapshotPreparation(files) {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "snapshot preparation must finish before worker dispatch")
	}
	return ValidateSnapshotImportPlan(files, options)
}

// ValidateSnapshotTaskPartitions guards the coordinator-to-worker routing
// contract. A reader alone cannot detect a task accidentally using all job
// partitions, which would silently violate the explicit file mapping.
func ValidateSnapshotTaskPartitions(files []*internalpb.ImportFile, partitions []int64) error {
	for _, file := range files {
		if id := file.GetSnapshotSource().GetTargetPartitionId(); id != 0 {
			if len(partitions) != 1 || partitions[0] != id {
				return merr.WrapErrServiceInternalMsg("snapshot file target partition does not match its task")
			}
		}
	}
	return nil
}
