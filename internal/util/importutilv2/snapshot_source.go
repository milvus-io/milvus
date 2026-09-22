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
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
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
			source.GetSourceCommitTimestamp() != 0 || source.GetTargetPartitionId() != 0 ||
			source.GetSourceChannel() != "" || source.GetSourcePartitionId() != 0 ||
			len(source.GetLegacyL0Deltalogs())+len(source.GetManifestL0Deltalogs()) != 0 {
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
		if source != nil && (source.GetVersion() < 1 || source.GetVersion() > 8) {
			return merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported snapshot import source version %d", source.GetVersion())
		}
		if len(source.GetSnapshotMetadata()) != 0 {
			return merr.WrapErrServiceInternalMsg("expanded snapshot source still contains preparation metadata")
		}
		// Development versions persisted L0 paths in every segment descriptor.
		// Do not migrate or replay those tasks: all L0 must now be task-shared.
		// Keep the wire fields so recovery can reject them rather than silently
		// dropping deletes. Versions 1-4 remain valid without inline L0.
		if len(source.GetLegacyL0Deltalogs())+len(source.GetManifestL0Deltalogs()) != 0 {
			return merr.Wrapf(merr.ErrServiceUnimplemented, "snapshot import tasks with inline L0 are no longer supported; resubmit the snapshot import")
		}
		contractVersion := source.GetVersion()
		if SnapshotSourceUsesSharedL0(source) {
			contractVersion -= 4
			if source.GetSourceChannel() == "" || source.GetSourcePartitionId() <= 0 {
				return merr.WrapErrServiceInternalMsg("invalid task-shared snapshot source descriptor")
			}
		} else if source.GetSourceChannel() != "" || source.GetSourcePartitionId() != 0 {
			return merr.WrapErrServiceInternalMsg("legacy snapshot descriptor contains shared source scope")
		}
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

// Versions 5-8 preserve the local/external and mapped/unmapped contracts of
// versions 1-4, but require a separate L0 inventory. Older workers reject the
// version rather than silently ignoring deletes they do not know how to bind.
func SnapshotSourceUsesSharedL0(source *internalpb.SnapshotImportSource) bool {
	return source.GetVersion() >= 5 && source.GetVersion() <= 8
}

type snapshotSourceScope struct {
	channel     string
	partitionID int64
}

// ValidateSnapshotImportPlan checks the complete durable representation before
// publication and after ACK/recovery. Empty exact-scope entries are intentional:
// a missing inventory is corruption, not evidence that a segment has no L0.
func ValidateSnapshotImportPlan(files []*internalpb.ImportFile, options Options, sources []*internalpb.SnapshotImportL0Source) error {
	if err := ValidateSnapshotImportFiles(files, options); err != nil {
		return err
	}
	scopes := make(map[snapshotSourceScope]bool)
	channels := make(map[string]bool)
	size := 0
	sharedFiles := 0
	for _, file := range files {
		size += proto.Size(file)
		if source := file.GetSnapshotSource(); SnapshotSourceUsesSharedL0(source) {
			sharedFiles++
			scopes[snapshotSourceScope{source.GetSourceChannel(), source.GetSourcePartitionId()}] = false
			channels[source.GetSourceChannel()] = true
		}
	}
	seen := make(map[snapshotSourceScope]bool)
	if len(scopes) == 0 && len(sources) == 0 {
		// Historical snapshots without commit timestamps or L0 can use plain
		// file paths. They still need the job-wide limit; ordinary imports do not.
		if IsSnapshotSource(options) && size > SnapshotSourcePlanMaxBytes {
			return merr.WrapErrImportFailedMsg("snapshot source plan exceeds 256 KiB")
		}
		return nil // Ordinary imports and snapshot jobs without shared L0.
	}
	if sharedFiles != len(files) {
		return merr.WrapErrServiceInternalMsg("mixed snapshot plan source representations")
	}
	kinds := make(map[string]bool)
	for _, source := range sources {
		scope := snapshotSourceScope{source.GetSourceChannel(), source.GetSourcePartitionId()}
		_, exact := scopes[scope]
		applicable := exact || (scope.partitionID == common.AllPartitionsID && channels[scope.channel])
		if seen[scope] || !applicable {
			return merr.WrapErrServiceInternalMsg("duplicate or unreferenced snapshot L0 scope")
		}
		seen[scope] = true
		if exact {
			scopes[scope] = true
		}
		for kind, paths := range [][]string{source.GetLegacyL0Deltalogs(), source.GetManifestL0Paths()} {
			for _, path := range paths {
				if strings.TrimSpace(path) == "" {
					return merr.WrapErrServiceInternalMsg("snapshot inventory contains an empty delete path")
				}
				if kind == 1 {
					base, version, err := packed.UnmarshalManifestPath(path)
					if err != nil || base == "" || version == packed.ManifestLatest {
						return merr.WrapErrServiceInternalMsg("snapshot L0 inventory requires exact manifests")
					}
				}
				if previous, ok := kinds[path]; ok && previous != (kind == 1) {
					return merr.WrapErrServiceInternalMsg("snapshot delete path has conflicting decoder contracts")
				}
				kinds[path] = kind == 1
			}
		}
		size += proto.Size(source)
	}
	for _, present := range scopes {
		if !present {
			return merr.WrapErrServiceInternalMsg("snapshot import lost its shared L0 inventory")
		}
	}
	if size > SnapshotSourcePlanMaxBytes {
		return merr.WrapErrImportFailedMsg("snapshot source plan exceeds 256 KiB")
	}
	return nil
}

// SnapshotTaskL0Source merges partition-local and channel-wide paths once per
// task, not per segment. Tasks may be split by size/count but never mix source
// scopes. Persisted jobs retain the unmerged inventories for retries/recovery.
func SnapshotTaskL0Source(files []*internalpb.ImportFile, sources []*internalpb.SnapshotImportL0Source) (*internalpb.SnapshotImportL0Source, error) {
	if len(files) == 0 || !SnapshotSourceUsesSharedL0(files[0].GetSnapshotSource()) {
		for _, file := range files {
			if SnapshotSourceUsesSharedL0(file.GetSnapshotSource()) {
				return nil, merr.WrapErrServiceInternalMsg("mixed snapshot task source representations")
			}
		}
		return nil, nil
	}
	first := files[0].GetSnapshotSource()
	result := &internalpb.SnapshotImportL0Source{SourceChannel: first.GetSourceChannel(), SourcePartitionId: first.GetSourcePartitionId()}
	for _, file := range files {
		source := file.GetSnapshotSource()
		if !SnapshotSourceUsesSharedL0(source) || source.GetSourceChannel() != result.SourceChannel || source.GetSourcePartitionId() != result.SourcePartitionId {
			return nil, merr.WrapErrServiceInternalMsg("snapshot task mixes source scopes")
		}
	}
	found := false
	seen := make(map[string]bool)
	for _, source := range sources {
		if source.GetSourceChannel() != result.SourceChannel ||
			(source.GetSourcePartitionId() != result.SourcePartitionId && source.GetSourcePartitionId() != common.AllPartitionsID) {
			continue
		}
		found = found || source.GetSourcePartitionId() == result.SourcePartitionId
		for kind, paths := range [][]string{source.GetLegacyL0Deltalogs(), source.GetManifestL0Paths()} {
			for _, path := range paths {
				if previous, ok := seen[path]; ok {
					if previous != (kind == 1) {
						return nil, merr.WrapErrServiceInternalMsg("snapshot delete path has conflicting decoder contracts")
					}
					continue
				}
				seen[path] = kind == 1
				if kind == 1 {
					result.ManifestL0Paths = append(result.ManifestL0Paths, path)
				} else {
					result.LegacyL0Deltalogs = append(result.LegacyL0Deltalogs, path)
				}
			}
		}
	}
	if !found {
		return nil, merr.WrapErrServiceInternalMsg("snapshot task lost its shared L0 inventory")
	}
	sort.Strings(result.LegacyL0Deltalogs)
	sort.Strings(result.ManifestL0Paths)
	return result, nil
}

func ValidateSnapshotImportTask(files []*internalpb.ImportFile, options Options, source *internalpb.SnapshotImportL0Source) error {
	if IsSnapshotPreparation(files) {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "snapshot preparation must finish before worker dispatch")
	}
	var sources []*internalpb.SnapshotImportL0Source
	if source != nil {
		sources = []*internalpb.SnapshotImportL0Source{source}
	}
	// A task has exactly one exact-scope inventory. Plan validation therefore
	// also rejects any file from a different scope, without copying the lists.
	return ValidateSnapshotImportPlan(files, options, sources)
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
