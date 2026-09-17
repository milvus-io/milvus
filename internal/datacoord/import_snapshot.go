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
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/snapshotio/storage"
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// expandSnapshotImportFiles converts the user-provided snapshot metadata path
// into one ImportFile per selected source segment. Expansion happens before the
// Import message is broadcast so CDC peers, PreImport, retries, and Import all
// observe the same immutable manifest versions rather than resolving latest
// manifests independently. The returned options are the source-normalized
// options to broadcast and persist; caller-owned options are never modified.
func expandSnapshotImportFiles(
	ctx context.Context,
	targetPartitionIDs []int64,
	cm milvusstorage.ChunkManager,
	targetSchema *schemapb.CollectionSchema,
	files []*internalpb.ImportFile,
	options importutilv2.Options,
) ([]*internalpb.ImportFile, importutilv2.Options, error) {
	if err := importutilv2.ValidateSnapshotSourceOptions(options); err != nil {
		return nil, nil, err
	}
	if !importutilv2.IsSnapshotSource(options) {
		return files, options, nil
	}
	if cm == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("chunk manager cannot be nil")
	}
	if len(files) != 1 || len(files[0].GetPaths()) != 1 || strings.TrimSpace(files[0].GetPaths()[0]) == "" {
		return nil, nil, merr.WrapErrImportFailedMsg(
			"snapshot-source import requires exactly one snapshot metadata path",
		)
	}

	metadataPath := strings.TrimSpace(files[0].GetPaths()[0])
	// Public snapshot input must carry a storage identity before references are
	// normalized to object keys. Internal manifest paths and resumed jobs do
	// not pass through expansion and retain their existing representation.
	bucket, _, _, err := storage.ParseForeignURI(metadataPath)
	if err != nil {
		return nil, nil, err
	}
	if bucket == "" {
		return nil, nil, merr.WrapErrParameterInvalidMsg("snapshot-source import requires a complete metadata URI; bare object keys are not supported")
	}
	if !importutilv2.HasExternalSource(options) {
		if err := storage.ValidateInstanceSnapshotImportURI(storage.InstanceConfigFromParamtable(paramtable.Get()), metadataPath); err != nil {
			return nil, nil, err
		}
	}
	cm, sourceConfig, err := importutilv2.ResolveSnapshotImportStorage(ctx, cm, compaction.CreateStorageConfig(), metadataPath, options)
	if err != nil {
		return nil, nil, err
	}
	if err := storage.ValidateSnapshotObjectPathForBucket(cm, "snapshot_source", metadataPath, ""); err != nil {
		return nil, nil, err
	}
	snapshot, err := storage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to read snapshot import source")
	}
	if snapshot == nil {
		return nil, nil, merr.WrapErrImportSysFailed("snapshot reader returned no data")
	}
	if err := storage.ValidateSnapshotMetadataLocation(metadataPath, snapshot.SnapshotInfo); err != nil {
		return nil, nil, err
	}
	if snapshot.Layout != datapb.SnapshotLayout_SnapshotLayoutReferenced &&
		snapshot.Layout != datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		return nil, nil, merr.WrapErrImportFailedMsg("unsupported snapshot layout: %s", snapshot.Layout.String())
	}
	sourceSchema := snapshot.Collection.GetSchema()
	if sourceSchema == nil {
		return nil, nil, merr.WrapErrImportFailedMsg("snapshot source schema is missing")
	}
	if typeutil.IsExternalCollection(sourceSchema) ||
		sourceSchema.GetExternalSource() != "" ||
		sourceSchema.GetExternalSpec() != "" {
		return nil, nil, merr.WrapErrOperationNotSupportedMsg(
			"external collection snapshots cannot be used as import sources",
		)
	}
	options, err = normalizeSnapshotImportEncryption(sourceSchema, options)
	if err != nil {
		return nil, nil, err
	}
	if err := validateSnapshotImportSchema(targetSchema, sourceSchema); err != nil {
		return nil, nil, err
	}
	partitionMapping, err := resolveSnapshotPartitionMapping(snapshot.Collection.GetPartitions(), targetPartitionIDs, targetSchema, options)
	if err != nil {
		return nil, nil, err
	}

	// The snapshot defines the source scope. Import all data partitions while
	// retaining their original IDs/channels for L0 matching; target placement
	// is independent and follows ordinary Import target partition routing.
	segments := make([]*datapb.SegmentDescription, 0)
	for _, segment := range snapshot.Segments {
		if segment == nil {
			return nil, nil, merr.WrapErrImportFailed("snapshot contains a nil segment")
		}
		if segment.GetSegmentLevel() != datapb.SegmentLevel_L0 {
			if partitionMapping != nil && partitionMapping[segment.GetPartitionId()] == 0 {
				return nil, nil, merr.WrapErrImportFailedMsg("snapshot segment %d belongs to an unknown source partition %d", segment.GetSegmentId(), segment.GetPartitionId())
			}
			segments = append(segments, segment)
		}
	}
	if len(segments) == 0 {
		return nil, nil, merr.WrapErrImportFailedMsg("snapshot contains no data segments")
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetSegmentId() < segments[j].GetSegmentId()
	})
	if len(segments) > paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt() {
		return nil, nil, merr.WrapErrImportFailedMsg("The max number of import files should not exceed %d, but got %d",
			paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt(), len(segments))
	}

	seenManifests := make(map[string]struct{}, len(segments))
	result := make([]*internalpb.ImportFile, 0, len(segments))
	for _, segment := range segments {
		if segment.GetStorageVersion() != milvusstorage.StorageV3 {
			return nil, nil, merr.WrapErrOperationNotSupportedMsg(
				"snapshot-source import only supports StorageV3 segments, segment %d uses storage version %d",
				segment.GetSegmentId(), segment.GetStorageVersion(),
			)
		}
		manifestPath := segment.GetManifestPath()
		if manifestPath == "" {
			return nil, nil, merr.WrapErrImportFailedMsg(
				"StorageV3 segment %d has no manifest path", segment.GetSegmentId(),
			)
		}
		_, version, err := packed.UnmarshalManifestPath(manifestPath)
		if err != nil {
			return nil, nil, merr.WrapErrImportFailedMsg(
				"invalid manifest path for StorageV3 segment %d: %s", segment.GetSegmentId(), err,
			)
		}
		if version == packed.ManifestLatest {
			return nil, nil, merr.WrapErrImportFailedMsg(
				"snapshot segment %d must reference an exact manifest version", segment.GetSegmentId(),
			)
		}
		if _, ok := seenManifests[manifestPath]; ok {
			return nil, nil, merr.WrapErrImportFailedMsg(
				"snapshot source contains duplicate manifest for segment %d", segment.GetSegmentId(),
			)
		}
		seenManifests[manifestPath] = struct{}{}
		result = append(result, &internalpb.ImportFile{Paths: []string{manifestPath}})
	}
	applicable, err := snapshotImportL0Segments(snapshot.Segments, segments)
	if err != nil {
		return nil, nil, err
	}
	if len(applicable) != 0 && !snapshot.SnapshotInfo.GetSegmentCommitTimestampsPreserved() {
		return nil, nil, merr.WrapErrOperationNotSupportedMsg("snapshot with applicable L0 lacks source commit timestamps; create a new snapshot")
	}
	validationSegments := append([]*datapb.SegmentDescription(nil), segments...)
	validationSegments = append(validationSegments, applicable...)
	// File validation can open StorageV3 manifests to enumerate delta and LOB
	// references. Keep it after source-encryption validation and selected-segment
	// checks so an invalid EZK or unsupported CMEK+TEXT/LOB source fails before
	// any manifest read.
	if err := storage.ValidateExternalSnapshotDataFiles(
		ctx,
		cm,
		metadataPath,
		snapshotImportValidationData(snapshot, validationSegments),
		sourceConfig,
	); err != nil {
		return nil, nil, merr.Wrap(err, "invalid snapshot import source files")
	}
	needsSourceContext := len(applicable) != 0
	for _, segment := range segments {
		// Compaction can move L0 deletes into a data segment's manifest. The
		// commit-time override still applies after the standalone L0 disappears;
		// never choose delete semantics based on where the deletes are stored.
		needsSourceContext = needsSourceContext || segment.GetCommitTimestamp() != 0
		if !snapshot.SnapshotInfo.GetSegmentCommitTimestampsPreserved() {
			// Old producers omitted commit timestamps, so zero cannot establish
			// that raw row timestamps are safe for segment-local deletes either.
			// Inspect exact manifests only after validating their source paths.
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			paths, err := packed.GetDeltaLogPathsFromManifest(segment.GetManifestPath(), sourceConfig)
			if err != nil {
				return nil, nil, merr.Wrap(err, "failed to inspect snapshot segment deletes")
			}
			if len(paths) != 0 {
				return nil, nil, merr.WrapErrOperationNotSupportedMsg("snapshot with segment-local deletes lacks source commit timestamps; create a new snapshot")
			}
		}
	}
	if needsSourceContext {
		if err := attachSnapshotImportSources(ctx, cm, metadataPath, snapshot, segments, applicable, result, sourceConfig); err != nil {
			return nil, nil, err
		}
	}
	if importutilv2.HasExternalSource(options) {
		for _, file := range result {
			if file.SnapshotSource == nil {
				file.SnapshotSource = &internalpb.SnapshotImportSource{ManifestPath: file.GetPaths()[0]}
			}
			// Older workers must reject this descriptor instead of ignoring
			// external_spec and reading the same object key in the target bucket.
			file.SnapshotSource.Version = 2
			file.Paths = nil
		}
	}
	if partitionMapping != nil {
		for i, file := range result {
			if file.SnapshotSource == nil {
				file.SnapshotSource = &internalpb.SnapshotImportSource{ManifestPath: file.GetPaths()[0]}
			}
			// Versions 3/4 add explicit target routing to local/external sources.
			// Keep source partition/channel identities intact until L0 is matched.
			file.SnapshotSource.Version = 3
			if importutilv2.HasExternalSource(options) {
				file.SnapshotSource.Version = 4
			}
			file.SnapshotSource.TargetPartitionId = partitionMapping[segments[i].GetPartitionId()]
			file.Paths = nil
		}
	}
	return result, options, nil
}

func resolveSnapshotPartitionMapping(sourcePartitions map[string]int64, targetIDs []int64,
	schema *schemapb.CollectionSchema, options importutilv2.Options,
) (map[int64]int64, error) {
	mapping, err := importutilv2.GetPartitionMapping(options)
	if err != nil || mapping == nil {
		return nil, err
	}
	if typeutil.HasPartitionKey(schema) {
		return nil, merr.WrapErrImportFailedMsg("partition_mapping does not support a partition-key target collection")
	}
	if len(mapping) != len(sourcePartitions) {
		return nil, merr.WrapErrImportFailedMsg("partition_mapping must cover every source snapshot partition")
	}
	names := importutilv2.PartitionMappingTargets(mapping)
	if len(names) != len(targetIDs) {
		return nil, merr.WrapErrServiceInternalMsg("partition_mapping target IDs do not match Proxy's resolved destinations")
	}
	targets := make(map[string]int64, len(names))
	for i, name := range names {
		if targetIDs[i] <= 0 {
			return nil, merr.WrapErrServiceInternalMsg("partition_mapping contains an invalid resolved target ID")
		}
		targets[name] = targetIDs[i]
	}
	result := make(map[int64]int64, len(mapping))
	for source, target := range mapping {
		id, ok := sourcePartitions[source]
		if !ok {
			return nil, merr.WrapErrImportFailedMsg("partition_mapping source partition %s does not exist in the snapshot", source)
		}
		if id <= 0 || result[id] != 0 {
			return nil, merr.WrapErrImportFailedMsg("snapshot contains invalid or duplicate source partition IDs")
		}
		result[id] = targets[target]
	}
	return result, nil
}

// Recheck Proxy's name-to-ID resolution while holding the collection broadcast
// lock. A drop/recreate during snapshot I/O must not redirect already-bound
// files to another partition with the same name.
func (s *Server) validateSnapshotPartitionTargets(ctx context.Context, collectionID int64,
	partitionIDs []int64, options importutilv2.Options,
) error {
	mapping, err := importutilv2.GetPartitionMapping(options)
	if err != nil || mapping == nil {
		return err
	}
	partitions, err := s.broker.ShowPartitions(ctx, collectionID)
	if err != nil {
		return err
	}
	if len(partitions.GetPartitionNames()) != len(partitions.GetPartitionIDs()) {
		return merr.WrapErrServiceInternalMsg("partition metadata has mismatched names and IDs")
	}
	current := make(map[string]int64)
	for i, name := range partitions.GetPartitionNames() {
		current[name] = partitions.GetPartitionIDs()[i]
	}
	names := importutilv2.PartitionMappingTargets(mapping)
	if len(names) != len(partitionIDs) {
		return merr.WrapErrServiceInternalMsg("partition_mapping destination count changed")
	}
	for i, name := range names {
		if current[name] != partitionIDs[i] {
			return merr.WrapErrServiceUnavailableMsg("target partition %s changed while preparing snapshot import; retry the request", name)
		}
	}
	return nil
}

// Match L0 against original source partitions/channels, including collection-wide
// deletes. Empty markers still activate the job.
func snapshotImportL0Segments(all, data []*datapb.SegmentDescription) ([]*datapb.SegmentDescription, error) {
	var result []*datapb.SegmentDescription
	for _, delta := range all {
		if delta.GetSegmentLevel() != datapb.SegmentLevel_L0 {
			continue
		}
		for _, segment := range data {
			if delta.GetPartitionId() != common.AllPartitionsID && delta.GetPartitionId() != segment.GetPartitionId() {
				continue
			}
			if delta.GetChannelName() == "" || segment.GetChannelName() == "" {
				return nil, merr.WrapErrImportFailedMsg("snapshot L0 matching requires source channel identity")
			}
			if delta.GetChannelName() != segment.GetChannelName() {
				continue
			}
			switch delta.GetStorageVersion() {
			case milvusstorage.StorageV1, milvusstorage.StorageV2:
			case milvusstorage.StorageV3:
				base, version, err := packed.UnmarshalManifestPath(delta.GetManifestPath())
				if err != nil || base == "" || version == packed.ManifestLatest {
					return nil, merr.WrapErrImportFailedMsg("snapshot L0 requires an exact manifest")
				}
			default:
				return nil, merr.WrapErrOperationNotSupportedMsg("unsupported snapshot L0 storage version %d", delta.GetStorageVersion())
			}
			result = append(result, delta)
			break
		}
	}
	return result, nil
}

// Attach every data segment's timestamp context together with its optional L0
// inputs. Uniform descriptors preserve the same semantics through WAL and both
// Import phases, including jobs containing only segment-local deletes.
func attachSnapshotImportSources(ctx context.Context, cm milvusstorage.ChunkManager, metadataPath string,
	snapshot *storage.SnapshotData, data, deltas []*datapb.SegmentDescription, files []*internalpb.ImportFile,
	sourceConfig *indexpb.StorageConfig,
) error {
	planSize := 0
	seen := make([]map[string]bool, len(files))
	for i, segment := range data {
		files[i].SnapshotSource = &internalpb.SnapshotImportSource{
			Version: 1, ManifestPath: segment.GetManifestPath(), SourceCommitTimestamp: segment.GetCommitTimestamp(),
		}
		files[i].Paths = nil // Old workers must not ignore source timestamp/delete semantics.
		planSize += proto.Size(files[i]) + 16
		if planSize > importutilv2.SnapshotSourcePlanMaxBytes {
			return merr.WrapErrImportFailedMsg("snapshot source plan exceeds 256 KiB")
		}
		seen[i] = make(map[string]bool)
	}
	kinds := make(map[string]bool)
	for _, delta := range deltas {
		if err := ctx.Err(); err != nil {
			return err
		}
		packedDelta := delta.GetStorageVersion() == milvusstorage.StorageV3
		var paths []string
		if packedDelta {
			var err error
			paths, err = packed.GetDeltaLogPathsFromManifest(delta.GetManifestPath(), sourceConfig)
			if err != nil {
				return merr.Wrap(err, "failed to resolve snapshot L0 manifest")
			}
		} else {
			for _, field := range delta.GetDeltalogs() {
				for _, log := range field.GetBinlogs() {
					// A legacy zero count does not imply an empty object.
					if strings.TrimSpace(log.GetLogPath()) == "" {
						return merr.WrapErrDataIntegrityMsg("snapshot L0 deltalog has no object path")
					}
					paths = append(paths, log.GetLogPath())
				}
			}
		}
		for _, path := range paths {
			if err := storage.ValidateSnapshotObjectPathForBucket(cm, "snapshot L0", path, ""); err != nil {
				return err
			}
			if err := storage.ValidateExternalSnapshotPaths(metadataPath, snapshot, []storage.SnapshotFileRef{{
				Path: path, NormalizedPath: storage.NormalizeSnapshotObjectPath(path), Type: storage.SnapshotFileTypeDeltaBinlog,
			}}); err != nil {
				return err
			}
			// Validate the original URI before dropping its storage identity.
			// ChunkManager reads object keys, not URIs; persist that same key
			// for both phases and use it for deduplication/decoder conflicts so
			// a URI and a bare key cannot describe the same object differently.
			path = storage.NormalizeSnapshotObjectPath(path)
			if previous, ok := kinds[path]; ok && previous != packedDelta {
				return merr.WrapErrDataIntegrityMsg("snapshot delete object has conflicting decoder contracts")
			}
			kinds[path] = packedDelta
			for i, segment := range data {
				if segment.GetChannelName() != delta.GetChannelName() ||
					(delta.GetPartitionId() != common.AllPartitionsID && segment.GetPartitionId() != delta.GetPartitionId()) {
					continue
				}
				if _, ok := seen[i][path]; ok {
					continue
				}
				// Bound fan-out before retaining the next reference, not after
				// constructing an arbitrarily large segment x L0 product.
				planSize += len(path) + 16
				if planSize > importutilv2.SnapshotSourcePlanMaxBytes {
					return merr.WrapErrImportFailedMsg("snapshot source plan exceeds 256 KiB")
				}
				seen[i][path] = packedDelta
				source := files[i].SnapshotSource
				if packedDelta {
					source.ManifestL0Deltalogs = append(source.ManifestL0Deltalogs, path)
				} else {
					source.LegacyL0Deltalogs = append(source.LegacyL0Deltalogs, path)
				}
			}
		}
	}
	for _, file := range files {
		sort.Strings(file.SnapshotSource.LegacyL0Deltalogs)
		sort.Strings(file.SnapshotSource.ManifestL0Deltalogs)
	}
	return importutilv2.ValidateSnapshotImportFiles(files, importutilv2.Options{
		{Key: importutilv2.BackupFlag, Value: "true"}, {Key: importutilv2.SourceType, Value: importutilv2.SourceTypeSnapshot},
	})
}

// normalizeSnapshotImportEncryption uses source metadata, not the presence of
// an EZK or the target schema, to decide whether source decryption is needed.
func normalizeSnapshotImportEncryption(sourceSchema *schemapb.CollectionSchema, options importutilv2.Options) (importutilv2.Options, error) {
	var (
		sourceEzID int64
		encrypted  bool
	)
	for _, property := range sourceSchema.GetProperties() {
		if property.GetKey() != common.EncryptionEzIDKey {
			continue
		}
		parsed, err := strconv.ParseInt(property.GetValue(), 10, 64)
		if err != nil {
			return nil, merr.WrapErrImportFailedMsg(
				"snapshot source has an invalid %s property", common.EncryptionEzIDKey,
			)
		}
		sourceEzID = parsed
		encrypted = true
		break
	}

	if !encrypted {
		// A plaintext snapshot needs no source key. Remove even a malformed EZK
		// before WAL/job persistence so both phases and retries select plaintext
		// reading, including TEXT/LOB, without retaining an unused credential.
		// Allocate a new slice: the request options may be shared by callers.
		normalized := make(importutilv2.Options, 0, len(options))
		for _, option := range options {
			if option.GetKey() != importutilv2.EZK {
				normalized = append(normalized, option)
			}
		}
		return normalized, nil
	}
	importEzk, _ := importutilv2.GetEZK(options)
	if importEzk == "" {
		return nil, merr.WrapErrImportFailedMsg(
			"CMEK-protected snapshot-source import requires ezk",
		)
	}

	importEzID, err := hookutil.GetEzIDByImportEzk(importEzk)
	if err != nil {
		// EZK is request content, so malformed base64/JSON is an input error. Do
		// not leak the opaque key or parser details into logs or client errors.
		return nil, merr.WrapErrImportFailedMsg("snapshot-source import received an invalid ezk")
	}
	if importEzID != sourceEzID {
		return nil, merr.WrapErrImportFailedMsg(
			"snapshot-source ezk belongs to encryption zone %d, source requires zone %d",
			importEzID,
			sourceEzID,
		)
	}

	// StorageV3 stores TEXT values as physical LOB references. Resolving those
	// references requires milvus-storage SegmentReader, whose current C API
	// cannot receive Milvus's source key-retriever context. Non-TEXT schemas use
	// PackedReader, which already accepts that context, so reject only this
	// unsupported combination instead of rejecting all CMEK snapshot imports.
	if typeutil.HasTextField(sourceSchema) {
		return nil, merr.WrapErrOperationNotSupportedMsg(
			"CMEK-protected snapshot-source import does not support TEXT/LOB fields",
		)
	}
	return options, nil
}

func snapshotImportValidationData(
	snapshot *storage.SnapshotData,
	segments []*datapb.SegmentDescription,
) *storage.SnapshotData {
	validation := *snapshot
	validation.Segments = make([]*datapb.SegmentDescription, 0, len(segments))
	for _, segment := range segments {
		cloned := proto.Clone(segment).(*datapb.SegmentDescription)
		// Snapshot Import reads logical rows, manifest-owned deltalogs, and LOBs.
		// It neither restores nor opens protobuf index/stat paths, which may be
		// stale and must not make an otherwise readable row source fail.
		cloned.Statslogs = nil
		if cloned.GetSegmentLevel() != datapb.SegmentLevel_L0 || cloned.GetStorageVersion() == milvusstorage.StorageV3 {
			cloned.Deltalogs = nil
		} else {
			cloned.Binlogs = nil
			cloned.ManifestPath = "" // Legacy L0 is an explicit delete inventory.
		}
		cloned.Bm25Statslogs = nil
		cloned.IndexFiles = nil
		cloned.TextIndexFiles = nil
		cloned.JsonKeyIndexFiles = nil
		validation.Segments = append(validation.Segments, cloned)
	}
	return &validation
}

func validateSnapshotImportSchema(target, source *schemapb.CollectionSchema) error {
	if target == nil {
		return merr.WrapErrImportSysFailed("target collection schema is missing")
	}
	if typeutil.IsExternalCollection(target) ||
		target.GetExternalSource() != "" ||
		target.GetExternalSpec() != "" {
		return merr.WrapErrOperationNotSupportedMsg(
			"snapshot-source import does not support an external target collection",
		)
	}
	if source == nil {
		return merr.WrapErrImportFailedMsg("snapshot source schema is missing")
	}
	// Match ordinary backup import: the target schema interprets physical field
	// IDs, and the reader validates required columns against each source segment.
	// Snapshot metadata is not an extra schema-equality or name-mapping contract.
	// Renaming a field does not remap its data; callers must retain the intended
	// field-ID correspondence, just as when importing binlog backups. Optional
	// target columns, AutoID and target routing use the existing Import behavior.
	return nil
}
