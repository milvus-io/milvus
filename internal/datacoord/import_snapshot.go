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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
// manifests independently.
func expandSnapshotImportFiles(
	ctx context.Context,
	cm milvusstorage.ChunkManager,
	targetSchema *schemapb.CollectionSchema,
	files []*internalpb.ImportFile,
	options importutilv2.Options,
) ([]*internalpb.ImportFile, error) {
	if err := importutilv2.ValidateSnapshotSourceOptions(options); err != nil {
		return nil, err
	}
	if !importutilv2.IsSnapshotSource(options) {
		return files, nil
	}
	if cm == nil {
		return nil, merr.WrapErrServiceInternalMsg("chunk manager cannot be nil")
	}
	if len(files) != 1 || len(files[0].GetPaths()) != 1 || strings.TrimSpace(files[0].GetPaths()[0]) == "" {
		return nil, merr.WrapErrImportFailedMsg(
			"snapshot-source import requires exactly one snapshot metadata path",
		)
	}

	metadataPath := strings.TrimSpace(files[0].GetPaths()[0])
	// Public snapshot input must carry a storage identity before references are
	// normalized to object keys. Internal manifest paths and resumed jobs do
	// not pass through expansion and retain their existing representation.
	bucket, _, _, err := storage.ParseForeignURI(metadataPath)
	if err != nil {
		return nil, err
	}
	if bucket == "" {
		return nil, merr.WrapErrParameterInvalidMsg("snapshot-source import requires a complete metadata URI; bare object keys are not supported")
	}
	if !importutilv2.HasExternalSource(options) {
		if err := storage.ValidateInstanceSnapshotImportURI(storage.InstanceConfigFromParamtable(paramtable.Get()), metadataPath); err != nil {
			return nil, err
		}
	}
	cm, sourceConfig, err := importutilv2.ResolveSnapshotImportStorage(ctx, cm, compaction.CreateStorageConfig(), metadataPath, options)
	if err != nil {
		return nil, err
	}
	if err := storage.ValidateSnapshotObjectPathForBucket(cm, "snapshot_source", metadataPath, ""); err != nil {
		return nil, err
	}
	snapshot, err := storage.NewSnapshotReader(cm).ReadSnapshot(ctx, metadataPath, true)
	if err != nil {
		return nil, merr.Wrap(err, "failed to read snapshot import source")
	}
	if snapshot == nil {
		return nil, merr.WrapErrImportSysFailed("snapshot reader returned no data")
	}
	if err := storage.ValidateSnapshotMetadataLocation(metadataPath, snapshot.SnapshotInfo); err != nil {
		return nil, err
	}
	if snapshot.Layout != datapb.SnapshotLayout_SnapshotLayoutReferenced &&
		snapshot.Layout != datapb.SnapshotLayout_SnapshotLayoutSelfContained {
		return nil, merr.WrapErrImportFailedMsg("unsupported snapshot layout: %s", snapshot.Layout.String())
	}
	sourceSchema := snapshot.Collection.GetSchema()
	if sourceSchema == nil {
		return nil, merr.WrapErrImportFailedMsg("snapshot source schema is missing")
	}
	if typeutil.IsExternalCollection(sourceSchema) ||
		sourceSchema.GetExternalSource() != "" ||
		sourceSchema.GetExternalSpec() != "" {
		return nil, merr.WrapErrOperationNotSupportedMsg(
			"external collection snapshots cannot be used as import sources",
		)
	}
	if err := validateSnapshotImportEncryption(sourceSchema, options); err != nil {
		return nil, err
	}
	if err := validateSnapshotImportSchema(targetSchema, sourceSchema); err != nil {
		return nil, err
	}

	// The snapshot defines the source scope. Import all data partitions while
	// retaining their original IDs/channels for L0 matching; target placement
	// is independent and follows ordinary Import target partition routing.
	segments := make([]*datapb.SegmentDescription, 0)
	for _, segment := range snapshot.Segments {
		if segment == nil {
			return nil, merr.WrapErrImportFailed("snapshot contains a nil segment")
		}
		if segment.GetSegmentLevel() != datapb.SegmentLevel_L0 {
			segments = append(segments, segment)
		}
	}
	if len(segments) == 0 {
		return nil, merr.WrapErrImportFailedMsg("snapshot contains no data segments")
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetSegmentId() < segments[j].GetSegmentId()
	})
	if len(segments) > paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt() {
		return nil, merr.WrapErrImportFailedMsg("The max number of import files should not exceed %d, but got %d",
			paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt(), len(segments))
	}

	seenManifests := make(map[string]struct{}, len(segments))
	result := make([]*internalpb.ImportFile, 0, len(segments))
	for _, segment := range segments {
		if segment.GetStorageVersion() != milvusstorage.StorageV3 {
			return nil, merr.WrapErrOperationNotSupportedMsg(
				"snapshot-source import only supports StorageV3 segments, segment %d uses storage version %d",
				segment.GetSegmentId(), segment.GetStorageVersion(),
			)
		}
		manifestPath := segment.GetManifestPath()
		if manifestPath == "" {
			return nil, merr.WrapErrImportFailedMsg(
				"StorageV3 segment %d has no manifest path", segment.GetSegmentId(),
			)
		}
		_, version, err := packed.UnmarshalManifestPath(manifestPath)
		if err != nil {
			return nil, merr.WrapErrImportFailedMsg(
				"invalid manifest path for StorageV3 segment %d: %s", segment.GetSegmentId(), err,
			)
		}
		if version == packed.ManifestLatest {
			return nil, merr.WrapErrImportFailedMsg(
				"snapshot segment %d must reference an exact manifest version", segment.GetSegmentId(),
			)
		}
		if _, ok := seenManifests[manifestPath]; ok {
			return nil, merr.WrapErrImportFailedMsg(
				"snapshot source contains duplicate manifest for segment %d", segment.GetSegmentId(),
			)
		}
		seenManifests[manifestPath] = struct{}{}
		result = append(result, &internalpb.ImportFile{Paths: []string{manifestPath}})
	}
	applicable, err := snapshotImportL0Segments(snapshot.Segments, segments)
	if err != nil {
		return nil, err
	}
	if len(applicable) != 0 && !snapshot.SnapshotInfo.GetSegmentCommitTimestampsPreserved() {
		return nil, merr.WrapErrOperationNotSupportedMsg("snapshot with applicable L0 lacks source commit timestamps; create a new snapshot")
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
		return nil, merr.Wrap(err, "invalid snapshot import source files")
	}
	if len(applicable) != 0 {
		if err := attachSnapshotImportL0(ctx, cm, metadataPath, snapshot, segments, applicable, result, sourceConfig); err != nil {
			return nil, err
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
	return result, nil
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

func attachSnapshotImportL0(ctx context.Context, cm milvusstorage.ChunkManager, metadataPath string,
	snapshot *storage.SnapshotData, data, deltas []*datapb.SegmentDescription, files []*internalpb.ImportFile,
	sourceConfig *indexpb.StorageConfig,
) error {
	planSize := 0
	seen := make([]map[string]bool, len(files))
	for i, segment := range data {
		files[i].SnapshotSource = &internalpb.SnapshotImportSource{
			Version: 1, ManifestPath: segment.GetManifestPath(), SourceCommitTimestamp: segment.GetCommitTimestamp(),
		}
		files[i].Paths = nil // Old workers must fail instead of ignoring deletes.
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

func validateSnapshotImportEncryption(sourceSchema *schemapb.CollectionSchema, options importutilv2.Options) error {
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
			return merr.WrapErrImportFailedMsg(
				"snapshot source has an invalid %s property", common.EncryptionEzIDKey,
			)
		}
		sourceEzID = parsed
		encrypted = true
		break
	}

	importEzk, _ := importutilv2.GetEZK(options)
	if !encrypted {
		if importEzk != "" {
			return merr.WrapErrImportFailedMsg(
				"snapshot-source import must not specify ezk for an unencrypted source",
			)
		}
		return nil
	}
	if importEzk == "" {
		return merr.WrapErrImportFailedMsg(
			"CMEK-protected snapshot-source import requires ezk",
		)
	}

	importEzID, err := hookutil.GetEzIDByImportEzk(importEzk)
	if err != nil {
		// EZK is request content, so malformed base64/JSON is an input error. Do
		// not leak the opaque key or parser details into logs or client errors.
		return merr.WrapErrImportFailedMsg("snapshot-source import received an invalid ezk")
	}
	if importEzID != sourceEzID {
		return merr.WrapErrImportFailedMsg(
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
		return merr.WrapErrOperationNotSupportedMsg(
			"CMEK-protected snapshot-source import does not support TEXT/LOB fields",
		)
	}
	return nil
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
	targetIdentity := snapshotImportSchemaIdentity(target)
	sourceIdentity := snapshotImportSchemaIdentity(source)
	if !proto.Equal(targetIdentity, sourceIdentity) {
		return merr.WrapErrImportFailedMsg(
			"snapshot source schema is incompatible with target collection schema",
		)
	}
	return nil
}

// snapshotImportSchemaIdentity keeps only user-visible row-storage semantics.
// Snapshot metadata comes from RootCoord and therefore includes the internal
// RowID and Timestamp fields, while the target schema supplied by Proxy omits
// fields below StartOfUserFieldID. The binlog reader reconstructs those
// internal fields when needed, so comparing them here would reject otherwise
// compatible schemas. Collection names, descriptions, properties, index
// params, and function definitions are also irrelevant because snapshot Import
// neither restores nor executes those collection-level objects.
func snapshotImportSchemaIdentity(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	identity := &schemapb.CollectionSchema{EnableDynamicField: schema.GetEnableDynamicField()}
	identity.Fields = make([]*schemapb.FieldSchema, 0, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		if field == nil {
			identity.Fields = append(identity.Fields, nil)
			continue
		}
		if common.IsSystemField(field.GetFieldID()) {
			continue
		}
		cloned := proto.Clone(field).(*schemapb.FieldSchema)
		normalizeSnapshotImportField(cloned)
		identity.Fields = append(identity.Fields, cloned)
	}
	sort.Slice(identity.Fields, func(i, j int) bool {
		return identity.Fields[i].GetFieldID() < identity.Fields[j].GetFieldID()
	})
	identity.StructArrayFields = make([]*schemapb.StructArrayFieldSchema, 0, len(schema.GetStructArrayFields()))
	for _, field := range schema.GetStructArrayFields() {
		if field == nil {
			identity.StructArrayFields = append(identity.StructArrayFields, nil)
			continue
		}
		cloned := proto.Clone(field).(*schemapb.StructArrayFieldSchema)
		cloned.Name = ""
		cloned.Description = ""
		sortSnapshotImportParams(cloned.TypeParams)
		for _, child := range cloned.Fields {
			normalizeSnapshotImportField(child)
		}
		sort.Slice(cloned.Fields, func(i, j int) bool {
			return cloned.Fields[i].GetFieldID() < cloned.Fields[j].GetFieldID()
		})
		cloned.ProtoReflect().SetUnknown(nil)
		identity.StructArrayFields = append(identity.StructArrayFields, cloned)
	}
	sort.Slice(identity.StructArrayFields, func(i, j int) bool {
		return identity.StructArrayFields[i].GetFieldID() < identity.StructArrayFields[j].GetFieldID()
	})
	return identity
}

func normalizeSnapshotImportField(field *schemapb.FieldSchema) {
	if field == nil {
		return
	}
	field.Name = ""
	field.Description = ""
	field.IndexParams = nil
	field.ExternalField = ""
	field.State = schemapb.FieldState_FieldCreated
	sortSnapshotImportParams(field.TypeParams)
	normalizeSnapshotImportType(field.TypeSchema)
	field.ProtoReflect().SetUnknown(nil)
}

func normalizeSnapshotImportType(typeSchema *schemapb.TypeSchema) {
	if typeSchema == nil {
		return
	}
	sortSnapshotImportParams(typeSchema.TypeParams)
	normalizeSnapshotImportType(typeSchema.GetArrayElement())
	typeSchema.ProtoReflect().SetUnknown(nil)
}

func sortSnapshotImportParams(params []*commonpb.KeyValuePair) {
	sort.Slice(params, func(i, j int) bool {
		if params[i].GetKey() == params[j].GetKey() {
			return params[i].GetValue() < params[j].GetValue()
		}
		return params[i].GetKey() < params[j].GetKey()
	})
}
