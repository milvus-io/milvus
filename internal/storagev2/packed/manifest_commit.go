// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package packed

/*
#cgo pkg-config: milvus_core milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// ManifestUpdates bundles every data-file-level change a single caller
// wants to apply atomically to a manifest. The slow file writes (parquet,
// deltalog, stat blobs) happen before this payload is assembled;
// CommitManifestUpdates then opens a transaction, applies everything in
// one shot, and commits.
//
// NewColumnGroups and NewSegmentOutput hold C memory produced by FFI
// writers and MUST be released by the caller via Destroy after
// CommitManifestUpdates returns (success or failure).
type ManifestUpdates struct {
	// NewColumnGroups is the column-groups payload returned by
	// FFIPackedWriter.Close. nil if no insert files were written.
	NewColumnGroups *ColumnGroups
	// NewSegmentOutput is the column-groups + LOB payload returned by
	// FFISegmentWriter.Close. nil if no segment-writer close happened.
	NewSegmentOutput *SegmentOutput
	// DeltaLogs is the list of delta-log entries to register.
	DeltaLogs []DeltaLogEntry
	// Stats is the list of stat entries (bloom filter, bm25, etc.) to
	// register. Each entry's Files / Metadata replace any existing entry
	// with the same Key (loon overwrite semantics).
	Stats []StatEntry
}

func (u *ManifestUpdates) isEmpty() bool {
	if u == nil {
		return true
	}
	if u.NewColumnGroups != nil && u.NewColumnGroups.cColumnGroups != nil {
		return false
	}
	if u.NewSegmentOutput != nil {
		if u.NewSegmentOutput.cOutput.column_groups != nil {
			return false
		}
		if u.NewSegmentOutput.cOutput.num_lob_files > 0 {
			return false
		}
	}
	return len(u.DeltaLogs) == 0 && len(u.Stats) == 0
}

// CommitManifestUpdates opens a loon transaction at (basePath, baseVersion),
// applies every change in updates, commits, and returns the new manifest
// path. With no effective changes it returns the unchanged manifest path
// without opening a transaction.
//
// The caller retains ownership of any ColumnGroups / SegmentOutput
// referenced by updates and is responsible for calling Destroy on them
// after CommitManifestUpdates returns.
func CommitManifestUpdates(basePath string, baseVersion int64,
	storageConfig *indexpb.StorageConfig, updates *ManifestUpdates,
) (string, error) {
	if updates.isEmpty() {
		return MarshalManifestPath(basePath, baseVersion), nil
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", fmt.Errorf("commit manifest: %w", err)
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	// Use OVERWRITE for the whole bundle: stats updates rely on
	// overwrite-on-key semantics (loon_transaction_update_stat replaces
	// existing entries with the same key), while column-group appends and
	// delta-log adds don't collide on a key in the first place. A single
	// resolve mode keeps the API simple and matches the prior
	// AddStatsToManifest behavior.
	var handle C.LoonTransactionHandle
	res := C.loon_transaction_begin(cBasePath, cProperties,
		C.int64_t(baseVersion),
		C.LOON_TRANSACTION_RESOLVE_OVERWRITE,
		getRetryLimit(), &handle)
	if err := HandleLoonFFIResult(res); err != nil {
		return "", fmt.Errorf("commit manifest begin: %w", err)
	}
	defer C.loon_transaction_destroy(handle)

	if err := applyManifestUpdates(handle, updates); err != nil {
		return "", err
	}

	var commitVersion C.int64_t
	res = C.loon_transaction_commit(handle, &commitVersion)
	if err := HandleLoonFFIResult(res); err != nil {
		return "", fmt.Errorf("commit manifest commit: %w", err)
	}
	return MarshalManifestPath(basePath, int64(commitVersion)), nil
}

// applyManifestUpdates stages every operation in updates onto the loon
// transaction handle.
func applyManifestUpdates(handle C.LoonTransactionHandle, updates *ManifestUpdates) error {
	if cg := updates.NewColumnGroups; cg != nil && cg.cColumnGroups != nil {
		if cg.addNewColumnGroups {
			slice := unsafe.Slice(cg.cColumnGroups.column_group_array, int(cg.cColumnGroups.num_of_column_groups))
			for i := range slice {
				if err := HandleLoonFFIResult(C.loon_transaction_add_column_group(handle, &slice[i])); err != nil {
					return fmt.Errorf("commit manifest add_column_group: %w", err)
				}
			}
		} else {
			if err := HandleLoonFFIResult(C.loon_transaction_append_files(handle, cg.cColumnGroups)); err != nil {
				return fmt.Errorf("commit manifest append_files: %w", err)
			}
		}
	}

	if so := updates.NewSegmentOutput; so != nil {
		if so.cOutput.column_groups != nil {
			if err := HandleLoonFFIResult(C.loon_transaction_append_files(handle, so.cOutput.column_groups)); err != nil {
				return fmt.Errorf("commit manifest append_files (segment): %w", err)
			}
		}
		if so.cOutput.num_lob_files > 0 && so.cOutput.lob_files != nil {
			lob := unsafe.Slice(so.cOutput.lob_files, so.cOutput.num_lob_files)
			for i := range lob {
				if err := HandleLoonFFIResult(C.loon_transaction_add_lob_file(handle, &lob[i])); err != nil {
					return fmt.Errorf("commit manifest add_lob_file: %w", err)
				}
			}
		}
	}

	for _, entry := range updates.DeltaLogs {
		cPath := C.CString(entry.Path)
		err := HandleLoonFFIResult(C.loon_transaction_add_delta_log(handle, cPath, C.int64_t(entry.NumEntries)))
		C.free(unsafe.Pointer(cPath))
		if err != nil {
			return fmt.Errorf("commit manifest add_delta_log: %w", err)
		}
	}

	for _, entry := range updates.Stats {
		if err := UpdateTransactionStat(handle, entry.Key, entry.Files, entry.Metadata); err != nil {
			return fmt.Errorf("commit manifest update_stat: %w", err)
		}
	}
	return nil
}
