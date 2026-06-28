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
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// WriterOutput is the data carrier returned by an FFI writer's Close. It
// owns C memory and must be released via Destroy after the surrounding
// CommitManifestUpdates call returns (success or failure).
//
// Concrete implementations live alongside the writers that produce them:
//   - *ColumnGroups (FFIPackedWriter.Close)
//   - *SegmentOutput (FFISegmentWriter.Close)
//
// Each implementation knows how to stage its payload onto a loon
// transaction handle via the package-internal applyTo method.
type WriterOutput interface {
	// Destroy releases the underlying C resources. Idempotent.
	Destroy()
	// applyTo stages the output onto a loon transaction handle. Called by
	// applyManifestUpdates as part of CommitManifestUpdates.
	applyTo(handle C.LoonTransactionHandle) error
}

// ManifestUpdates bundles every data-file-level change a single caller
// wants to apply atomically to a manifest. The slow file writes (parquet,
// deltalog, stat blobs) happen before this payload is assembled;
// CommitManifestUpdates then opens a transaction, applies everything in
// one shot, and commits.
//
// NewFiles holds C memory produced by an FFI writer and MUST be released
// by the caller via Destroy after CommitManifestUpdates returns (success
// or failure).
type ManifestUpdates struct {
	// NewFiles is the column-groups / LOB payload returned by an FFI
	// writer's Close. nil if no insert files were written.
	NewFiles WriterOutput
	// DeltaLogs is the list of delta-log entries to register.
	DeltaLogs []DeltaLogEntry
	// Stats is the list of stat entries (bloom filter, bm25, etc.) to
	// register. Each entry's Files / Metadata replace any existing entry
	// with the same Key (loon overwrite semantics).
	Stats []StatEntry
}

// isEmpty short-circuits CommitManifestUpdates when the caller assembled
// no work. A non-nil NewFiles is treated as real work; callers must not
// pass an already-destroyed payload here (the C transaction would then
// commit with zero staged ops and the loon side would return
// "Cannot commit: no updates recorded").
func (u *ManifestUpdates) isEmpty() bool {
	if u == nil {
		return true
	}
	if u.NewFiles != nil {
		return false
	}
	return len(u.DeltaLogs) == 0 && len(u.Stats) == 0
}

// CommitManifestUpdates opens a loon transaction at (basePath, baseVersion),
// applies every change in updates, commits, and returns the new manifest
// path. With no effective changes it returns the unchanged manifest path
// without opening a transaction.
//
// The caller retains ownership of any WriterOutput referenced by updates
// and is responsible for calling Destroy on it after CommitManifestUpdates
// returns.
func CommitManifestUpdates(basePath string, baseVersion int64,
	storageConfig *indexpb.StorageConfig, updates *ManifestUpdates,
) (string, error) {
	if updates.isEmpty() {
		return MarshalManifestPath(basePath, baseVersion), nil
	}

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return "", merr.Wrap(err, "commit manifest")
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
		// HandleLoonFFIResult returns internal FFI sentinels; give them the
		// storage wire code like transaction.go does, instead of leaking 65535.
		return "", merr.WrapErrStorage(err, "commit manifest begin")
	}
	defer C.loon_transaction_destroy(handle)

	if err := applyManifestUpdates(handle, updates); err != nil {
		return "", err
	}

	var commitVersion C.int64_t
	res = C.loon_transaction_commit(handle, &commitVersion)
	if err := HandleLoonFFIResult(res); err != nil {
		return "", merr.WrapErrStorage(err, "commit manifest commit")
	}
	return MarshalManifestPath(basePath, int64(commitVersion)), nil
}

// applyManifestUpdates stages every operation in updates onto the loon
// transaction handle.
func applyManifestUpdates(handle C.LoonTransactionHandle, updates *ManifestUpdates) error {
	if updates.NewFiles != nil {
		if err := updates.NewFiles.applyTo(handle); err != nil {
			return err
		}
	}

	for _, entry := range updates.DeltaLogs {
		cPath := C.CString(entry.Path)
		err := HandleLoonFFIResult(C.loon_transaction_add_delta_log(handle, cPath, C.int64_t(entry.NumEntries)))
		C.free(unsafe.Pointer(cPath))
		if err != nil {
			return merr.WrapErrStorage(err, "commit manifest add_delta_log")
		}
	}

	for _, entry := range updates.Stats {
		if err := UpdateTransactionStat(handle, entry.Key, entry.Files, entry.Metadata); err != nil {
			return merr.WrapErrStorage(err, "commit manifest update_stat")
		}
	}
	return nil
}
