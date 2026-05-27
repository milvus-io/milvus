// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#include "milvus-storage/ffi_c.h"

/**
 * @brief Inject every external_spec-derived property into an existing
 *        LoonProperties by delegating to C++ InjectExternalSpecProperties.
 *
 * Given a LoonProperties already populated with fs.* baseline (typically
 * produced by loon_properties_create from StorageConfig), this function
 * parses external_source + external_spec and appends:
 *   - extfs.{collection_id}.* storage-layer properties (credentials,
 *     endpoint, AWS-form rewrite, Tier-1/2 derivation)
 *   - format-layer properties derived from spec.format (e.g.
 *     iceberg.snapshot_id when format="iceberg-table")
 *
 * Idempotent over reallocation: the function frees existing LoonProperty
 * entries and rebuilds the array with the merged key set, so the caller
 * should NOT touch the array between create and inject. Caller remains
 * responsible for loon_properties_free.
 *
 * When external_source is null/empty, this is a no-op.
 */
LoonFFIResult
loon_properties_inject_external_spec(LoonProperties* properties,
                                     int64_t collection_id,
                                     const char* external_source,
                                     const char* external_spec);

#ifdef __cplusplus
}
#endif
