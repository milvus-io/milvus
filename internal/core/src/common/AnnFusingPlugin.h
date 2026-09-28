// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <stdbool.h>
#include <stdint.h>

// Frozen native C ABI: V4 transmits original Milvus enum values without ANN-specific mappings.
// V1/V2/V3 factories are intentionally not provided. Incompatible versions fail
// symbol resolution, never reinterpret an older request. No protobuf objects,
// STL ownership or exceptions cross the DSO boundary.
typedef struct MilvusAnnFusingRuleV4 {
    uint32_t struct_size;
    uint32_t data_type;  // milvus::DataType
    uint32_t expr_type;  // proto::plan::Expr::ExprCase
    uint32_t operation;  // proto::plan::OpType (comparison); Invalid if absent
    uint32_t arith_operation;  // proto::plan::ArithOpType; Unknown if absent
    uint32_t
        access_path;  // milvus::exec::ExprExecPath, separate from index kind
    uint32_t index_type;  // milvus::index::ScalarIndexType; NONE is not UNKNOWN
} MilvusAnnFusingRuleV4;

typedef struct MilvusAnnFusingSampleV4 {
    uint32_t struct_size;
    double filter_ratio;            // whole user predicate estimate, not count
    double mandatory_filter_ratio;  // system visibility only; -1 if unknown
} MilvusAnnFusingSampleV4;

typedef struct MilvusAnnFusingPluginV4 {
    uint32_t struct_size;
    uint32_t abi_major;
    void* context;  // immutable, thread-safe callbacks; valid until destroy
    bool (*consider)(const void*, const MilvusAnnFusingRuleV4*);
    bool (*choose)(const void*, const MilvusAnnFusingSampleV4*);
    void (*destroy)(void*);
} MilvusAnnFusingPluginV4;

// No in-place ABI upgrade. Invalid ABI/config leaves the caller's out untouched.
typedef bool (*MilvusCreateAnnFusingPluginV4Fn)(const char* yaml_path,
                                                uint32_t output_size,
                                                MilvusAnnFusingPluginV4* out);
