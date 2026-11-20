# Design: Bulk Import a Fixed Lance Snapshot through Parquet

## Summary

This design adds support for bulk importing a fixed Lance snapshot into Milvus by converting the selected Lance snapshot to Parquet and reusing the existing Parquet import pipeline. The goal is to minimize scope changes, keep the import logic consistent with the current format-based import flow, and avoid modifying the core Parquet reader.

## Motivation

Milvus currently supports bulk import through multiple file formats through a common import path. Lance is a new supported source format, but it is not yet integrated into the import workflow. We need to allow importing a specific Lance snapshot version, not just the latest available snapshot, while reusing the existing downstream import code.

The main requirements are:

- Import a fixed Lance snapshot identified by an explicit version
- Reuse the existing Parquet reader and import pipeline
- Keep file-type detection and option handling aligned with the existing import framework
- Avoid broad changes to `parquet/`

## Goals

- Support `FileTypeLance` in the bulk import entry points
- Require a Lance version instead of using a default "latest"
- Convert a Lance snapshot to Parquet before downstream import
- Reuse the existing Parquet import path and validation logic
- Keep behavior deterministic and version-pinned

## Non-Goals

- Changing the behavior of the existing Parquet import path
- Adding a new full-feature import engine for Lance
- Supporting implicit "latest" snapshot resolution
- Refactoring unrelated file import logic

## Background

Milvus bulk import is structured around file type detection, reader construction, and downstream ingestion logic. Each format has:

- a file type constant
- an option accessor for format-specific settings
- a reader implementation under a dedicated package
- a `NewReader` factory branch
- common import validation and execution logic

The Lance import flow should follow this same structure and delegate the actual snapshot conversion to a format-specific conversion step.

## Design

### 1. File type detection

Add a new `FileTypeLance` entry to the existing file-type enumeration and detection logic.

Rules:
- Recognize `.lance` files
- Treat Lance input as a single-path import source, similar to Parquet/CSV
- Fail early for multi-file Lance import patterns that do not fit the standard import API

This keeps Lance consistent with the current import contract.

### 2. Required version option

Add a required Lance version option modeled after existing format-specific options such as CSV separator handling.

Example semantics:
- `GetLanceVersion()` returns the desired snapshot version
- The option is mandatory
- No implicit "latest" fallback is allowed

This makes the import deterministic and ensures the selected snapshot is fixed at import time rather than dynamically changing with upstream state.

### 3. Reader factory integration

Extend the `NewReader` factory with a `FileTypeLance` branch.

Behavior:
- Create a Lance reader instance for `.lance` input
- Validate the input version requirement
- Delegate conversion to the Lance-to-Parquet layer

This ensures Lance input follows the same top-level importer flow as the other supported formats.

### 4. Lance conversion layer

Implement a Lance-specific package, e.g. `lance/`, that contains:
- reader construction and initialization
- version validation
- Lance snapshot conversion logic to Parquet
- integration with the existing Parquet reader abstraction

The conversion layer is intentionally a thin adapter:
- read the fixed Lance snapshot
- materialize it into a temporary Parquet representation
- pass the result into the existing Parquet import path

This minimizes changes and avoids touching the existing Parquet reader internals.

## Data Flow

1. User specifies a Lance source path and snapshot version
2. Import code detects `FileTypeLance`
3. Version option is validated and required
4. Reader factory creates a Lance reader
5. Lance reader loads the fixed snapshot
6. Snapshot is converted to Parquet
7. Downstream import logic consumes the Parquet data using the existing implementation

## Compatibility and Backward Compatibility

This change is additive:
- no existing import formats are changed
- no Parquet behavior is altered
- no default version behavior is introduced for existing formats

The only new requirement is for Lance import to provide an explicit version.

## Risks and Mitigations

### Risk: hidden snapshot drift
If the import logic resolves "latest" implicitly, data can change between runs.

Mitigation:
- Require an explicit Lance version and reject missing versions

### Risk: conversion incompatibility
Lance-to-Parquet conversion may fail for unsupported schema or type combinations.

Mitigation:
- Validate the snapshot before conversion
- Reuse existing Parquet validation and import checks
- Fail early with clear error messages

### Risk: large import memory usage
Snapshot conversion may temporarily expand data before Parquet import.

Mitigation:
- Keep the conversion step local to the importer and aligned with existing import staging behavior
- Avoid broad in-memory changes outside the conversion layer

## Rollout

This feature is introduced behind the Lance format path and does not change the behavior of existing import formats. It can be rolled out in the standard import flow after the Lance converter is implemented and verified by CI and targeted tests.

## Testing

Planned validation:
- option validation for required Lance version
- file-type detection for `.lance`
- NewReader factory dispatch for `FileTypeLance`
- conversion smoke tests for a valid fixed snapshot
- end-to-end import flow using a Lance snapshot converted to Parquet

## Alternatives Considered

### Alternative 1: Write a native Lance reader
This would require a larger implementation footprint and bypass the existing import framework. It increases maintenance and risk.

### Alternative 2: Use the latest snapshot implicitly
This is simpler, but it breaks deterministic imports and violates the fixed-snapshot requirement. It is rejected.

### Alternative 3: Modify the core Parquet path
This is risky and broad. The project prefers keeping the Parquet reader generic and adding a Lance adapter instead.

## Decision

Implement a Lance-specific adapter that converts a fixed Lance snapshot to Parquet and reuses the existing Parquet import flow. This keeps the design small, deterministic, and consistent with the existing import architecture.