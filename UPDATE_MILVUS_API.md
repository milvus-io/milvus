# Update Milvus API Version

This document explains how to use the `update-milvus-api` command to update the milvus-proto version across all go.mod files in the project.

## Overview

The `update-milvus-api` command has been enhanced to update the milvus-proto version in all 4 go.mod files:

1. `go.mod` (main module)
2. `client/go.mod` (client module)
3. `pkg/go.mod` (pkg module)
4. `tests/go_client/go.mod` (test client module)

## Usage

### Update to a specific tagged version

```bash
make update-milvus-api PROTO_API_VERSION=v2.3.0-dev.1
```

### Update to a specific commit ID

```bash
make update-milvus-api PROTO_API_VERSION=4080770055ad
```

## What the command does

1. **Validates the version**: Checks if the provided version is a valid git tag or treats it as a commit ID
2. **Updates all go.mod files**: Updates the milvus-proto dependency in all 4 go.mod files
3. **Runs go mod tidy**: Ensures all dependencies are properly resolved
4. **Updates the local milvus-proto repo**: Updates the third-party milvus-proto repository if it exists
5. **Provides feedback**: Shows which files were updated

## Example output

```
----------------------------
Update the milvus-proto/go-api/v2@v2.3.0-dev.1
Updating milvus-proto version in all go.mod files...
Updating main go.mod...
Updating client/go.mod...
Updating pkg/go.mod...
Updating tests/go_client/go.mod...
----------------------------
Update the milvus-proto repo
----------------------------
Successfully updated milvus-proto version to v2.3.0-dev.1 in all go.mod files:
  - go.mod
  - client/go.mod
  - pkg/go.mod
  - tests/go_client/go.mod
```

## Error handling

- If no version is provided, the command will show usage examples
- If the milvus-proto third-party directory doesn't exist, a warning will be displayed but the go.mod updates will still proceed
- If a git branch with the same name already exists, the command will checkout the commit directly

## Prerequisites

- Go must be installed and available in PATH
- Git must be installed and available in PATH
- Internet connection to fetch the milvus-proto repository

## Notes

- The command will automatically determine if the provided version is a git tag or commit ID
- All go.mod files will be updated to the same version
- The command includes proper error handling and user feedback 