# RESTful v2 Server Version Parity

## Overview

This document specifies the RESTful v2 server version endpoint providing parity with PyMilvus 3.0 `MilvusClient.get_server_version()` and `MilvusClient.get_server_version(detail=True)`.

## Endpoint Specification

- **Path:** `/v2/vectordb/server/version`
- **Method:** `POST`
- **Category:** `/server/`
- **Action:** `version`

### Request

Headers:
- `Content-Type: application/json`
- `Authorization: Bearer <token>` (if auth is enabled)

Request Body:
```json
{
  "detail": false
}
```

Parameters:
| Field | Type | Description | Default |
|---|---|---|---|
| `detail` | boolean | Whether to return detailed server build information. Can also be provided via query param `?detail=true`. | `false` |

### Response

#### Basic Version (`detail: false`)
When `detail` is `false` (default), the endpoint delegates to the backend `GetVersion` RPC (`/milvus.proto.milvus.MilvusService/GetVersion`):

```json
{
  "code": 0,
  "data": {
    "version": "2.6.11"
  }
}
```

#### Detailed Version (`detail: true`)
When `detail` is `true`, the endpoint delegates to the backend `Connect` RPC (`/milvus.proto.milvus.MilvusService/Connect`) and extracts server build metadata:

```json
{
  "code": 0,
  "data": {
    "version": "2.6.11",
    "buildTags": "2.6.11",
    "buildTime": "2026-02-26 15:20:47",
    "gitCommit": "2d14975d18",
    "goVersion": "go version go1.23.6 darwin/arm64",
    "deployMode": "standalone"
  }
}
```

### Route Metadata and Metrics

- When `detail` is `false`, the handler invokes `GetVersion` with route metadata `GetVersion` and metric tag `GetVersion`.
- When `detail` is `true`, the handler invokes `Connect` with route metadata `Connect` and metric tag `Connect`.

