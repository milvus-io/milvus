## Appendix D. Error Code

> **⚠️ Deprecated.** This appendix documents the legacy `commonpb.ErrorCode`
> enum (Milvus 2.0-era), which is no longer the source of truth for error codes.
> Current error codes are the `merr` sentinel codes (e.g. `ParameterInvalid`
> = 1100, `ServiceInternal` = 5, `FunctionFailed` = 2400) carried in
> `commonpb.Status.Code`. The canonical list is the sentinel definitions in
> [`pkg/util/merr/errors.go`](../../pkg/util/merr/errors.go); see also
> [error_handling_guide.md](../dev/error_handling_guide.md) and
> [error_sentinel_convention.md](../dev/error_sentinel_convention.md).

**ErrorCode**

```protobuf
enum ErrorCode {
    Success = 0;
    UnexpectedError = 1;
    ConnectFailed = 2;
    PermissionDenied = 3;
    CollectionNotExists = 4;
    IllegalArgument = 5;
    IllegalDimension = 7;
    IllegalIndexType = 8;
    IllegalCollectionName = 9;
    IllegalTOPK = 10;
    IllegalRowRecord = 11;
    IllegalVectorID = 12;
    IllegalSearchResult = 13;
    FileNotFound = 14;
    MetaFailed = 15;
    CacheFailed = 16;
    CannotCreateFolder = 17;
    CannotCreateFile = 18;
    CannotDeleteFolder = 19;
    CannotDeleteFile = 20;
    BuildIndexError = 21;
    IllegalNLIST = 22;
    IllegalMetricType = 23;
    OutOfMemory = 24;
    IndexNotExist = 25;
    EmptyCollection = 26;

    // internal error code.
    DDRequestRace = 1000;
}
```
