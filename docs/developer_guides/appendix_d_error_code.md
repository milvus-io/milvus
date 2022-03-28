## Appendix D. Error Code

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
