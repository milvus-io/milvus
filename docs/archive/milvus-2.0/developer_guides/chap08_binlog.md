## 8 Binlog

InsertBinlog、DeleteBinlog、DDLBinlog

Binlog is stored in a columnar storage format, every column in schema is stored in an individual file.
Timestamp, schema, row id and primary key allocated by system are four special columns.
Schema column records the DDL of the collection.

## Event format

Binlog file consists of 4 bytes magic number and a series of events. The first event must be a descriptor event.

### 8.1 Event format

```
+=====================================+=====================================================================+
| event  | Timestamp         0 : 8    | create timestamp                                                    |
| header +----------------------------+---------------------------------------------------------------------+
|        | TypeCode          8 : 1    | event type code                                                     |
|        +----------------------------+---------------------------------------------------------------------+
|        | EventLength       9 : 4    | length of event, including header and data                          |
|        +----------------------------+---------------------------------------------------------------------+
|        | NextPosition     13 : 4    | offset of next event from the start of file                         |
+=====================================+=====================================================================+
| event  | fixed part       17 : x    |                                                                     |
| data   +----------------------------+---------------------------------------------------------------------+
|        | variable part              |                                                                     |
+=====================================+=====================================================================+
```

### 8.2 Descriptor Event format

```
+=====================================+=====================================================================+
| event  | Timestamp         0 : 8    | create timestamp                                                    |
| header +----------------------------+---------------------------------------------------------------------+
|        | TypeCode          8 : 1    | event type code                                                     |
|        +----------------------------+---------------------------------------------------------------------+
|        | EventLength       9 : 4    | length of event, including header and data                          |
|        +----------------------------+---------------------------------------------------------------------+
|        | NextPosition     13 : 4    | offset of next event from the start of file                         |
+=====================================+=====================================================================+
| event  | CollectionID     17 : 8    | collection id                                                       |
| data   +----------------------------+---------------------------------------------------------------------+
|        | PartitionID      25 : 8    | partition id (schema column does not need)                          |
|        +----------------------------+---------------------------------------------------------------------+
|        | SegmentID        33 : 8    | segment id (schema column does not need)                            |
|        +----------------------------+---------------------------------------------------------------------+
|        | FieldID          41 : 8    | field id (schema column does not need)                              |
|        +----------------------------+---------------------------------------------------------------------+
|        | StartTimestamp   49 : 8    | minimum timestamp allocated by master of all events in this file    |
|        +----------------------------+---------------------------------------------------------------------+
|        | EndTimestamp     57 : 8    | maximum timestamp allocated by master of all events in this file    |
|        +----------------------------+---------------------------------------------------------------------+
|        | PayloadDataType  65 : 4    | data type of payload                                                |
|        +----------------------------+---------------------------------------------------------------------+
|        | PostHeaderLengths n : n    | header lengths for all event types                                  |
|        +----------------------------+---------------------------------------------------------------------+
|        | ExtraLength      69 : 4    | length of extra information                                         |
|        +----------------------------+---------------------------------------------------------------------+
|        | ExtraBytes       73 : n    | extra information in json format                                    |
+=====================================+=====================================================================|
```

`ExtraBytes` is in json format.

`ExtraBytes` stores the extra information of the binlog file.

In binlog file, we have stored many common fields in fixed part, such as `CollectionID`, `PartitionID` and etc.

However, different binlog files have some other different information which differs from each other.

So, `ExtraBytes` was designed to store this different information.

For example, for index binlog file, we will store `indexID`, `indexBuildID`, `indexID` and other index-related
information to `ExtraBytes`.

In addition, `ExtraBytes` was also designed to extend binlog. Then we can add new features to binlog file without
breaking the compatibility.

For example, we can store the memory size of original content(before encoding) to `ExtraBytes`.
The key in `ExtraBytes` is `original_size`. For now, `original_size` is required, not optional.

### 8.3 Type code

```
DESCRIPTOR_EVENT
INSERT_EVENT
DELETE_EVENT
CREATE_COLLECTION_EVENT
DROP_COLLECTION_EVENT
CREATE_PARTITION_EVENT
DROP_PARTITION_EVENT
INDEX_FILE_EVENT
```

DESCRIPTOR_EVENT must appear in all column files and always be the first event.

INSERT_EVENT may appear in any column binlog except DDL binlog files.

DELETE_EVENT can only be used in primary key's binlog files（currently we can only delete by primary key).

CREATE_COLLECTION_EVENT、DROP_COLLECTION_EVENT、CREATE_PARTITION_EVENT、DROP_PARTITION_EVENT only appears in DDL binlog files.

### 8.4 Event data part

```
event data part

INSERT_EVENT:
+================================================+==========================================================+
| event  | fixed  |  StartTimestamp      x : 8   | min timestamp in this event                              |
| data   | part   +------------------------------+----------------------------------------------------------+
|        |        |  EndTimestamp      x+8 : 8   | max timestamp in this event                              |
|        +--------+------------------------------+----------------------------------------------------------+
|        |variable|  parquet payload             | payload in parquet format                                |
|        |part    |                              |                                                          |
+================================================+==========================================================+

other events are similar with INSERT_EVENT
```

### 8.5 Example

Schema

​ string | int | float(optional) | vector(512)

Request:

​ InsertRequest rows(1W)

​ DeleteRequest pk=1

​ DropPartition partitionTag="abc"

insert binlogs:

​ rowid, pk, ts, string, int, float, vector 6 files

​ all events are INSERT_EVENT
​ float column file contains some NULL value

delete binlogs:

​ pk, ts 2 files

​ pk's events are DELETE_EVENT, ts's events are INSERT_EVENT

DDL binlogs:

​ ddl, ts

​ ddl's event is DROP_PARTITION_EVENT, ts's event is INSERT_EVENT

C++ interface

```c++
typedef void* CPayloadWriter
typedef struct CBuffer {
  char* data;
  int length;
} CBuffer

typedef struct CStatus {
  int error_code;
  const char* error_msg;
} CStatus


// C++ interface
// writer
CPayloadWriter NewPayloadWriter(int columnType);
CStatus AddBooleanToPayload(CPayloadWriter payloadWriter, bool *values, int length);
CStatus AddInt8ToPayload(CPayloadWriter payloadWriter, int8_t *values, int length);
CStatus AddInt16ToPayload(CPayloadWriter payloadWriter, int16_t *values, int length);
CStatus AddInt32ToPayload(CPayloadWriter payloadWriter, int32_t *values, int length);
CStatus AddInt64ToPayload(CPayloadWriter payloadWriter, int64_t *values, int length);
CStatus AddFloatToPayload(CPayloadWriter payloadWriter, float *values, int length);
CStatus AddDoubleToPayload(CPayloadWriter payloadWriter, double *values, int length);
CStatus AddOneStringToPayload(CPayloadWriter payloadWriter, char *cstr, int str_size);
CStatus AddBinaryVectorToPayload(CPayloadWriter payloadWriter, uint8_t *values, int dimension, int length);
CStatus AddFloatVectorToPayload(CPayloadWriter payloadWriter, float *values, int dimension, int length);

CStatus FinishPayloadWriter(CPayloadWriter payloadWriter);
CBuffer GetPayloadBufferFromWriter(CPayloadWriter payloadWriter);
int GetPayloadLengthFromWriter(CPayloadWriter payloadWriter);
CStatus ReleasePayloadWriter(CPayloadWriter handler);

// reader
CPayloadReader NewPayloadReader(int columnType, uint8_t *buffer, int64_t buf_size);
CStatus GetBoolFromPayload(CPayloadReader payloadReader, bool **values, int *length);
CStatus GetInt8FromPayload(CPayloadReader payloadReader, int8_t **values, int *length);
CStatus GetInt16FromPayload(CPayloadReader payloadReader, int16_t **values, int *length);
CStatus GetInt32FromPayload(CPayloadReader payloadReader, int32_t **values, int *length);
CStatus GetInt64FromPayload(CPayloadReader payloadReader, int64_t **values, int *length);
CStatus GetFloatFromPayload(CPayloadReader payloadReader, float **values, int *length);
CStatus GetDoubleFromPayload(CPayloadReader payloadReader, double **values, int *length);
CStatus GetOneStringFromPayload(CPayloadReader payloadReader, int idx, char **cstr, int *str_size);
CStatus GetBinaryVectorFromPayload(CPayloadReader payloadReader, uint8_t **values, int *dimension, int *length);
CStatus GetFloatVectorFromPayload(CPayloadReader payloadReader, float **values, int *dimension, int *length);

int GetPayloadLengthFromReader(CPayloadReader payloadReader);
CStatus ReleasePayloadReader(CPayloadReader payloadReader);
```
