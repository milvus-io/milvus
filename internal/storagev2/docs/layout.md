

**storage layer interface**: supply reader/writer of storage which contains read options. Maintain meta of storage and handle atomic read/write with multiple files (maybe have different format) on disks.

---

**File Reader/Writer interface**: receive data and read options from upper layer and turn the raw data to our defined data.

---

**File Format Reader/Writer**: file format reader/writer (eg. parquet/raw/others like orc).

---

**File system interface**: support different file system (eg. in-memory, aws, minio, posix, windows).



 



