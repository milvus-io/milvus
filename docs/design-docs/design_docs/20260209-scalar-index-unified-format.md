# Scalar Index V3 Format

## 1. Goals

| Goal | Description |
|------|-------------|
| Single file | All entries packed into one remote object |
| Concurrent upload | Multipart Upload in parallel |
| Concurrent encrypt/decrypt | Slice-level parallelism, same granularity as V2 |
| Concurrent download | `ReadAt` multi-threaded parallel |
| Low memory peak | `O(W × slice_size)`, does not grow with entry size |
| Encryption transparent | Index classes unaware of encryption |
| Reduce small IO | O(1) meta packed into a single entry |
| Compatible with V2 | Go control plane routes by `SCALAR_INDEX_ENGINE_VERSION`; data lake by filename format |

---

## 2. File Format

### 2.1 Layout

```
┌─────────────────────────────────────────┐
│ Magic Number (8 bytes): "MVSIDXV3"       │
├──────────── Data Region ────────────────┤
│ Entry 0 data                             │
│ ...                                      │
│ Entry N data                             │
│ Meta Entry data (always last entry)      │
├──────────── Directory Table (JSON) ─────┤
│ { "entries": [...] }                     │
├──────────── Footer (32 bytes) ──────────┤
│ version       (uint16)  — format version │
│ reserved      (22 bytes) — zero-filled   │
│ meta_entry_size   (uint32)               │
│ directory_table_size (uint32)            │
└─────────────────────────────────────────┘
```

Tail three sections can be located from the end of file:
- Footer: last 32 bytes.
- Directory Table: `directory_table_size` bytes before Footer.
- Meta Entry: `meta_entry_size` bytes before Directory Table.

Data is written first, Meta Entry written last, then Directory Table and Footer appended.

### 2.2 Slice — Encryption Only

Slice is the **encryption boundary**, not a general format concept:

- **Encrypted**: Each entry split into fixed-size slices (default 16MB). Each slice independently encrypted via `IEncryptor::Encrypt()`. Slice boundaries recorded in Directory Table.
- **Unencrypted**: Entry is contiguous plaintext. Directory Table records `offset` + `size`. EntryReader self-splits by range for concurrent `ReadAt`.

### 2.3 Directory Table

**Unencrypted**:

```json
{
  "entries": [
    {"name": "SORT_INDEX_META", "offset": 0, "size": 48, "crc32": "A1B2C3D4"},
    {"name": "index_data", "offset": 48, "size": 100000000, "crc32": "E5F6A7B8"},
    {"name": "__meta__", "offset": 100000048, "size": 64, "crc32": "C3D4E5F6"}
  ]
}
```

**Encrypted**:

```json
{
  "slice_size": 16777216,
  "entries": [
    {
      "name": "SORT_INDEX_META",
      "original_size": 48,
      "crc32": "A1B2C3D4",
      "slices": [
        {"offset": 0, "size": 76}
      ]
    },
    {
      "name": "index_data",
      "original_size": 100000000,
      "crc32": "E5F6A7B8",
      "slices": [
        {"offset": 76, "size": 16777244},
        {"offset": 16777320, "size": 16777244}
      ]
    },
    {
      "name": "__meta__",
      "original_size": 64,
      "crc32": "C3D4E5F6",
      "slices": [
        {"offset": 33554564, "size": 92}
      ]
    }
  ],
  "__edek__": "base64_encoded_encrypted_dek",
  "__ez_id__": "12345"
}
```

Field semantics:
- `version` is in the Footer (uint16), not in the Directory Table.
- Unencrypted: `offset` + `size` are position within Data Region (relative to Magic end).
- Encrypted: all entries use `original_size` + `slices[]` format uniformly.
  - `original_size`: plaintext size, for pre-allocating buffer.
  - `slices[]`: each slice's position and ciphertext size (including IV/Tag overhead) in Data Region.
  - Even single-slice entries use `slices[]`.
- `crc32`: CRC-32C (Castagnoli) checksum of the entry's **plaintext** data, stored as 8-char uppercase hex string. Present on every entry in both encrypted and unencrypted modes. Computed over the original data before encryption; verified after decryption (encrypted) or after read (unencrypted).
- `__edek__` / `__ez_id__`: encryption metadata, present only when encrypted. `__ez_id__` stored as string.
- `slice_size`: plaintext slice size, present only when encrypted.

**Detection**: `__edek__` present → encrypted; absent → unencrypted.

### 2.4 Meta Entry

The Meta Entry is the **last entry** in the Data Region, always named `__meta__`. Physically it is a regular entry — same write, read, encryption treatment as all other entries. It is listed in the Directory Table like any other entry.

Its position can also be located directly from the Footer (`meta_entry_size` bytes before Directory Table), enabling a single tail read to fetch Footer + Directory Table + Meta Entry without scanning the full directory.

Content is a JSON object. Initial fields:

```json
{
  "index_type": "stlsort",
  "build_id": 12345
}
```

The schema is extensible — new fields can be added without format version change.

### 2.5 Footer

Fixed 32 bytes, always the last 32 bytes of the file:

```
┌──────────────────────────────────────────┐
│ Offset  Size   Field                      │
├──────────────────────────────────────────┤
│  0      2B     version (uint16, LE)       │
│  2      22B    reserved (zero-filled)     │
│  24     4B     meta_entry_size (uint32, LE)│
│  28     4B     directory_table_size (uint32, LE)│
└──────────────────────────────────────────┘
```

- `version`: format version, currently `3`.
- `reserved`: zero-filled, available for future use.
- `meta_entry_size`: on-disk byte size of the Meta Entry (ciphertext size if encrypted, plaintext size if not).
- `directory_table_size`: byte size of the Directory Table JSON.

**Read sequence from tail**:
1. Read last 32 bytes → parse Footer → get `version`, `meta_entry_size`, `directory_table_size`.
2. Compute `tail_size = 32 + directory_table_size + meta_entry_size`.
3. If `tail_size ≤ initial_tail_read_size`, a single initial tail read already covers all three sections. `initial_tail_read_size` is a Milvus runtime config (default 64KB), not part of the file format.

---

## 3. Upload Path

### 3.1 IndexEntryWriter Interface

Index classes only see this interface, unaware of encryption and transport:

```cpp
// storage/IndexEntryWriter.h
namespace milvus::storage {

constexpr char MILVUS_V3_MAGIC[] = "MVSIDXV3";
constexpr size_t MILVUS_V3_MAGIC_SIZE = 8;

class IndexEntryWriter {
public:
    virtual ~IndexEntryWriter() = default;
    virtual void WriteEntry(const std::string& name, const void* data, size_t size) = 0;
    virtual void WriteEntry(const std::string& name, int fd, size_t size) = 0;
    // Set file-level metadata (written as the __meta__ entry during Finish).
    virtual void SetMeta(const std::string& json) = 0;
    virtual void Finish() = 0;
    virtual size_t GetTotalBytesWritten() const = 0;
};
}
```

**Entry name uniqueness**: `CheckDuplicateName` is in the base class, inherited by both subclasses. Duplicate names throw immediately.

### 3.2 Two Implementations

```
CreateIndexEntryWriterV3(filename, is_index_file)
    ├─ No encryption → IndexEntryDirectStreamWriter → direct write RemoteOutputStream
    └─ Encryption    → IndexEntryEncryptedLocalWriter → local temp file → upload
```

**Why two paths**:
- Unencrypted: data size predictable, write directly to remote stream. `RemoteOutputStream` internal `background_writes` handles parallel upload.
- Encrypted: ciphertext size unpredictable. Write to local file (>1GB/s, no network backpressure blocking encryption pipeline). Local file also provides task-level retry capability.

### 3.3 IndexEntryDirectStreamWriter (Unencrypted)

```
Index → WriteEntry → RemoteOutputStream → background_writes → S3 Multipart
```

```cpp
// storage/IndexEntryDirectStreamWriter.h
class IndexEntryDirectStreamWriter : public IndexEntryWriter {

    explicit IndexEntryDirectStreamWriter(
        std::shared_ptr<milvus::OutputStream> output,
        size_t read_buf_size = 16 * 1024 * 1024);

    void WriteEntry(const std::string& name, const void* data, size_t size) override;
    void WriteEntry(const std::string& name, int fd, size_t size) override;
    void Finish() override;
    size_t GetTotalBytesWritten() const override;
};
```

Constructor writes Magic. `WriteEntry()` computes CRC-32C incrementally (`crc32c_update` per chunk as data streams through) and records the final checksum in the directory entry. For fd-based entries, CRC is updated per 16MB read chunk — no extra memory. `Finish()` writes the Meta Entry (as the last regular entry), then Directory Table JSON, then 32-byte Footer (`version` + `meta_entry_size` + `directory_table_size`), computes `total_bytes_written_`, closes stream.

### 3.4 IndexEntryEncryptedLocalWriter (Encrypted)

```
Index → WriteEntry → encrypt thread pool (parallel) → ordered queue → sequential write local file
                                                            ↓ Finish()
                                                 upload local file → S3
```

Encryption pipeline uses sliding window: W slices encrypt in parallel, dequeued in submission order, written sequentially to local file. Offset from actual `encrypted.size()` increment, always correct. CRC-32C is computed incrementally by the main thread: each slice's plaintext is fed to `crc32c_update` before submitting to the encryption thread pool. Sequential read order guarantees correctness, no extra memory.

**Thread pool**: Reuses V2 global priority thread pool `ThreadPools::GetThreadPool(MIDDLE)`.

**Thread safety**: Each slice encryption task creates its own `IEncryptor` inside the lambda, destroyed after use. No sharing, no caching — global thread pool threads outlive Writer, caching would leave stale edek.

```cpp
// storage/IndexEntryEncryptedLocalWriter.h
class IndexEntryEncryptedLocalWriter : public IndexEntryWriter {
public:
    IndexEntryEncryptedLocalWriter(const std::string& remote_path,
                         milvus_storage::ArrowFileSystemPtr fs,
                         std::shared_ptr<plugin::ICipherPlugin> cipher_plugin,
                         int64_t ez_id, int64_t collection_id,
                         const std::string& temp_dir = "",
                         size_t slice_size = 16 * 1024 * 1024);
    ~IndexEntryEncryptedLocalWriter();

    void WriteEntry(const std::string& name, const void* data, size_t size) override;
    void WriteEntry(const std::string& name, int fd, size_t size) override;
    void Finish() override;
    size_t GetTotalBytesWritten() const override;

private:
    void EncryptAndWriteSlices(const std::string& name, uint64_t original_size,
                               const uint8_t* data, size_t size);
    void UploadLocalFile();
};
```

Constructor obtains edek, creates temp file at `<temp_dir>/milvus_enc_<uuid>`, writes Magic. `temp_dir` defaults to the Milvus config value `localStorage.path` (typically `/var/lib/milvus/data`); if that is empty, falls back to `/tmp`. Destructor cleans up fd and temp file. `Finish()` writes Meta Entry (encrypted, as last entry), then Directory Table (with `__edek__`, `__ez_id__`, `slice_size`), then 32-byte Footer, closes fd, uploads local file via `UploadLocalFile()`, unlinks temp file.

### 3.5 Factory Method

```cpp
// In FileManagerImpl (storage/FileManager.h)
std::unique_ptr<IndexEntryWriter>
FileManagerImpl::CreateIndexEntryWriterV3(const std::string& filename,
                                          bool is_index_file = true) {
    if (plugin_context_) {
        auto cipher_plugin = PluginLoader::GetInstance().getCipherPlugin();
        if (cipher_plugin) {
            auto remote_path = is_index_file ? GetRemoteIndexObjectPrefixV2()
                                             : GetRemoteTextLogPrefixV2();
            remote_path += "/" + GetFileName(filename);
            return std::make_unique<IndexEntryEncryptedLocalWriter>(
                remote_path, fs_, cipher_plugin,
                plugin_context_->ez_id, plugin_context_->collection_id,
                GetLocalTempDir());
        }
    }
    return std::make_unique<IndexEntryDirectStreamWriter>(
        OpenOutputStream(filename, is_index_file));
}
```

---

## 4. Load Path

### 4.1 IndexEntryReader Interface

```cpp
// storage/IndexEntryReader.h
struct Entry {
    std::vector<uint8_t> data;
};

class IndexEntryReader {
public:
    static std::unique_ptr<IndexEntryReader> Open(
        std::shared_ptr<milvus::InputStream> input,
        int64_t file_size,
        int64_t collection_id = 0,
        ThreadPoolPriority priority = ThreadPoolPriority::HIGH);

    std::vector<std::string> GetEntryNames() const;
    Entry ReadEntry(const std::string& name);
    void ReadEntryToFile(const std::string& name, const std::string& local_path);
    void ReadEntriesToFiles(
        const std::vector<std::pair<std::string, std::string>>& name_path_pairs);

private:
    // Small entries (≤ 1MB) are cached after first read
    static constexpr size_t kSmallEntryCacheThreshold = 1 * 1024 * 1024;
    std::unordered_map<std::string, Entry> small_entry_cache_;
};
```

**Encryption plugin**: `Open()` does not take `ICipherPlugin` explicitly. If Directory Table contains `__edek__`, `IndexEntryReader` obtains the global cipher plugin via `PluginLoader::GetInstance().getCipherPlugin()`. Decryption uses `collection_id`, `ez_id` (from Directory Table), and `edek` to create `IDecryptor`.

**Thread pool**: Reuses V2 global priority pool `ThreadPools::GetThreadPool(priority)`. Default `HIGH`.

### 4.2 Load Flow

1. **Open()**: Validates Magic (1 ReadAt). Reads file tail (up to `initial_tail_read_size` in one ReadAt, default 64KB, configurable) to get Footer + Directory Table + Meta Entry. Parses 32-byte Footer to get `version`, `meta_entry_size`, `directory_table_size`. If `meta_entry_size + directory_table_size + 32` exceeds the initial read, does one additional ReadAt. Parses Directory Table JSON, builds `name → EntryMeta` index. If `__edek__` present, obtains cipher plugin. Meta Entry (`__meta__`) is accessible via normal `ReadEntry("__meta__")`.
2. **ReadEntry(name)** (small entry): Checks `small_entry_cache_` first. One `ReadAt`; if encrypted, creates `IDecryptor` to decrypt. Verifies CRC-32C against the `crc32` field in Directory Table; throws on mismatch. Result cached if ≤ 1MB.
3. **ReadEntry(name)** (large entry, to memory):
   - Encrypted: Thread pool concurrent `ReadAt` for each slice, each task creates own `IDecryptor`, decrypts, copies to target buffer. After all slices assembled, verifies CRC-32C over the full plaintext buffer; throws on mismatch.
   - Unencrypted: Self-splits by 16MB ranges, concurrent `ReadAt`, copies to target buffer. After all ranges assembled, verifies CRC-32C over the full buffer; throws on mismatch.
   - CRC verification is a single sequential pass over the already-assembled buffer. No extra memory.
4. **ReadEntryToFile(name, path)**: Same concurrent read but output via `pwrite` to local file. `pwrite` is lock-free (each range writes different offset). Encrypted path does `ftruncate` first. CRC verified via per-range `crc32c_combine` (see §6.1); no need to re-read the file.
5. **ReadEntriesToFiles(pairs)**: Submits all entries to the thread pool concurrently — each entry's ranges/slices are read in parallel, and multiple entries are also processed in parallel. Cross-entry concurrency shares the same thread pool; total in-flight tasks bounded by pool size.

**Thread safety**: Each decryption task creates own `IDecryptor` in lambda, destroyed after use.

### 4.3 IO Counts

| Step | IO Count |
|------|----------|
| Read Footer + Directory Table + Meta Entry | 1–2 (tail read) |
| Read meta entry | 1 (all O(1) meta packed) |
| Read large data entry | Concurrent, by slice/range count |

---

## 5. Index Class Interface

### 5.1 Base Class

```cpp
// index/ScalarIndex.h
template <typename T>
class ScalarIndex : public IndexBase {
public:
    // V3 streaming upload — calls WriteEntries(), returns IndexStats
    IndexStatsPtr UploadV3(const Config& config) override;

    // V3 streaming load — opens packed file, calls LoadEntries()
    void LoadV3(const Config& config) override;

    virtual void WriteEntries(storage::IndexEntryWriter* writer) {
        ThrowInfo(Unsupported, "WriteEntries is not implemented");
    }
    virtual void LoadEntries(storage::IndexEntryReader& reader, const Config& config) {
        ThrowInfo(Unsupported, "LoadEntries is not implemented");
    }

protected:
    storage::MemFileManagerImplPtr file_manager_;

    // Controls remote path prefix for V3 upload/load:
    //   true  → index_files/...  (default, normal scalar indexes)
    //   false → text_log/...     (TextMatchIndex)
    bool is_index_file_ = true;
};
```

**UploadV3 flow**:
1. Build filename: `milvus_packed_<type>_index.v3` (type from `GetIndexType()`, lowercased).
2. Create writer via `file_manager_->CreateIndexEntryWriterV3(filename, is_index_file_)`.
3. Call `WriteEntries(writer.get())`.
4. Call `writer->SetMeta(json)` with file-level metadata (index type, build id, etc.).
5. Call `writer->Finish()` — writes `__meta__` entry, Directory Table, Footer.
6. Return `IndexStats` with the single packed file path and `writer->GetTotalBytesWritten()`.

**LoadV3 flow**:
1. Read `INDEX_FILES` from config — must contain exactly 1 packed file path.
2. Open `InputStream` via `file_manager_->OpenInputStream(packed_file, is_index_file_)`.
3. Create `IndexEntryReader::Open(input, file_size, collection_id)`.
4. Call `LoadEntries(*reader, config)`.

### 5.2 Meta Packing

Each index packs all O(1) metadata into a **single meta entry**, eliminating multiple small IOs by design.

| Index | Meta Entry | Data Entries |
|-------|-----------|-------------|
| ScalarIndexSort | `SORT_INDEX_META` | `index_data` |
| BitmapIndex | `BITMAP_INDEX_META` | `BITMAP_INDEX_DATA` |
| StringIndexMarisa | (none) | `MARISA_TRIE_INDEX`, `MARISA_STR_IDS` |
| StringIndexSort | `STRING_SORT_META` | `index_data`, `valid_bitset` |
| InvertedIndexTantivy | `TANTIVY_META` | N tantivy files, `index_null_offset` |
| JsonInvertedIndex | `TANTIVY_META` (with `has_non_exist`) | N tantivy files, `index_null_offset`, `json_index_non_exist_offsets` |
| NgramInvertedIndex | `TANTIVY_META` | N tantivy files, `index_null_offset`, `ngram_avg_row_size` |
| RTreeIndex | `RTREE_META` | N rtree files, `index_null_offset` |
| HybridScalarIndex | `HYBRID_INDEX_META` | (delegated to internal index) |

Meta structure definitions:

```cpp
// ScalarIndexSort:  {"index_length": N, "num_rows": N, "is_nested": bool}
// BitmapIndex:      {"bitmap_index_length": N, "bitmap_index_num_rows": N}
// StringIndexSort:  {"version": V, "num_rows": N, "is_nested": bool}
// Tantivy:          {"file_names": [...], "has_null": bool}
// Tantivy (Json):   {"file_names": [...], "has_null": bool, "has_non_exist": bool}
// RTree:            {"file_names": [...], "has_null": bool}
// Hybrid:           {"index_type": uint8}
```

### 5.3 Inheritance Patterns

**InvertedIndexTantivy** provides `BuildTantivyMeta()` as a virtual hook. Subclasses extend by:

| Subclass | Pattern |
|----------|---------|
| JsonInvertedIndex | Overrides `BuildTantivyMeta` to add `has_non_exist`. Overrides `WriteEntries`/`LoadEntries`, calling parent first then adding extra entries. |
| NgramInvertedIndex | Overrides `WriteEntries`/`LoadEntries`, calling parent first then adding `ngram_avg_row_size`. |

This keeps parent meta fields (`file_names`, `has_null`) in the single `TANTIVY_META` entry, one IO. Subclass-specific entries are separate.

---

## 6. Concurrency Model

All thread pools reuse V2 `ThreadPools::GetThreadPool(priority)`, no custom pools.

| Path | Thread Pool | V2 Equivalent |
|------|------------|---------------|
| Encrypted upload | `MIDDLE` (`MIDD_SEGC_POOL`) | V2 `PutIndexData` |
| Download/decrypt | Caller-specified, default `HIGH` (`HIGH_SEGC_POOL`) | V2 `GetObjectData` |
| Unencrypted upload | None (RemoteOutputStream internal `background_writes`) | — |

### Upload — Unencrypted

```
Main thread: [read chunk → crc32c_update → Write to RemoteOutputStream] ──→ sequential fill
RemoteOutputStream:  background_writes parallel upload Part 0, 1, 2...  ──→ S3
```

### Upload — Encrypted

```
Main thread:  read slice_i → crc32c_update(crc, slice_i) → submit encrypt(slice_i)
MIDD_SEGC_POOL (W threads):  parallel Encrypt slice 0, 1, 2...
Main thread:  in-order .get() → sequential write to local file (>1GB/s)
    ↓ Finish()
Upload local file via RemoteOutputStream → S3
```

CRC-32C is computed in the main thread's sequential read loop, before encryption submission. No impact on encryption parallelism.

### Download/Load (Unified)

```
HIGH_SEGC_POOL (N threads, or caller-specified priority):
  Thread i: ReadAt(range_i) → [Decrypt*] → output to memory or pwrite to file
  * Decrypt only when encrypted
  "range" = encrypted: slice boundaries; unencrypted: 16MB self-split
```

### 6.1 CRC-32C Verification in Parallel Download

`crc32c_update(crc, data, len)` is chain-dependent — each call requires the previous CRC value, so it cannot run out of order. For **ReadEntry to memory**, the full plaintext buffer is already assembled after parallel download; CRC is verified via a single sequential pass.

For **ReadEntryToFile**, data is written to file via `pwrite` without passing through a single buffer. CRC is verified using `crc32c_combine`:

```
Thread 0: ReadAt(range_0) → [Decrypt] → pwrite → crc_0 = crc32c(0, range_0_data, len_0)
Thread 1: ReadAt(range_1) → [Decrypt] → pwrite → crc_1 = crc32c(0, range_1_data, len_1)
Thread 2: ReadAt(range_2) → [Decrypt] → pwrite → crc_2 = crc32c(0, range_2_data, len_2)
  (all threads run in parallel, compute per-range CRC independently)

Main thread (after all tasks complete, combine in sequential order):
  crc = crc32c_combine(crc_0, crc_1, len_1)
  crc = crc32c_combine(crc,   crc_2, len_2)
  verify crc == expected
```

**Key properties**:
- Each thread computes its range CRC independently over data already in memory (the read buffer before `pwrite`). No extra memory.
- `crc32c_combine(crc_a, crc_b, len_b)` merges two CRCs algebraically (GF(2) matrix exponentiation, `O(log(len_b))`), without accessing original data.
- Per-range CRC **computation is parallel and order-independent**; only the final **combine must be in data order**.
- Hardware-accelerated CRC-32C (SSE4.2 / ARMv8) runs at ~30 GB/s, negligible vs encryption (~5-10 GB/s) and network IO.

---

## 7. Memory Peak

| Scenario | Peak |
|----------|------|
| Upload, unencrypted, memory entry | entry itself |
| Upload, unencrypted, disk entry | 1 × `read_buf_size` (16MB) |
| Upload, encrypted, memory entry | entry + W × `slice_size` |
| Upload, encrypted, disk entry | W × `slice_size` × 2 |
| Download, to memory | N × `range_size` + `original_size` |
| Download, to file | N × `range_size` (reusable) |

Consistent with V2: peak determined by concurrency × slice size, does not grow with entry size.

CRC-32C adds no extra memory: upload path uses incremental `crc32c_update` on data already being read; download-to-memory verifies over the output buffer; download-to-file computes per-range CRC on the existing read buffer and combines via `crc32c_combine`.

---

## 8. Compatibility

### Version Routing

V3 does **not** detect version by reading file Magic Number at load time. Version routing is entirely caller-side:

1. **Build side**: `ScalarIndexCreator::Upload()` checks `SCALAR_INDEX_ENGINE_VERSION` from config. If `>= 3`, calls `index->UploadV3(config)`; otherwise calls `index->Upload(config)`.
2. **Load side**: `SealedIndexTranslator` checks `scalar_index_engine_version` from config. If `>= 3` and not vector type, calls `index->LoadV3(config)`; otherwise calls `index->Load(ctx, config)`.
3. **Go control plane**: `CurrentScalarIndexEngineVersion` in `pkg/common/common.go` (currently `2`). Bump to `3` to enable V3 for new index builds.
4. **UT routing**: `kScalarIndexUseV3` global bool (default `false`). When set to `true`, individual index `Upload()` and `Load()` methods internally redirect to `UploadV3()`/`LoadV3()`. This flag is for unit testing only; production routing uses the caller-side config.

### File Naming

V3 packed file name format: `milvus_packed_<type>_index.v3`, where `<type>` is the lowercased `ScalarIndexType` string (e.g., `stlsort`, `bitmap`, `marisa`, `inverted`, `hybrid`, `rtree`, `ngram`).

### Remote Paths

V3 keeps V2 remote root directory classification unchanged:

| Root | Index Type | V3 Path |
|------|-----------|---------|
| `index_files/` | Normal scalar indexes, InvertedIndexTantivy, etc. | `.../milvus_packed_<type>_index.v3` |
| `text_log/` | TextMatchIndex | `.../milvus_packed_<type>_index.v3` |

`FileManagerImpl` has `OpenOutputStream(filename, bool is_index_file)` and `CreateIndexEntryWriterV3(filename, bool is_index_file)`. Default `is_index_file=true`. TextMatchIndex sets `is_index_file_ = false` on its `ScalarIndex` base.

### Backward Compatibility

- V2 `Upload()` and `Load()` methods retained, not deleted.
- Rollback: set `CurrentScalarIndexEngineVersion` back to `2`. No code change needed.

---

## 9. Open Questions

### 9.1 Why not encode the index into existing file format, like Lance?

Lance is a columnar storage format that stores data in a columnar format. Any single Lance file has its own schema, and multiple Lance files form a table format.

In the end of the day, Lance still follows a tabular format design, while using a columnar storage.

#### How does Lance store an index?

In most cases, indexes would have their own specialized data structure, algorithm, etc. Those are highly optimized for the index type, and may not fit easily into a tabular format.

Lance addresses this by storing indexes in **separate Lance files** (typically `index.idx` + `auxiliary.idx`) that are still columnar under the hood. These files use Lance's own Arrow-based schema tailored to the index type:

- For vector indexes (e.g. IVF_PQ), the index file stores graph/structure (e.g. IVF partitions, HNSW neighbors), while the auxiliary file stores quantized vector codes, both organized as columns with `_rowid`, codes, distances, etc.
- Even scalar indexes (bitmap, BTree-like) are encoded into columnar pages with metadata in Protobuf.

This approach works well for Lance's ecosystem — it reuses the same file format, zero-copy reads, and manifest versioning. However, it comes with trade-offs:

- **Forced columnar mapping** — arbitrary graph structures, tree nodes, hash tables, or custom layouts must be “flattened” into columns (e.g. lists/arrays), which can introduce overhead in encoding, padding, or indirection.
- **Schema rigidity** — every index type needs a custom Arrow schema + Protobuf metadata, limiting flexibility if you want to support diverse or evolving index algorithms.
- **Not truly generic** — third-party indexes (e.g. modified Faiss, custom implementations) are hard to import directly without re-implementation in Lance's layout.

For a **generic scalar index format** that aims to support many different scalar index types (bitmap, trie, inverted list, RTree, etc.) with minimal adaptation cost, a **pure KV style** (key → serialized index blob + metadata) avoids these constraints entirely.

It trades some of Lance's columnar optimizations for maximum flexibility and easier integration with existing index libraries.
