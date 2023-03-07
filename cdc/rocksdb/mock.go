package gorocksdb

import (
	"unsafe"
)

type Iterator struct {
}

func NewNativeIterator(c unsafe.Pointer) *Iterator {
	return nil
}
func (iter *Iterator) Valid() bool {
	return false
}
func (iter *Iterator) ValidForPrefix(prefix []byte) bool {
	return false
}
func (iter *Iterator) Key() *Slice {
	return nil
}
func (iter *Iterator) Value() *Slice {
	return nil
}
func (iter *Iterator) Next() {
}
func (iter *Iterator) Prev() {
}
func (iter *Iterator) SeekToFirst() {
}
func (iter *Iterator) SeekToLast() {
}
func (iter *Iterator) Seek(_ []byte) {
}
func (iter *Iterator) SeekForPrev(_ []byte) {
}
func (iter *Iterator) Err() error {
	return nil
}
func (iter *Iterator) Close() {
}

type Slice struct {
}
type Slices []*Slice

func (slices Slices) Destroy() {
}
func (s *Slice) Data() []byte {
	return nil
}
func (s *Slice) Size() int {
	return 0
}
func (s *Slice) Exists() bool {
	return false
}
func (s *Slice) Free() {
}

type DB struct {
}

func OpenDb(opts *Options, name string) (*DB, error) {
	return nil, nil
}
func OpenDbWithTTL(opts *Options, name string, ttl int) (*DB, error) {
	return nil, nil
}
func OpenDbForReadOnly(opts *Options, name string, errorIfLogFileExist bool) (*DB, error) {
	return nil, nil
}
func OpenDbColumnFamilies(
	opts *Options,
	name string,
	cfNames []string,
	cfOpts []*Options,
) (*DB, []*ColumnFamilyHandle, error) {
	return nil, nil, nil
}
func OpenDbForReadOnlyColumnFamilies(
	opts *Options,
	name string,
	cfNames []string,
	cfOpts []*Options,
	errorIfLogFileExist bool,
) (*DB, []*ColumnFamilyHandle, error) {
	return nil, nil, nil
}
func ListColumnFamilies(opts *Options, name string) ([]string, error) {
	return nil, nil
}
func (db *DB) UnsafeGetDB() unsafe.Pointer {
	return nil
}
func (db *DB) Name() string {
	return ""
}
func (db *DB) Get(opts *ReadOptions, key []byte) (*Slice, error) {
	return nil, nil
}
func (db *DB) GetBytes(opts *ReadOptions, key []byte) ([]byte, error) {
	return nil, nil
}
func (db *DB) GetCF(opts *ReadOptions, cf *ColumnFamilyHandle, key []byte) (*Slice, error) {
	return nil, nil
}
func (db *DB) GetPinned(opts *ReadOptions, key []byte) (*PinnableSliceHandle, error) {
	return nil, nil
}
func (db *DB) MultiGet(opts *ReadOptions, keys ...[]byte) (Slices, error) {
	return nil, nil
}
func (db *DB) MultiGetCF(opts *ReadOptions, cf *ColumnFamilyHandle, keys ...[]byte) (Slices, error) {
	return nil, nil
}
func (db *DB) MultiGetCFMultiCF(opts *ReadOptions, cfs ColumnFamilyHandles, keys [][]byte) (Slices, error) {
	return nil, nil
}
func (db *DB) Put(opts *WriteOptions, key, value []byte) error {
	return nil
}
func (db *DB) PutCF(opts *WriteOptions, cf *ColumnFamilyHandle, key, value []byte) error {
	return nil
}
func (db *DB) Delete(opts *WriteOptions, key []byte) error {
	return nil
}
func (db *DB) DeleteCF(opts *WriteOptions, cf *ColumnFamilyHandle, key []byte) error {
	return nil
}
func (db *DB) Merge(opts *WriteOptions, key []byte, value []byte) error {
	return nil
}
func (db *DB) MergeCF(opts *WriteOptions, cf *ColumnFamilyHandle, key []byte, value []byte) error {
	return nil
}
func (db *DB) Write(opts *WriteOptions, batch *WriteBatch) error {
	return nil
}
func (db *DB) NewIterator(opts *ReadOptions) *Iterator {
	return nil
}
func (db *DB) NewIteratorCF(opts *ReadOptions, cf *ColumnFamilyHandle) *Iterator {
	return nil
}
func (db *DB) GetUpdatesSince(seqNumber uint64) (*WalIterator, error) {
	return nil, nil
}
func (db *DB) GetLatestSequenceNumber() uint64 {
	return 0
}
func (db *DB) NewSnapshot() *Snapshot {
	return nil
}
func (db *DB) ReleaseSnapshot(snapshot *Snapshot) {
}
func (db *DB) GetProperty(propName string) string {
	return ""
}
func (db *DB) GetPropertyCF(propName string, cf *ColumnFamilyHandle) string {
	return ""
}
func (db *DB) CreateColumnFamily(opts *Options, name string) (*ColumnFamilyHandle, error) {
	return nil, nil
}
func (db *DB) DropColumnFamily(c *ColumnFamilyHandle) error {
	return nil
}
func (db *DB) GetApproximateSizes(ranges []Range) []uint64 {
	return nil
}
func (db *DB) GetApproximateSizesCF(cf *ColumnFamilyHandle, ranges []Range) []uint64 {
	return nil
}
func (db *DB) SetOptions(keys, values []string) error {
	return nil
}

type LiveFileMetadata struct {
}

func (db *DB) GetLiveFilesMetaData() []LiveFileMetadata {
	return nil
}
func (db *DB) CompactRange(r Range) {
}
func (db *DB) CompactRangeCF(cf *ColumnFamilyHandle, r Range) {
}
func (db *DB) Flush(opts *FlushOptions) error {
	return nil
}
func (db *DB) DisableFileDeletions() error {
	return nil
}
func (db *DB) EnableFileDeletions(force bool) error {
	return nil
}
func (db *DB) DeleteFile(name string) {
}
func (db *DB) DeleteFileInRange(r Range) error {
	return nil
}
func (db *DB) DeleteFileInRangeCF(cf *ColumnFamilyHandle, r Range) error {
	return nil
}
func (db *DB) IngestExternalFile(filePaths []string, opts *IngestExternalFileOptions) error {
	return nil
}
func (db *DB) IngestExternalFileCF(handle *ColumnFamilyHandle, filePaths []string, opts *IngestExternalFileOptions) error {
	return nil
}
func (db *DB) NewCheckpoint() (*Checkpoint, error) {
	return nil, nil
}
func (db *DB) Close() {
}
func DestroyDb(name string, opts *Options) error {
	return nil
}
func RepairDb(name string, opts *Options) error {
	return nil
}

type Options struct {
}

func NewDefaultOptions() *Options {
	return nil
}
func GetOptionsFromString(base *Options, optStr string) (*Options, error) {
	return nil, nil
}
func (opts *Options) SetCompactionFilter(value CompactionFilter) {
}
func (opts *Options) SetComparator(value Comparator) {
}
func (opts *Options) SetMergeOperator(value MergeOperator) {
}
func (opts *Options) SetCreateIfMissing(value bool) {
}
func (opts *Options) SetErrorIfExists(value bool) {
}
func (opts *Options) SetParanoidChecks(value bool) {
}
func (opts *Options) SetDBPaths(dbpaths []*DBPath) {
}
func (opts *Options) SetEnv(value *Env) {
}
func (opts *Options) SetInfoLogLevel(value InfoLogLevel) {
}
func (opts *Options) IncreaseParallelism(total_threads int) {
}
func (opts *Options) OptimizeForPointLookup(block_cache_size_mb uint64) {
}
func (opts *Options) SetAllowConcurrentMemtableWrites(allow bool) {
}
func (opts *Options) OptimizeLevelStyleCompaction(memtable_memory_budget uint64) {
}
func (opts *Options) OptimizeUniversalStyleCompaction(memtable_memory_budget uint64) {
}
func (opts *Options) SetWriteBufferSize(value int) {
}
func (opts *Options) SetMaxWriteBufferNumber(value int) {
}
func (opts *Options) SetMinWriteBufferNumberToMerge(value int) {
}
func (opts *Options) SetMaxOpenFiles(value int) {
}
func (opts *Options) SetMaxFileOpeningThreads(value int) {
}
func (opts *Options) SetMaxTotalWalSize(value uint64) {
}
func (opts *Options) SetCompression(value CompressionType) {
}
func (opts *Options) SetCompressionPerLevel(value []CompressionType) {
}
func (opts *Options) SetMinLevelToCompress(value int) {
}
func (opts *Options) SetCompressionOptions(value *CompressionOptions) {
}
func (opts *Options) SetPrefixExtractor(value SliceTransform) {
}
func (opts *Options) SetNumLevels(value int) {
}
func (opts *Options) SetLevel0FileNumCompactionTrigger(value int) {
}
func (opts *Options) SetLevel0SlowdownWritesTrigger(value int) {
}
func (opts *Options) SetLevel0StopWritesTrigger(value int) {
}
func (opts *Options) SetMaxMemCompactionLevel(value int) {
}
func (opts *Options) SetTargetFileSizeBase(value uint64) {
}
func (opts *Options) SetTargetFileSizeMultiplier(value int) {
}
func (opts *Options) SetMaxBytesForLevelBase(value uint64) {
}
func (opts *Options) SetMaxBytesForLevelMultiplier(value float64) {
}
func (opts *Options) SetLevelCompactionDynamicLevelBytes(value bool) {
}
func (opts *Options) SetMaxCompactionBytes(value uint64) {
}
func (opts *Options) SetSoftPendingCompactionBytesLimit(value uint64) {
}
func (opts *Options) SetHardPendingCompactionBytesLimit(value uint64) {
}
func (opts *Options) SetMaxBytesForLevelMultiplierAdditional(value []int) {
}
func (opts *Options) SetUseFsync(value bool) {
}
func (opts *Options) SetDbLogDir(value string) {
}
func (opts *Options) SetWalDir(value string) {
}
func (opts *Options) SetDeleteObsoleteFilesPeriodMicros(value uint64) {
}
func (opts *Options) SetMaxBackgroundCompactions(value int) {
}
func (opts *Options) SetMaxBackgroundFlushes(value int) {
}
func (opts *Options) SetMaxLogFileSize(value int) {
}
func (opts *Options) SetLogFileTimeToRoll(value int) {
}
func (opts *Options) SetKeepLogFileNum(value int) {
}
func (opts *Options) SetSoftRateLimit(value float64) {
}
func (opts *Options) SetHardRateLimit(value float64) {
}
func (opts *Options) SetRateLimitDelayMaxMilliseconds(value uint) {
}
func (opts *Options) SetMaxManifestFileSize(value uint64) {
}
func (opts *Options) SetTableCacheNumshardbits(value int) {
}
func (opts *Options) SetTableCacheRemoveScanCountLimit(value int) {
}
func (opts *Options) SetArenaBlockSize(value int) {
}
func (opts *Options) SetDisableAutoCompactions(value bool) {
}
func (opts *Options) SetWALRecoveryMode(mode WALRecoveryMode) {
}
func (opts *Options) SetWALTtlSeconds(value uint64) {
}
func (opts *Options) SetWalSizeLimitMb(value uint64) {
}
func (opts *Options) SetEnablePipelinedWrite(value bool) {
}
func (opts *Options) SetManifestPreallocationSize(value int) {
}
func (opts *Options) SetPurgeRedundantKvsWhileFlush(value bool) {
}
func (opts *Options) SetAllowMmapReads(value bool) {
}
func (opts *Options) SetAllowMmapWrites(value bool) {
}
func (opts *Options) SetUseDirectReads(value bool) {
}
func (opts *Options) SetUseDirectIOForFlushAndCompaction(value bool) {
}
func (opts *Options) SetIsFdCloseOnExec(value bool) {
}
func (opts *Options) SetSkipLogErrorOnRecovery(value bool) {
}
func (opts *Options) SetStatsDumpPeriodSec(value uint) {
}
func (opts *Options) SetAdviseRandomOnOpen(value bool) {
}
func (opts *Options) SetDbWriteBufferSize(value int) {
}
func (opts *Options) SetAccessHintOnCompactionStart(value CompactionAccessPattern) {
}
func (opts *Options) SetUseAdaptiveMutex(value bool) {
}
func (opts *Options) SetBytesPerSync(value uint64) {
}
func (opts *Options) SetCompactionStyle(value CompactionStyle) {
}
func (opts *Options) SetUniversalCompactionOptions(value *UniversalCompactionOptions) {
}
func (opts *Options) SetFIFOCompactionOptions(value *FIFOCompactionOptions) {
}
func (opts *Options) GetStatisticsString() string {
	return ""
}
func (opts *Options) SetRateLimiter(rateLimiter *RateLimiter) {
}
func (opts *Options) SetMaxSequentialSkipInIterations(_ uint64) {
}
func (opts *Options) SetInplaceUpdateSupport(_ bool) {
}
func (opts *Options) SetInplaceUpdateNumLocks(_ int) {
}
func (opts *Options) SetMemtableHugePageSize(_ int) {
}
func (opts *Options) SetBloomLocality(_ uint32) {
}
func (opts *Options) SetMaxSuccessiveMerges(_ int) {
}
func (opts *Options) EnableStatistics() {
}
func (opts *Options) PrepareForBulkLoad() {
}
func (opts *Options) SetMemtableVectorRep() {
}
func (opts *Options) SetHashSkipListRep(_ int, _, _ int32) {
}
func (opts *Options) SetHashLinkListRep(_ int) {
}
func (opts *Options) SetPlainTableFactory(_ uint32, _ int, _ float64, _ int) {
}
func (opts *Options) SetCreateIfMissingColumnFamilies(_ bool) {
}
func (opts *Options) SetBlockBasedTableFactory(_ *BlockBasedTableOptions) {
}
func (opts *Options) SetAllowIngestBehind(_ bool) {
}
func (opts *Options) SetMemTablePrefixBloomSizeRatio(_ float64) {
}
func (opts *Options) SetOptimizeFiltersForHits(_ bool) {
}
func (opts *Options) Destroy() {
}

type BlockBasedTableOptions struct {
}

func NewDefaultBlockBasedTableOptions() *BlockBasedTableOptions {
	return nil
}
func (opts *BlockBasedTableOptions) Destroy() {
}
func (opts *BlockBasedTableOptions) SetCacheIndexAndFilterBlocks(_ bool) {
}
func (opts *BlockBasedTableOptions) SetCacheIndexAndFilterBlocksWithHighPriority(_ bool) {
}
func (opts *BlockBasedTableOptions) SetPinL0FilterAndIndexBlocksInCache(_ bool) {
}
func (opts *BlockBasedTableOptions) SetPinTopLevelIndexAndFilter(_ bool) {
}
func (opts *BlockBasedTableOptions) SetBlockSize(_ int) {
}
func (opts *BlockBasedTableOptions) SetBlockSizeDeviation(_ int) {
}
func (opts *BlockBasedTableOptions) SetBlockRestartInterval(_ int) {
}
func (opts *BlockBasedTableOptions) SetIndexBlockRestartInterval(_ int) {
}
func (opts *BlockBasedTableOptions) SetMetadataBlockSize(_ uint64) {
}
func (opts *BlockBasedTableOptions) SetPartitionFilters(_ bool) {
}
func (opts *BlockBasedTableOptions) SetUseDeltaEncoding(_ bool) {
}
func (opts *BlockBasedTableOptions) SetFilterPolicy(_ FilterPolicy) {
}
func (opts *BlockBasedTableOptions) SetNoBlockCache(_ bool) {
}
func (opts *BlockBasedTableOptions) SetBlockCache(_ *Cache) {
}
func (opts *BlockBasedTableOptions) SetBlockCacheCompressed(cache *Cache) {
}
func (opts *BlockBasedTableOptions) SetWholeKeyFiltering(_ bool) {
}
func (opts *BlockBasedTableOptions) SetFormatVersion(_ int) {
}
func (opts *BlockBasedTableOptions) SetIndexType(_ IndexType) {
}

type IndexType uint

const (
	KBinarySearchIndexType        = 0
	KHashSearchIndexType          = 1
	KTwoLevelIndexSearchIndexType = 2
)

type Cache struct {
}

func NewLRUCache(capacity uint64) *Cache {
	return nil
}
func (c *Cache) GetUsage() uint64 {
	return 0
}
func (c *Cache) GetPinnedUsage() uint64 {
	return 0
}
func (c *Cache) Destroy() {
}

type FilterPolicy interface {
	CreateFilter(keys [][]byte) []byte
	KeyMayMatch(key []byte, filter []byte) bool
	Name() string
}
type RateLimiter struct {
}

func NewRateLimiter(_, _ int64, fairness int32) *RateLimiter {
	return nil
}
func (self *RateLimiter) Destroy() {
}

type UniversalCompactionStopStyle uint

const (
	CompactionStopStyleSimilarSize = UniversalCompactionStopStyle(1)
	CompactionStopStyleTotalSize   = UniversalCompactionStopStyle(2)
)

type FIFOCompactionOptions struct {
}

func NewDefaultFIFOCompactionOptions() *FIFOCompactionOptions {
	return nil
}
func (opts *FIFOCompactionOptions) SetMaxTableFilesSize(value uint64) {
}
func (opts *FIFOCompactionOptions) Destroy() {
}

type UniversalCompactionOptions struct {
}

func NewDefaultUniversalCompactionOptions() *UniversalCompactionOptions {
	return nil
}
func (opts *UniversalCompactionOptions) SetSizeRatio(value uint) {
}
func (opts *UniversalCompactionOptions) SetMinMergeWidth(value uint) {
}
func (opts *UniversalCompactionOptions) SetMaxMergeWidth(value uint) {
}
func (opts *UniversalCompactionOptions) SetMaxSizeAmplificationPercent(value uint) {
}
func (opts *UniversalCompactionOptions) SetCompressionSizePercent(_ int) {
}
func (opts *UniversalCompactionOptions) SetStopStyle(_ UniversalCompactionStopStyle) {
}
func (opts *UniversalCompactionOptions) Destroy() {
}

type CompressionType uint

const (
	NoCompression     = CompressionType(0)
	SnappyCompression = CompressionType(1)
	ZLibCompression   = CompressionType(2)
	Bz2Compression    = CompressionType(3)
	LZ4Compression    = CompressionType(4)
	LZ4HCCompression  = CompressionType(5)
	XpressCompression = CompressionType(6)
	ZSTDCompression   = CompressionType(7)
)

type CompactionStyle uint

const (
	LevelCompactionStyle     = CompactionStyle(0)
	UniversalCompactionStyle = CompactionStyle(1)
	FIFOCompactionStyle      = CompactionStyle(2)
)

type CompactionAccessPattern uint

const (
	NoneCompactionAccessPattern       = CompactionAccessPattern(0)
	NormalCompactionAccessPattern     = CompactionAccessPattern(1)
	SequentialCompactionAccessPattern = CompactionAccessPattern(2)
	WillneedCompactionAccessPattern   = CompactionAccessPattern(3)
)

type InfoLogLevel uint

const (
	DebugInfoLogLevel = InfoLogLevel(0)
	InfoInfoLogLevel  = InfoLogLevel(1)
	WarnInfoLogLevel  = InfoLogLevel(2)
	ErrorInfoLogLevel = InfoLogLevel(3)
	FatalInfoLogLevel = InfoLogLevel(4)
)

type WALRecoveryMode int

const (
	TolerateCorruptedTailRecordsRecovery = WALRecoveryMode(0)
	AbsoluteConsistencyRecovery          = WALRecoveryMode(1)
	PointInTimeRecovery                  = WALRecoveryMode(2)
	SkipAnyCorruptedRecordsRecovery      = WALRecoveryMode(3)
)

type SliceTransform interface {
	Transform(src []byte) []byte
	InDomain(src []byte) bool
	InRange(src []byte) bool
	Name() string
}

func NewFixedPrefixTransform(prefixLen int) SliceTransform {
	return nil
}
func NewNoopPrefixTransform() SliceTransform {
	return nil
}

type CompressionOptions struct {
	WindowBits   int
	Level        int
	Strategy     int
	MaxDictBytes int
}

func NewDefaultCompressionOptions() *CompressionOptions {
	return nil
}
func NewCompressionOptions(windowBits, level, strategy, maxDictBytes int) *CompressionOptions {
	return nil
}

type Env struct {
}

func NewDefaultEnv() *Env {
	return nil
}
func NewMemEnv() *Env {
	return nil
}
func (env *Env) SetBackgroundThreads(n int) {
}
func (env *Env) SetHighPriorityBackgroundThreads(n int) {
}
func (env *Env) Destroy() {
}

type DBPath struct {
}

func NewDBPath(path string, target_size uint64) *DBPath {
	return nil
}
func (dbpath *DBPath) Destroy() {
}
func NewDBPathsFromData(paths []string, target_sizes []uint64) []*DBPath {
	return nil
}
func DestroyDBPaths(dbpaths []*DBPath) {
}

type MergeOperator interface {
	FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool)
	Name() string
}
type PartialMerger interface {
	PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool)
}
type MultiMerger interface {
	PartialMergeMulti(key []byte, operands [][]byte) ([]byte, bool)
}
type Comparator interface {
	Compare(a, b []byte) int
	Name() string
}
type CompactionFilter interface {
	Filter(level int, key, val []byte) (remove bool, newVal []byte)
	Name() string
}
type Checkpoint struct {
}

func (checkpoint *Checkpoint) CreateCheckpoint(checkpoint_dir string, log_size_for_flush uint64) error {
	return nil
}
func (checkpoint *Checkpoint) Destroy() {
}

type ColumnFamilyHandle struct {
}

func (h *ColumnFamilyHandle) UnsafeGetCFHandler() unsafe.Pointer {
	return nil
}
func (h *ColumnFamilyHandle) Destroy() {
}

type ColumnFamilyHandles []*ColumnFamilyHandle
type IngestExternalFileOptions struct {
}

func NewDefaultIngestExternalFileOptions() *IngestExternalFileOptions {
	return nil
}
func (opts *IngestExternalFileOptions) SetMoveFiles(flag bool) {
}
func (opts *IngestExternalFileOptions) SetSnapshotConsistency(flag bool) {
}
func (opts *IngestExternalFileOptions) SetAllowGlobalSeqNo(flag bool) {
}
func (opts *IngestExternalFileOptions) SetAllowBlockingFlush(flag bool) {
}
func (opts *IngestExternalFileOptions) SetIngestionBehind(flag bool) {
}
func (opts *IngestExternalFileOptions) Destroy() {
}

type Range struct {
	Start []byte
	Limit []byte
}
type FlushOptions struct {
}

func NewDefaultFlushOptions() *FlushOptions {
	return nil
}
func (opts *FlushOptions) SetWait(value bool) {
}
func (opts *FlushOptions) Destroy() {
}

type Snapshot struct {
}
type WalIterator struct {
}

func NewNativeWalIterator(c unsafe.Pointer) *WalIterator {
	return nil
}
func (iter *WalIterator) Valid() bool {
	return false
}
func (iter *WalIterator) Next() {
}
func (iter *WalIterator) Err() error {
	return nil
}
func (iter *WalIterator) Destroy() {
}
func (iter *WalIterator) GetBatch() (*WriteBatch, uint64) {
	return nil, 0
}

type WriteBatch struct {
}

func NewWriteBatch() *WriteBatch {
	return nil
}
func WriteBatchFrom(data []byte) *WriteBatch {
	return nil
}
func (wb *WriteBatch) Put(key, value []byte) {
}
func (wb *WriteBatch) PutCF(cf *ColumnFamilyHandle, key, value []byte) {
}
func (wb *WriteBatch) PutLogData(blob []byte) {
}
func (wb *WriteBatch) Merge(key, value []byte) {
}
func (wb *WriteBatch) MergeCF(cf *ColumnFamilyHandle, key, value []byte) {
}
func (wb *WriteBatch) Delete(key []byte) {
}
func (wb *WriteBatch) DeleteCF(cf *ColumnFamilyHandle, key []byte) {
}
func (wb *WriteBatch) DeleteRange(startKey []byte, endKey []byte) {
}
func (wb *WriteBatch) DeleteRangeCF(cf *ColumnFamilyHandle, startKey []byte, endKey []byte) {
}
func (wb *WriteBatch) Data() []byte {
	return nil
}
func (wb *WriteBatch) Count() int {
	return 0
}
func (wb *WriteBatch) NewIterator() *WriteBatchIterator {
	return nil
}
func (wb *WriteBatch) Clear() {
}
func (wb *WriteBatch) Destroy() {
}

type WriteBatchRecordType byte

const (
	WriteBatchDeletionRecord                 WriteBatchRecordType = 0x0
	WriteBatchValueRecord                    WriteBatchRecordType = 0x1
	WriteBatchMergeRecord                    WriteBatchRecordType = 0x2
	WriteBatchLogDataRecord                  WriteBatchRecordType = 0x3
	WriteBatchCFDeletionRecord               WriteBatchRecordType = 0x4
	WriteBatchCFValueRecord                  WriteBatchRecordType = 0x5
	WriteBatchCFMergeRecord                  WriteBatchRecordType = 0x6
	WriteBatchSingleDeletionRecord           WriteBatchRecordType = 0x7
	WriteBatchCFSingleDeletionRecord         WriteBatchRecordType = 0x8
	WriteBatchBeginPrepareXIDRecord          WriteBatchRecordType = 0x9
	WriteBatchEndPrepareXIDRecord            WriteBatchRecordType = 0xA
	WriteBatchCommitXIDRecord                WriteBatchRecordType = 0xB
	WriteBatchRollbackXIDRecord              WriteBatchRecordType = 0xC
	WriteBatchNoopRecord                     WriteBatchRecordType = 0xD
	WriteBatchRangeDeletion                  WriteBatchRecordType = 0xF
	WriteBatchCFRangeDeletion                WriteBatchRecordType = 0xE
	WriteBatchCFBlobIndex                    WriteBatchRecordType = 0x10
	WriteBatchBlobIndex                      WriteBatchRecordType = 0x11
	WriteBatchBeginPersistedPrepareXIDRecord WriteBatchRecordType = 0x12
	WriteBatchNotUsedRecord                  WriteBatchRecordType = 0x7F
)

type WriteBatchRecord struct {
}
type WriteBatchIterator struct {
}

func (iter *WriteBatchIterator) Next() bool {
	return false
}
func (iter *WriteBatchIterator) Record() *WriteBatchRecord {
	return nil
}
func (iter *WriteBatchIterator) Error() error {
	return nil
}
func (iter *WriteBatchIterator) decodeSlice() []byte {
	return nil
}
func (iter *WriteBatchIterator) decodeRecType() WriteBatchRecordType {
	return WriteBatchRecordType(0)
}
func (iter *WriteBatchIterator) decodeVarint() uint64 {
	return 0
}

type ReadTier uint

const (
	ReadAllTier    = ReadTier(0)
	BlockCacheTier = ReadTier(1)
)

type ReadOptions struct {
}

func NewDefaultReadOptions() *ReadOptions {
	return nil
}
func (opts *ReadOptions) UnsafeGetReadOptions() unsafe.Pointer {
	return nil
}
func (opts *ReadOptions) SetVerifyChecksums(value bool) {
}
func (opts *ReadOptions) SetPrefixSameAsStart(value bool) {
}
func (opts *ReadOptions) SetFillCache(value bool) {
}
func (opts *ReadOptions) SetSnapshot(snap *Snapshot) {
}
func (opts *ReadOptions) SetReadTier(value ReadTier) {
}
func (opts *ReadOptions) SetTailing(value bool) {
}
func (opts *ReadOptions) SetIterateUpperBound(key []byte) {
}
func (opts *ReadOptions) SetPinData(value bool) {
}
func (opts *ReadOptions) SetReadaheadSize(value uint64) {
}
func (opts *ReadOptions) Destroy() {
}

type WriteOptions struct {
}

func NewDefaultWriteOptions() *WriteOptions {
	return nil
}
func (opts *WriteOptions) SetSync(value bool) {
}
func (opts *WriteOptions) DisableWAL(value bool) {
}
func (opts *WriteOptions) Destroy() {
}

type PinnableSliceHandle struct {
}

func (h *PinnableSliceHandle) Data() []byte {
	return nil
}
func (h *PinnableSliceHandle) Destroy() {
}
