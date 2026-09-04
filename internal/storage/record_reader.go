package storage

import (
	"errors"
	"io"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type RecordReader interface {
	// Next returns a record borrowed from the reader and valid until the next
	// Next or Close. Callers retaining it longer must Retain and Release it.
	Next() (Record, error)
	Close() error
}

type packedRecordReader struct {
	reader    *packed.PackedReader
	field2Col map[FieldID]int
}

var _ RecordReader = (*packedRecordReader)(nil)

type ffiPackedRecordReader struct {
	reader    *packed.FFIPackedReader
	field2Col map[FieldID]int
}

var _ RecordReader = (*ffiPackedRecordReader)(nil)

func (pr *packedRecordReader) Next() (Record, error) {
	rec, err := pr.reader.ReadNext()
	if err != nil {
		return nil, err
	}
	return NewSimpleArrowRecord(rec, pr.field2Col), nil
}

func (pr *packedRecordReader) Close() error {
	if pr == nil || pr.reader == nil {
		return nil
	}
	return pr.reader.Close()
}

func (pr *ffiPackedRecordReader) Next() (Record, error) {
	rec, err := pr.reader.ReadNext()
	if err != nil {
		return nil, err
	}
	return NewSimpleArrowRecord(rec, pr.field2Col), nil
}

func (pr *ffiPackedRecordReader) Close() error {
	if pr == nil || pr.reader == nil {
		return nil
	}
	return pr.reader.Close()
}

func newPackedRecordReader(
	paths []string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	externalReader packed.ExternalReaderContext,
) (*packedRecordReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "convert collection schema [%s] to arrow schema", schema.Name)
	}
	field2Col := make(map[FieldID]int)
	allFields := typeutil.GetAllFieldSchemas(schema)
	for i, field := range allFields {
		field2Col[field.FieldID] = i
	}
	reader, err := packed.NewPackedReaderWithExtfs(paths, arrowSchema, bufferSize, storageConfig, storagePluginContext, externalReader)
	if err != nil {
		return nil, err
	}
	return &packedRecordReader{
		reader:    reader,
		field2Col: field2Col,
	}, nil
}

func newFFIPackedRecordReaderFromFragments(
	fragments []packed.Fragment,
	format string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	externalReader packed.ExternalReaderContext,
) (*ffiPackedRecordReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}
	field2Col := make(map[FieldID]int)
	allFields := typeutil.GetAllFieldSchemas(schema)
	columns := make([]string, 0, len(allFields))
	for i, field := range allFields {
		field2Col[field.FieldID] = i
		columns = append(columns, strconv.FormatInt(field.FieldID, 10))
	}
	reader, err := packed.NewFFIPackedReaderWithFragments(
		columns,
		format,
		fragments,
		arrowSchema,
		columns,
		bufferSize,
		storageConfig,
		storagePluginContext,
		externalReader,
	)
	if err != nil {
		return nil, err
	}
	return &ffiPackedRecordReader{
		reader:    reader,
		field2Col: field2Col,
	}, nil
}

func NewRecordReaderFromManifest(manifest string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	option ...RwOption,
) (RecordReader, error) {
	return NewManifestReader(manifest, schema, bufferSize, storageConfig, storagePluginContext, option...)
}

var _ RecordReader = (*IterativeRecordReader)(nil)

// prefetchedChunk is the outcome of opening one chunk ahead of time. It
// carries the chunk's first record as well as its reader: for storage v2 a chunk
// is one binlog file whose column groups are fetched by that first Next(), so
// opening alone would overlap only the footer read, not the payload download.
type prefetchedChunk struct {
	reader RecordReader
	rec    Record
	err    error
	// exhausted distinguishes "iterate() says there is no further chunk" from
	// "this chunk turned out to be empty", which are both io.EOF at the source
	// but mean different things to the consumer.
	exhausted bool
}

type IterativeRecordReader struct {
	cur     RecordReader
	iterate func() (RecordReader, error)

	// window is the number of chunks that may be open at once, the one being
	// consumed included. With window <= 1 chunks are opened strictly one after
	// another (nextSerial). With a larger window the object-storage fetch of
	// up to window-1 further chunks overlaps the caller's processing of the
	// current one. Chunks are still delivered in order: for storage v2 the
	// first Next() on a chunk pulls its whole payload, so keeping the fetches
	// concurrent is what matters, not the delivery order.
	window int

	// queue hands the per-chunk result channels from the producer to the
	// consumer in iteration order. Its capacity (window-1) is the back
	// pressure: the producer reserves a slot by enqueueing the channel before
	// it opens the chunk, so it can never run more than window-1 chunks ahead.
	queue chan chan *prefetchedChunk
	// stop tells the producer to quit; done is closed once it has.
	stop chan struct{}
	done chan struct{}
	// started records that the producer has been launched; failed remembers a
	// terminal error so later Next calls repeat it instead of reporting EOF.
	started bool
	failed  error
	drained bool
}

// Close implements RecordReader.
func (ir *IterativeRecordReader) Close() error {
	var firstErr error
	if ir.started {
		// Stop the producer, wait for it, then close every reader it managed
		// to open: each channel it enqueued is guaranteed to be filled.
		close(ir.stop)
		<-ir.done
		for {
			var ch chan *prefetchedChunk
			select {
			case ch = <-ir.queue:
			default:
			}
			if ch == nil {
				break
			}
			p := <-ch
			if p.reader != nil {
				if err := p.reader.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
		ir.started = false
	}
	// A closed reader stays closed: later Next calls report EOF instead of
	// restarting the producer and opening chunks nobody will consume.
	ir.drained = true
	if ir.cur != nil {
		if err := ir.cur.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		ir.cur = nil
	}
	return firstErr
}

// start launches the producer that walks iterate() and opens chunks ahead of
// the consumer. iterate() is only ever called from this one goroutine, which
// keeps the closure's chunk cursor free of races; the first Next() of each
// chunk, i.e. its payload download, runs in a goroutine of its own so that up
// to window-1 downloads are in flight at the same time.
func (ir *IterativeRecordReader) start() {
	if ir.started {
		return
	}
	ir.started = true
	ir.queue = make(chan chan *prefetchedChunk, ir.window-1)
	ir.stop = make(chan struct{})
	ir.done = make(chan struct{})
	iterate, queue, stop, done := ir.iterate, ir.queue, ir.stop, ir.done
	go func() {
		defer close(done)
		for {
			ch := make(chan *prefetchedChunk, 1)
			// Reserve the slot first: once ch is queued the consumer relies
			// on it being filled, so everything after this point must send.
			select {
			case queue <- ch:
			case <-stop:
				return
			}
			p := &prefetchedChunk{}
			r, err := func() (r RecordReader, err error) {
				defer func() {
					if x := recover(); x != nil {
						err = merr.WrapErrServiceInternalMsg("internal error recovered: %v", x)
					}
				}()
				return iterate()
			}()
			if err != nil {
				if errors.Is(err, io.EOF) {
					p.exhausted = true
				} else {
					// iterate() may hand back a typed-nil reader alongside
					// the error (see the comment in nextSerial); do not keep
					// it, Close() must never dereference it.
					p.err = err
				}
				ch <- p
				return
			}
			p.reader = r
			go func() {
				defer func() {
					if x := recover(); x != nil {
						// Keep p.reader: the consumer still has to close it.
						p.rec = nil
						p.err = merr.WrapErrServiceInternalMsg("internal error recovered: %v", x)
					}
					ch <- p
				}()
				p.rec, p.err = r.Next()
			}()
		}
	}()
}

// nextChunk installs the next non-empty chunk and returns its first record.
func (ir *IterativeRecordReader) nextChunk() (Record, error) {
	if ir.failed != nil {
		return nil, ir.failed
	}
	if ir.drained {
		return nil, io.EOF
	}
	ir.start()
	for {
		p := <-<-ir.queue
		if p.exhausted {
			ir.drained = true
			return nil, io.EOF
		}
		if p.err != nil {
			if p.reader != nil {
				_ = p.reader.Close()
			}
			if errors.Is(p.err, io.EOF) {
				// The chunk opened but held no rows; move on to the next one.
				continue
			}
			ir.failed = p.err
			return nil, p.err
		}
		ir.cur = p.reader
		return p.rec, nil
	}
}

func (ir *IterativeRecordReader) Next() (rec Record, err error) {
	defer func() {
		if x := recover(); x != nil {
			rec, err = nil, merr.WrapErrServiceInternalMsg("internal error recovered: %v", x)
		}
	}()
	if ir.window > 1 {
		if ir.cur == nil {
			return ir.nextChunk()
		}
		rec, err = ir.cur.Next()
		if errors.Is(err, io.EOF) {
			// Drop the reader before reporting its Close error: it has been
			// closed either way, and keeping it would close it twice later.
			closeErr := ir.cur.Close()
			ir.cur = nil
			if closeErr != nil {
				return nil, closeErr
			}
			return ir.nextChunk()
		}
		return rec, err
	}
	return ir.nextSerial()
}

// nextSerial is the original one-chunk-at-a-time path, kept intact for the
// readers that do not opt into prefetching.
func (ir *IterativeRecordReader) nextSerial() (rec Record, err error) {
	if ir.cur == nil {
		r, err := ir.iterate()
		if err != nil {
			return nil, err
		}
		ir.cur = r
	}
	rec, err = ir.cur.Next()
	if err == io.EOF {
		closeErr := ir.cur.Close()
		if closeErr != nil {
			return nil, closeErr
		}
		// Clear cur before iterating: iterate() returns a typed-nil reader
		// (e.g. a nil *packedRecordReader boxed into the RecordReader
		// interface) together with an error when opening the next chunk
		// fails, e.g. a binlog object is missing in object storage. Assigning
		// that to ir.cur would leave a non-nil interface holding a nil pointer,
		// and the deferred Close() would then dereference it and panic. Only
		// publish the reader once iterate() succeeds.
		ir.cur = nil
		next, iterErr := ir.iterate()
		if iterErr != nil {
			return nil, iterErr
		}
		ir.cur = next
		rec, err = ir.cur.Next()
	}
	return rec, err
}

func newIterativePackedRecordReader(
	paths [][]string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	externalReader packed.ExternalReaderContext,
	readConcurrency int,
) *IterativeRecordReader {
	chunk := 0
	return &IterativeRecordReader{
		window: readConcurrency,
		iterate: func() (RecordReader, error) {
			if chunk >= len(paths) {
				return nil, io.EOF
			}
			currentPaths := paths[chunk]
			chunk++
			return newPackedRecordReader(currentPaths, schema, bufferSize, storageConfig, storagePluginContext, externalReader)
		},
	}
}

type ManifestReader struct {
	fieldBinlogs []*datapb.FieldBinlog
	manifest     string
	reader       *packed.FFIPackedReader

	bufferSize           int64
	arrowSchema          *arrow.Schema
	schema               *schemapb.CollectionSchema
	schemaHelper         *typeutil.SchemaHelper
	field2Col            map[FieldID]int
	storageConfig        *indexpb.StorageConfig
	storagePluginContext *indexcgopb.StoragePluginContext
	externalSpecContext  packed.ExternalSpecContext

	neededColumns []string
}

// NewManifestReaderFromBinlogs creates a ManifestReader from binlogs
func NewManifestReaderFromBinlogs(fieldBinlogs []*datapb.FieldBinlog,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*ManifestReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema, false)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "convert collection schema [%s] to arrow schema", schema.Name)
	}
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, err
	}
	field2Col := make(map[FieldID]int)
	allFields := typeutil.GetAllFieldSchemas(schema)
	neededColumns := make([]string, 0, len(allFields))
	for i, field := range allFields {
		field2Col[field.FieldID] = i
		neededColumns = append(neededColumns, field.Name)
	}
	prr := &ManifestReader{
		fieldBinlogs:         fieldBinlogs,
		bufferSize:           bufferSize,
		arrowSchema:          arrowSchema,
		schema:               schema,
		schemaHelper:         schemaHelper,
		field2Col:            field2Col,
		storageConfig:        storageConfig,
		storagePluginContext: storagePluginContext,

		neededColumns: neededColumns,
	}

	err = prr.init()
	if err != nil {
		return nil, err
	}

	return prr, nil
}

func NewManifestReader(manifest string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	option ...RwOption,
) (*ManifestReader, error) {
	rwOptions := DefaultReaderOptions()
	for _, opt := range option {
		opt(rwOptions)
	}

	return NewManifestReaderWithExtfs(
		manifest,
		schema,
		bufferSize,
		storageConfig,
		storagePluginContext,
		rwOptions.externalReader,
	)
}

// NewManifestReaderWithExtfs opens a manifest with external filesystem
// properties injected for source manifests referenced by external collections.
func NewManifestReaderWithExtfs(
	manifest string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	extfs packed.ExternalSpecContext,
) (*ManifestReader, error) {
	columnResolver := typeutil.NewStorageColumnResolver(schema, typeutil.WithStorageColumnExternalSpec(extfs.Spec))
	arrowSchema, err := ConvertToArrowSchemaWithNameResolver(
		schema,
		true,
		columnResolver.ManifestStoredColumnName,
	)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "convert collection schema [%s] to arrow schema", schema.Name)
	}

	// The Arrow schema passed to storagev2 is a physical read contract, not a
	// generic "accept whatever the reader returns" conversion layer. TEXT is the
	// boundary case: internal packed manifests store TEXT as binary LOB
	// references, while external collections read source columns where TEXT is
	// ordinary UTF8 data. Keep that storage-format split here so later
	// RecordToInsertData conversion does not accidentally decode internal LOB
	// references as user text. Any source type coercion must stay in the
	// external-source normalization path, not in the internal manifest path.
	if !typeutil.IsExternalCollection(schema) || columnResolver.IsMilvusTable() {
		arrowSchema = overrideTextFieldsToBinaryByFields(
			columnResolver.ManifestStoredFields(),
			arrowSchema,
		)
	}

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, err
	}
	field2Col := make(map[FieldID]int)
	allFields := typeutil.GetAllFieldSchemas(schema)
	neededColumns := make([]string, 0, len(allFields))
	for _, field := range allFields {
		columnName, ok := columnResolver.ManifestStoredColumnName(field)
		if !ok {
			continue
		}
		field2Col[field.FieldID] = len(neededColumns)
		neededColumns = append(neededColumns, columnName)
	}
	prr := &ManifestReader{
		manifest:             manifest,
		bufferSize:           bufferSize,
		arrowSchema:          arrowSchema,
		schema:               schema,
		schemaHelper:         schemaHelper,
		field2Col:            field2Col,
		storageConfig:        storageConfig,
		storagePluginContext: storagePluginContext,
		externalSpecContext:  extfs,

		neededColumns: neededColumns,
	}

	err = prr.init()
	if err != nil {
		return nil, err
	}

	return prr, nil
}

func (mr *ManifestReader) init() error {
	reader, err := packed.NewFFIPackedReader(mr.manifest, mr.arrowSchema, mr.neededColumns,
		mr.bufferSize,
		mr.storageConfig,
		mr.storagePluginContext,
		mr.externalSpecContext,
	)
	if err != nil {
		return err
	}
	mr.reader = reader
	return nil
}

func (mr *ManifestReader) Next() (Record, error) {
	rec, err := mr.reader.ReadNext()
	if err != nil {
		return nil, err
	}
	return NewSimpleArrowRecord(rec, mr.field2Col), nil
}

func (mr *ManifestReader) Close() error {
	if mr.reader != nil {
		return mr.reader.Close()
	}
	return nil
}

// ChunkedBlobsReader returns a chunk composed of blobs, or io.EOF if no more data
type ChunkedBlobsReader func() ([]*Blob, error)

type CompositeBinlogRecordReader struct {
	fields  map[FieldID]*schemapb.FieldSchema
	index   map[FieldID]int16
	brs     []*BinlogReader
	rrs     []array.RecordReader
	current Record
}

var _ RecordReader = (*CompositeBinlogRecordReader)(nil)

func (crr *CompositeBinlogRecordReader) Next() (Record, error) {
	crr.releaseCurrent()

	recs := make([]arrow.Array, len(crr.fields))
	releaseRecsOnError := true
	defer func() {
		if releaseRecsOnError {
			for _, rec := range recs {
				if rec != nil {
					rec.Release()
				}
			}
		}
	}()
	nonExistingFields := make([]*schemapb.FieldSchema, 0)
	nRows := 0
	for _, f := range crr.fields {
		idx := crr.index[f.FieldID]
		if crr.rrs[idx] != nil {
			if ok := crr.rrs[idx].Next(); !ok {
				return nil, io.EOF
			}
			r := crr.rrs[idx].Record()
			recs[idx] = r.Column(0)
			recs[idx].Retain()
			if nRows == 0 {
				nRows = int(r.NumRows())
			}
			if nRows != int(r.NumRows()) {
				return nil, merr.WrapErrServiceInternalMsg("number of rows mismatch for field %d", f.FieldID)
			}
		} else {
			nonExistingFields = append(nonExistingFields, f)
		}
	}
	for _, f := range nonExistingFields {
		// If the field is not in the current batch, fill with null array
		arr, err := GenerateEmptyArrayFromSchema(f, nRows)
		if err != nil {
			return nil, err
		}
		recs[crr.index[f.FieldID]] = arr
	}
	releaseRecsOnError = false
	crr.current = &compositeRecord{
		index: crr.index,
		recs:  recs,
	}
	return crr.current, nil
}

func (crr *CompositeBinlogRecordReader) Close() error {
	crr.releaseCurrent()

	if crr.brs != nil {
		for _, er := range crr.brs {
			if er != nil {
				er.Close()
			}
		}
	}
	if crr.rrs != nil {
		for _, rr := range crr.rrs {
			if rr != nil {
				rr.Release()
			}
		}
	}
	return nil
}

func (crr *CompositeBinlogRecordReader) releaseCurrent() {
	if crr.current != nil {
		crr.current.Release()
		crr.current = nil
	}
}

// NewAbsentFieldFillRecordReader completes a partial record to full read-schema
// width the way V1's CompositeBinlogRecordReader does: every read-schema field NOT
// physically present (FieldID not in presentFields) is filled via
// GenerateEmptyArrayFromSchema -- its declared default when it has one, else null,
// erroring on a non-nullable absent field. Present columns pass through from inner.
// This gives the packed StorageV3 manifest reader the same default-for-absent
// semantic the V1 binlog reader already applies, so it stops presenting declared
// defaults as NULL (issue #52771). presentFields is the physically-present field
// set -- self-sourced from the manifest, or supplied by a caller that already
// computed it (compaction via WithPresentFields). Returns inner unchanged when
// nothing is absent.
func NewAbsentFieldFillRecordReader(inner RecordReader, neededSchema *schemapb.CollectionSchema, presentFields map[FieldID]struct{}) RecordReader {
	fill := make([]*schemapb.FieldSchema, 0)
	for _, f := range typeutil.GetAllFieldSchemas(neededSchema) {
		if _, present := presentFields[f.GetFieldID()]; present {
			continue
		}
		fill = append(fill, f)
	}
	if len(fill) == 0 {
		return inner
	}
	return &absentFieldFillRecordReader{inner: inner, fill: fill}
}

type absentFieldFillRecordReader struct {
	inner RecordReader
	fill  []*schemapb.FieldSchema
	cur   *absentFilledRecord
}

var _ RecordReader = (*absentFieldFillRecordReader)(nil)

func (r *absentFieldFillRecordReader) Next() (Record, error) {
	r.releaseCur()
	base, err := r.inner.Next()
	if err != nil {
		return nil, err
	}
	computed := make(map[FieldID]arrow.Array, len(r.fill))
	for _, f := range r.fill {
		arr, genErr := GenerateEmptyArrayFromSchema(f, base.Len())
		if genErr != nil {
			for _, a := range computed {
				a.Release()
			}
			return nil, genErr
		}
		computed[f.GetFieldID()] = arr
	}
	base.Retain()
	r.cur = &absentFilledRecord{base: base, computed: computed}
	return r.cur, nil
}

func (r *absentFieldFillRecordReader) releaseCur() {
	if r.cur == nil {
		return
	}
	r.cur.Release()
	r.cur = nil
}

func (r *absentFieldFillRecordReader) Close() error {
	r.releaseCur()
	if r.inner == nil {
		return nil
	}
	return r.inner.Close()
}

// absentFilledRecord overlays filled columns onto base (the present columns). It
// owns a retained ref on base plus the filled arrays; Release drops both, so the
// wrapping reader frees them by releasing this record on its next Next/Close.
type absentFilledRecord struct {
	base     Record
	computed map[FieldID]arrow.Array
}

var _ Record = (*absentFilledRecord)(nil)

func (r *absentFilledRecord) Column(i FieldID) arrow.Array {
	if col, ok := r.computed[i]; ok {
		return col
	}
	return r.base.Column(i)
}

func (r *absentFilledRecord) Len() int { return r.base.Len() }

func (r *absentFilledRecord) Retain() {
	r.base.Retain()
	for _, col := range r.computed {
		col.Retain()
	}
}

func (r *absentFilledRecord) Release() {
	r.base.Release()
	for _, col := range r.computed {
		col.Release()
	}
}
