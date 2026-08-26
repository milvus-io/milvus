package storage

import (
	"io"
	"path"
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

type IterativeRecordReader struct {
	cur     RecordReader
	iterate func() (RecordReader, error)
}

// Close implements RecordReader.
func (ir *IterativeRecordReader) Close() error {
	if ir.cur != nil {
		return ir.cur.Close()
	}
	return nil
}

func (ir *IterativeRecordReader) Next() (rec Record, err error) {
	defer func() {
		if x := recover(); x != nil {
			rec, err = nil, merr.WrapErrServiceInternalMsg("internal error recovered: %v", x)
		}
	}()
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
) *IterativeRecordReader {
	chunk := 0
	return &IterativeRecordReader{
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

	neededColumns  []string
	resolveTextLob bool
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
		option...,
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
	option ...RwOption,
) (*ManifestReader, error) {
	rwOptions := DefaultReaderOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	// milvus-storage SegmentReader resolves TEXT LOB values, but its current C
	// API has no key-retriever plugin context. Reject CMEK input explicitly so
	// this import path never ignores the supplied key and attempts a plaintext
	// read. CMEK-protected StorageV3 backup import is intentionally out of scope
	// for this change.
	if rwOptions.resolveTextLob && storagePluginContext != nil {
		return nil, merr.WrapErrOperationNotSupportedMsg(
			"CMEK-protected StorageV3 backup import is not supported",
		)
	}

	columnResolver := typeutil.NewStorageColumnResolver(schema, typeutil.WithStorageColumnExternalSpec(extfs.Spec))
	arrowSchema, err := ConvertToArrowSchemaWithNameResolver(
		schema,
		true,
		columnResolver.ManifestStoredColumnName,
	)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "convert collection schema [%s] to arrow schema", schema.Name)
	}

	// The packed reader consumes internal TEXT columns as physical binary LOB
	// references. SegmentReader is the logical reader and requires UTF8 TEXT so
	// it can resolve those references before returning Arrow records. External
	// source coercion remains in the external-source normalization path.
	if !rwOptions.resolveTextLob &&
		(!typeutil.IsExternalCollection(schema) || columnResolver.IsMilvusTable()) {
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

		neededColumns:  neededColumns,
		resolveTextLob: rwOptions.resolveTextLob,
	}

	err = prr.init()
	if err != nil {
		return nil, err
	}

	return prr, nil
}

func (mr *ManifestReader) init() error {
	if mr.resolveTextLob {
		basePath, _, err := packed.UnmarshalManifestPath(mr.manifest)
		if err != nil {
			return merr.Wrap(err, "failed to parse manifest path for TEXT LOB resolution")
		}

		textColumns := make([]packed.TextColumnConfig, 0)
		for _, field := range typeutil.GetAllFieldSchemas(mr.schema) {
			if field.GetDataType() != schemapb.DataType_Text {
				continue
			}
			if _, ok := mr.field2Col[field.GetFieldID()]; !ok {
				continue
			}
			textColumns = append(textColumns, packed.TextColumnConfig{
				FieldID:     field.GetFieldID(),
				LobBasePath: path.Join(path.Dir(basePath), "lobs", strconv.FormatInt(field.GetFieldID(), 10)),
			})
		}

		// This wrapper only owns the existing SegmentReader handle and Arrow
		// stream. LOB reference parsing, I/O, null handling, and UTF8 materialization
		// stay in milvus-storage rather than being duplicated in the import path.
		reader, err := packed.NewFFISegmentReader(
			mr.manifest,
			mr.arrowSchema,
			mr.neededColumns,
			mr.bufferSize,
			mr.storageConfig,
			textColumns,
		)
		if err != nil {
			return err
		}
		mr.reader = reader
		return nil
	}

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
		err := mr.reader.Close()
		mr.reader = nil
		return err
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
