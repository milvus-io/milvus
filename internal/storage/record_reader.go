package storage

import (
	"context"
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

const ImportFragmentFormatParquet = "parquet"

// ImportFragmentReaderSpec is the storage-layer description of one immutable
// Import V3 fragment.  The Import proto is deliberately not referenced here:
// DataNode validates the wire FragmentRef and then passes only the fields the
// packed reader needs.  This keeps storage reusable by internal and future
// external fragment producers.
type ImportFragmentReaderSpec struct {
	Path     string
	Format   string
	StartRow int64
	EndRow   int64
	Rows     int64
}

// importFragmentRecordReader adds the Import contract that the packed FFI
// reader itself cannot check: cancellation and an exact logical row count.
// Sort-order validation remains in storage.MergeSort, at the point where the
// keys are already decoded and compared.
type importFragmentRecordReader struct {
	ctx          context.Context
	reader       RecordReader
	expectedRows int64
	readRows     int64
	finished     bool
}

var _ RecordReader = (*importFragmentRecordReader)(nil)

func (r *importFragmentRecordReader) Next() (Record, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.finished {
		return nil, io.EOF
	}
	rec, err := r.reader.Next()
	if err == io.EOF {
		r.finished = true
		if r.readRows != r.expectedRows {
			return nil, merr.WrapErrDataIntegrityMsg(
				"import fragment row count mismatch: expected=%d actual=%d", r.expectedRows, r.readRows)
		}
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, merr.WrapErrDataIntegrityMsg("import fragment reader returned a nil record without EOF")
	}
	r.readRows += int64(rec.Len())
	if r.readRows > r.expectedRows {
		return nil, merr.WrapErrDataIntegrityMsg(
			"import fragment row count exceeds manifest: expected=%d actual=%d", r.expectedRows, r.readRows)
	}
	return rec, nil
}

func (r *importFragmentRecordReader) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

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

// NewImportFragmentRecordReader opens exactly one immutable packed fragment.
// It performs only cheap descriptor validation before opening the object.  A
// content checksum hook can be added here later; Import V3 intentionally does
// not calculate SHA-256 in the first implementation.
func NewImportFragmentRecordReader(
	ctx context.Context,
	spec ImportFragmentReaderSpec,
	schema *schemapb.CollectionSchema,
	option ...RwOption,
) (RecordReader, error) {
	if ctx == nil {
		return nil, merr.WrapErrImportSysFailedMsg("import fragment reader context is nil")
	}
	if spec.Path == "" {
		return nil, merr.WrapErrImportSysFailedMsg("import fragment path is empty")
	}
	if spec.Format != ImportFragmentFormatParquet {
		return nil, merr.WrapErrImportSysFailedMsg("unsupported import fragment format %q", spec.Format)
	}
	if spec.StartRow < 0 || spec.EndRow <= spec.StartRow || spec.Rows <= 0 || spec.EndRow-spec.StartRow != spec.Rows {
		return nil, merr.WrapErrImportSysFailedMsg(
			"invalid import fragment range: start=%d end=%d rows=%d", spec.StartRow, spec.EndRow, spec.Rows)
	}
	if schema == nil {
		return nil, merr.WrapErrImportSysFailedMsg("import fragment schema is nil")
	}

	rwOptions := DefaultReaderOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	// Import V3 fragments are always produced by the packed writer.  The
	// storage version controls the final segment writer, not this temporary
	// object reader, so validate only the fields needed by the FFI reader.
	if rwOptions.storageConfig == nil {
		return nil, merr.WrapErrImportSysFailedMsg("storage config is nil for import fragment reader")
	}

	reader, err := newFFIPackedRecordReaderFromFragments(
		[]packed.Fragment{{
			FilePath: spec.Path,
			StartRow: spec.StartRow,
			EndRow:   spec.EndRow,
			RowCount: spec.Rows,
		}},
		spec.Format,
		schema,
		rwOptions.bufferSize,
		rwOptions.storageConfig,
		rwOptions.pluginContext,
		rwOptions.externalReader,
	)
	if err != nil {
		return nil, err
	}
	return &importFragmentRecordReader{
		ctx:          ctx,
		reader:       reader,
		expectedRows: spec.Rows,
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
