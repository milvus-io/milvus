package storage

import (
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
	fields map[FieldID]*schemapb.FieldSchema
	index  map[FieldID]int16
	brs    []*BinlogReader
	rrs    []array.RecordReader
}

var _ RecordReader = (*CompositeBinlogRecordReader)(nil)

func (crr *CompositeBinlogRecordReader) Next() (Record, error) {
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
	return &compositeRecord{
		index: crr.index,
		recs:  recs,
	}, nil
}

func (crr *CompositeBinlogRecordReader) Close() error {
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
