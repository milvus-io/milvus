package importutil

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path"
	"strconv"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	JSONFileExt  = ".json"
	NumpyFileExt = ".npy"
)

type ImportWrapper struct {
	ctx              context.Context            // for canceling parse process
	cancel           context.CancelFunc         // for canceling parse process
	collectionSchema *schemapb.CollectionSchema // collection schema
	shardNum         int32                      // sharding number of the collection
	segmentSize      int32                      // maximum size of a segment in MB
	rowIDAllocator   *allocator.IDAllocator     // autoid allocator

	callFlushFunc func(fields map[string]storage.FieldData) error // call back function to flush a segment
}

func NewImportWrapper(ctx context.Context, collectionSchema *schemapb.CollectionSchema, shardNum int32, segmentSize int32,
	idAlloc *allocator.IDAllocator, flushFunc func(fields map[string]storage.FieldData) error) *ImportWrapper {
	if collectionSchema == nil {
		log.Error("import error: collection schema is nil")
		return nil
	}

	// ignore the RowID field and Timestamp field
	realSchema := &schemapb.CollectionSchema{
		Name:        collectionSchema.GetName(),
		Description: collectionSchema.GetDescription(),
		AutoID:      collectionSchema.GetAutoID(),
		Fields:      make([]*schemapb.FieldSchema, 0),
	}
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		if schema.GetName() == common.RowIDFieldName || schema.GetName() == common.TimeStampFieldName {
			continue
		}
		realSchema.Fields = append(realSchema.Fields, schema)
	}

	ctx, cancel := context.WithCancel(ctx)

	wrapper := &ImportWrapper{
		ctx:              ctx,
		cancel:           cancel,
		collectionSchema: realSchema,
		shardNum:         shardNum,
		segmentSize:      segmentSize,
		rowIDAllocator:   idAlloc,
		callFlushFunc:    flushFunc,
	}

	return wrapper
}

// this method can be used to cancel parse process
func (p *ImportWrapper) Cancel() error {
	p.cancel()
	return nil
}

func (p *ImportWrapper) printFieldsDataInfo(fieldsData map[string]storage.FieldData, msg string, files []string) {
	stats := make([]zapcore.Field, 0)
	for k, v := range fieldsData {
		stats = append(stats, zap.Int(k, v.RowNum()))
	}
	for i := 0; i < len(files); i++ {
		stats = append(stats, zap.String("file", files[i]))
	}
	log.Debug(msg, stats...)
}

// import process entry
// filePath and rowBased are from ImportTask
// if onlyValidate is true, this process only do validation, no data generated, callFlushFunc will not be called
func (p *ImportWrapper) Import(filePaths []string, rowBased bool, onlyValidate bool) error {
	if rowBased {
		// parse and consume row-based files
		// for row-based files, the JSONRowConsumer will generate autoid for primary key, and split rows into segments
		// according to shard number, so the callFlushFunc will be called in the JSONRowConsumer
		for i := 0; i < len(filePaths); i++ {
			filePath := filePaths[i]
			fileName := path.Base(filePath)
			fileType := path.Ext(fileName)
			log.Debug("imprort wrapper:  row-based file ", zap.Any("filePath", filePath), zap.Any("fileType", fileType))

			if fileType == JSONFileExt {
				err := func() error {
					file, err := os.Open(filePath)
					if err != nil {
						return err
					}
					defer file.Close()

					reader := bufio.NewReader(file)
					parser := NewJSONParser(p.ctx, p.collectionSchema)
					var consumer *JSONRowConsumer
					if !onlyValidate {
						flushFunc := func(fields map[string]storage.FieldData) error {
							p.printFieldsDataInfo(fields, "import wrapper: prepare to flush segment", filePaths)
							return p.callFlushFunc(fields)
						}
						consumer = NewJSONRowConsumer(p.collectionSchema, p.rowIDAllocator, p.shardNum, p.segmentSize, flushFunc)
					}
					validator := NewJSONRowValidator(p.collectionSchema, consumer)

					err = parser.ParseRows(reader, validator)
					if err != nil {
						return err
					}

					return nil
				}()

				if err != nil {
					log.Error("imprort error: "+err.Error(), zap.String("filePath", filePath))
					return err
				}
			}
		}
	} else {
		// parse and consume row-based files
		// for column-based files, the XXXColumnConsumer only output map[string]storage.FieldData
		// after all columns are parsed/consumed, we need to combine map[string]storage.FieldData into one
		// and use splitFieldsData() to split fields data into segments according to shard number
		fieldsData := initSegmentData(p.collectionSchema)
		rowCount := 0

		// function to combine column data into fieldsData
		combineFunc := func(fields map[string]storage.FieldData) error {
			if len(fields) == 0 {
				return nil
			}

			fieldNames := make([]string, 0)
			for k, v := range fields {
				data, ok := fieldsData[k]
				if ok && data.RowNum() > 0 {
					return errors.New("imprort error:  the field " + k + " is duplicated")
				}

				fieldsData[k] = v
				fieldNames = append(fieldNames, k)

				if rowCount == 0 {
					rowCount = v.RowNum()
				} else if rowCount != v.RowNum() {
					return errors.New("imprort error:  the field " + k + " row count " + strconv.Itoa(v.RowNum()) + " doesn't equal " + strconv.Itoa(rowCount))
				}
			}

			log.Debug("imprort wrapper:  ", zap.Any("fieldNames", fieldNames), zap.Int("rowCount", rowCount))

			return nil
		}

		// parse/validate/consume data
		for i := 0; i < len(filePaths); i++ {
			filePath := filePaths[i]
			fileName := path.Base(filePath)
			fileType := path.Ext(fileName)
			log.Debug("imprort wrapper:  column-based file ", zap.Any("filePath", filePath), zap.Any("fileType", fileType))

			if fileType == JSONFileExt {
				err := func() error {
					file, err := os.Open(filePath)
					if err != nil {
						log.Error("imprort error: "+err.Error(), zap.String("filePath", filePath))
						return err
					}
					defer file.Close()

					reader := bufio.NewReader(file)
					parser := NewJSONParser(p.ctx, p.collectionSchema)
					var consumer *JSONColumnConsumer
					if !onlyValidate {
						consumer = NewJSONColumnConsumer(p.collectionSchema, combineFunc)
					}
					validator := NewJSONColumnValidator(p.collectionSchema, consumer)

					err = parser.ParseColumns(reader, validator)
					if err != nil {
						log.Error("imprort error: "+err.Error(), zap.String("filePath", filePath))
						return err
					}

					return nil
				}()

				if err != nil {
					log.Error("imprort error: "+err.Error(), zap.String("filePath", filePath))
					return err
				}
			} else if fileType == NumpyFileExt {

			}
		}

		// split fields data into segments
		err := p.splitFieldsData(fieldsData, filePaths)
		if err != nil {
			log.Error("imprort error: " + err.Error())
			return err
		}
	}

	return nil
}

func (p *ImportWrapper) appendFunc(schema *schemapb.FieldSchema) func(src storage.FieldData, n int, target storage.FieldData) error {
	switch schema.DataType {
	case schemapb.DataType_Bool:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.BoolFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(bool))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Float:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.FloatFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(float32))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Double:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.DoubleFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(float64))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int8:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int8FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int8))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int16:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int16FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int16))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int32:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int32FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int32))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_Int64:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.Int64FieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(int64))
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_BinaryVector:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.BinaryVectorFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).([]byte)...)
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_FloatVector:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.FloatVectorFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).([]float32)...)
			arr.NumRows[0]++
			return nil
		}
	case schemapb.DataType_String:
		return func(src storage.FieldData, n int, target storage.FieldData) error {
			arr := target.(*storage.StringFieldData)
			arr.Data = append(arr.Data, src.GetRow(n).(string))
			return nil
		}
	default:
		return nil
	}
}

func (p *ImportWrapper) splitFieldsData(fieldsData map[string]storage.FieldData, files []string) error {
	if len(fieldsData) == 0 {
		return errors.New("imprort error: fields data is empty")
	}

	var primaryKey *schemapb.FieldSchema
	for i := 0; i < len(p.collectionSchema.Fields); i++ {
		schema := p.collectionSchema.Fields[i]
		if schema.GetIsPrimaryKey() {
			primaryKey = schema
		} else {
			_, ok := fieldsData[schema.GetName()]
			if !ok {
				return errors.New("imprort error: field " + schema.GetName() + " not provided")
			}
		}
	}
	if primaryKey == nil {
		return errors.New("imprort error: primary key field is not found")
	}

	rowCount := 0
	for _, v := range fieldsData {
		rowCount = v.RowNum()
		break
	}

	primaryData, ok := fieldsData[primaryKey.GetName()]
	if !ok {
		// generate auto id for primary key
		if primaryKey.GetAutoID() {
			var rowIDBegin typeutil.UniqueID
			var rowIDEnd typeutil.UniqueID
			rowIDBegin, rowIDEnd, _ = p.rowIDAllocator.Alloc(uint32(rowCount))

			primaryDataArr := primaryData.(*storage.Int64FieldData)
			for i := rowIDBegin; i < rowIDEnd; i++ {
				primaryDataArr.Data = append(primaryDataArr.Data, rowIDBegin+i)
			}
		}
	}

	if primaryData.RowNum() <= 0 {
		return errors.New("imprort error: primary key " + primaryKey.GetName() + " not provided")
	}

	// prepare segemnts
	segmentsData := make([]map[string]storage.FieldData, 0, p.shardNum)
	for i := 0; i < int(p.shardNum); i++ {
		segmentData := initSegmentData(p.collectionSchema)
		if segmentData == nil {
			return nil
		}
		segmentsData = append(segmentsData, segmentData)
	}

	// prepare append functions
	appendFunctions := make(map[string]func(src storage.FieldData, n int, target storage.FieldData) error)
	for i := 0; i < len(p.collectionSchema.Fields); i++ {
		schema := p.collectionSchema.Fields[i]
		appendFunc := p.appendFunc(schema)
		if appendFunc == nil {
			return errors.New("imprort error: unsupported field data type")
		}
		appendFunctions[schema.GetName()] = appendFunc
	}

	// split data into segments
	for i := 0; i < rowCount; i++ {
		id := primaryData.GetRow(i).(int64)
		// hash to a shard number
		hash, _ := typeutil.Hash32Int64(id)
		shard := hash % uint32(p.shardNum)

		for k := 0; k < len(p.collectionSchema.Fields); k++ {
			schema := p.collectionSchema.Fields[k]
			srcData := fieldsData[schema.GetName()]
			targetData := segmentsData[shard][schema.GetName()]
			appendFunc := appendFunctions[schema.GetName()]
			err := appendFunc(srcData, i, targetData)
			if err != nil {
				return err
			}
		}
	}

	// call flush function
	for i := 0; i < int(p.shardNum); i++ {
		segmentData := segmentsData[i]
		p.printFieldsDataInfo(segmentData, "import wrapper: prepare to flush segment", files)
		err := p.callFlushFunc(segmentData)
		if err != nil {
			return err
		}
	}

	return nil
}
