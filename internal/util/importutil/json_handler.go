package importutil

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// interface to process rows data
type JSONRowHandler interface {
	Handle(rows []map[string]interface{}) error
}

// interface to process column data
type JSONColumnHandler interface {
	Handle(columns map[string][]interface{}) error
}

// method to get dimension of vecotor field
func getFieldDimension(schema *schemapb.FieldSchema) (int, error) {
	for _, kvPair := range schema.GetTypeParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		if key == "dim" {
			dim, err := strconv.Atoi(value)
			if err != nil {
				return 0, errors.New("vector dimension is invalid")
			}
			return dim, nil
		}
	}

	return 0, errors.New("vector dimension is not defined")
}

// field value validator
type Validator struct {
	validateFunc func(obj interface{}) error                          // validate data type function
	convertFunc  func(obj interface{}, field storage.FieldData) error // convert data function
	primaryKey   bool                                                 // true for primary key
	autoID       bool                                                 // only for primary key field
	dimension    int                                                  // only for vector field
}

// method to construct valiator functions
func initValidators(collectionSchema *schemapb.CollectionSchema, validators map[string]*Validator) error {
	if collectionSchema == nil {
		return errors.New("collection schema is nil")
	}

	// json decoder parse all the numeric value into float64
	numericValidator := func(obj interface{}) error {
		switch obj.(type) {
		case float64:
			return nil
		default:
			s := fmt.Sprintf("%v", obj)
			msg := "illegal numeric value " + s
			return errors.New(msg)
		}

	}

	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]

		validators[schema.GetName()] = &Validator{}
		validators[schema.GetName()].primaryKey = schema.GetIsPrimaryKey()
		validators[schema.GetName()].autoID = schema.GetAutoID()

		switch schema.DataType {
		case schemapb.DataType_Bool:
			validators[schema.GetName()].validateFunc = func(obj interface{}) error {
				switch obj.(type) {
				case bool:
					return nil
				default:
					s := fmt.Sprintf("%v", obj)
					msg := "illegal value " + s + " for bool type field " + schema.GetName()
					return errors.New(msg)
				}

			}
			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := obj.(bool)
				field.(*storage.BoolFieldData).Data = append(field.(*storage.BoolFieldData).Data, value)
				field.(*storage.BoolFieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_Float:
			validators[schema.GetName()].validateFunc = numericValidator
			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := float32(obj.(float64))
				field.(*storage.FloatFieldData).Data = append(field.(*storage.FloatFieldData).Data, value)
				field.(*storage.FloatFieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_Double:
			validators[schema.GetName()].validateFunc = numericValidator
			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := obj.(float64)
				field.(*storage.DoubleFieldData).Data = append(field.(*storage.DoubleFieldData).Data, value)
				field.(*storage.DoubleFieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_Int8:
			validators[schema.GetName()].validateFunc = numericValidator
			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := int8(obj.(float64))
				field.(*storage.Int8FieldData).Data = append(field.(*storage.Int8FieldData).Data, value)
				field.(*storage.Int8FieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_Int16:
			validators[schema.GetName()].validateFunc = numericValidator
			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := int16(obj.(float64))
				field.(*storage.Int16FieldData).Data = append(field.(*storage.Int16FieldData).Data, value)
				field.(*storage.Int16FieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_Int32:
			validators[schema.GetName()].validateFunc = numericValidator
			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := int32(obj.(float64))
				field.(*storage.Int32FieldData).Data = append(field.(*storage.Int32FieldData).Data, value)
				field.(*storage.Int32FieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_Int64:
			validators[schema.GetName()].validateFunc = numericValidator
			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := int64(obj.(float64))
				field.(*storage.Int64FieldData).Data = append(field.(*storage.Int64FieldData).Data, value)
				field.(*storage.Int64FieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_BinaryVector:
			dim, err := getFieldDimension(schema)
			if err != nil {
				return err
			}
			validators[schema.GetName()].dimension = dim

			validators[schema.GetName()].validateFunc = func(obj interface{}) error {
				switch vt := obj.(type) {
				case []interface{}:
					if len(vt)*8 != dim {
						msg := "bit size " + strconv.Itoa(len(vt)*8) + " doesn't equal to vector dimension " + strconv.Itoa(dim)
						return errors.New(msg)
					}
					for i := 0; i < len(vt); i++ {
						if e := numericValidator(vt[i]); e != nil {
							msg := e.Error() + " for binary vector field " + schema.GetName()
							return errors.New(msg)
						}

						t := int(vt[i].(float64))
						if t >= 255 || t < 0 {
							msg := "illegal value " + strconv.Itoa(t) + " for binary vector field " + schema.GetName()
							return errors.New(msg)
						}
					}
					return nil
				default:
					s := fmt.Sprintf("%v", obj)
					msg := s + " is not an array for binary vector field " + schema.GetName()
					return errors.New(msg)
				}
			}

			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				arr := obj.([]interface{})
				for i := 0; i < len(arr); i++ {
					value := byte(arr[i].(float64))
					field.(*storage.BinaryVectorFieldData).Data = append(field.(*storage.BinaryVectorFieldData).Data, value)
				}

				field.(*storage.BinaryVectorFieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_FloatVector:
			dim, err := getFieldDimension(schema)
			if err != nil {
				return err
			}
			validators[schema.GetName()].dimension = dim

			validators[schema.GetName()].validateFunc = func(obj interface{}) error {
				switch vt := obj.(type) {
				case []interface{}:
					if len(vt) != dim {
						msg := "array size " + strconv.Itoa(len(vt)) + " doesn't equal to vector dimension " + strconv.Itoa(dim)
						return errors.New(msg)
					}
					for i := 0; i < len(vt); i++ {
						if e := numericValidator(vt[i]); e != nil {
							msg := e.Error() + " for float vector field " + schema.GetName()
							return errors.New(msg)
						}
					}
					return nil
				default:
					s := fmt.Sprintf("%v", obj)
					msg := s + " is not an array for float vector field " + schema.GetName()
					return errors.New(msg)
				}
			}

			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				arr := obj.([]interface{})
				for i := 0; i < len(arr); i++ {
					value := float32(arr[i].(float64))
					field.(*storage.FloatVectorFieldData).Data = append(field.(*storage.FloatVectorFieldData).Data, value)
				}
				field.(*storage.FloatVectorFieldData).NumRows[0]++
				return nil
			}
		case schemapb.DataType_String:
			validators[schema.GetName()].validateFunc = func(obj interface{}) error {
				switch obj.(type) {
				case string:
					return nil
				default:
					s := fmt.Sprintf("%v", obj)
					msg := s + " is not a string for string type field " + schema.GetName()
					return errors.New(msg)
				}
			}

			validators[schema.GetName()].convertFunc = func(obj interface{}, field storage.FieldData) error {
				value := obj.(string)
				field.(*storage.StringFieldData).Data = append(field.(*storage.StringFieldData).Data, value)
				field.(*storage.StringFieldData).NumRows[0]++
				return nil
			}
		default:
			return errors.New("unsupport data type: " + strconv.Itoa(int(collectionSchema.Fields[i].DataType)))
		}
	}

	return nil
}

// row-based json format validator class
type JSONRowValidator struct {
	downstream JSONRowHandler        // downstream processor, typically is a JSONRowComsumer
	validators map[string]*Validator // validators for each field
	rowCounter int64                 // how many rows have been validated
}

func NewJSONRowValidator(collectionSchema *schemapb.CollectionSchema, downstream JSONRowHandler) *JSONRowValidator {
	v := &JSONRowValidator{
		validators: make(map[string]*Validator),
		downstream: downstream,
		rowCounter: 0,
	}
	initValidators(collectionSchema, v.validators)

	return v
}

func (v *JSONRowValidator) ValidateCount() int64 {
	return v.rowCounter
}

func (v *JSONRowValidator) Handle(rows []map[string]interface{}) error {
	if v.validators == nil || len(v.validators) == 0 {
		return errors.New("JSON row validator is not initialized")
	}

	// parse completed
	if rows == nil {
		log.Debug("JSON row validation finished")
		if v.downstream != nil {
			return v.downstream.Handle(rows)
		}
		return nil
	}

	for i := 0; i < len(rows); i++ {
		row := rows[i]
		for name, validator := range v.validators {
			if validator.primaryKey && validator.autoID {
				// auto-generated primary key, ignore
				continue
			}
			value, ok := row[name]
			if !ok {
				return errors.New("JSON row validator: field " + name + " missed at the row " + strconv.FormatInt(v.rowCounter+int64(i), 10))
			}

			if err := validator.validateFunc(value); err != nil {
				return errors.New("JSON row validator: " + err.Error() + " at the row " + strconv.FormatInt(v.rowCounter, 10))
			}
		}
	}

	v.rowCounter += int64(len(rows))

	if v.downstream != nil {
		return v.downstream.Handle(rows)
	}

	return nil
}

// column-based json format validator class
type JSONColumnValidator struct {
	downstream JSONColumnHandler     // downstream processor, typically is a JSONColumnComsumer
	validators map[string]*Validator // validators for each field
	rowCounter map[string]int64      // row count of each field
}

func NewJSONColumnValidator(schema *schemapb.CollectionSchema, downstream JSONColumnHandler) *JSONColumnValidator {
	v := &JSONColumnValidator{
		validators: make(map[string]*Validator),
		downstream: downstream,
		rowCounter: make(map[string]int64),
	}
	initValidators(schema, v.validators)

	return v
}

func (v *JSONColumnValidator) ValidateCount() map[string]int64 {
	return v.rowCounter
}

func (v *JSONColumnValidator) Handle(columns map[string][]interface{}) error {
	if v.validators == nil || len(v.validators) == 0 {
		return errors.New("JSON column validator is not initialized")
	}

	// parse completed
	if columns == nil {
		// compare the row count of columns, should be equal
		rowCount := int64(-1)
		for k, counter := range v.rowCounter {
			if rowCount == -1 {
				rowCount = counter
			} else if rowCount != counter {
				return errors.New("JSON column validator: the field " + k + " row count " + strconv.Itoa(int(counter)) + " is not equal to other fields " + strconv.Itoa(int(rowCount)))
			}
		}

		// let the downstream know parse is completed
		log.Debug("JSON column validation finished")
		if v.downstream != nil {
			return v.downstream.Handle(nil)
		}
		return nil
	}

	for name, values := range columns {
		validator, ok := v.validators[name]
		if !ok {
			// not a valid field name, skip without parsing
			break
		}

		for i := 0; i < len(values); i++ {
			if err := validator.validateFunc(values[i]); err != nil {
				return errors.New("JSON column validator: " + err.Error() + " at the row " + strconv.FormatInt(v.rowCounter[name]+int64(i), 10))
			}
		}

		v.rowCounter[name] += int64(len(values))
	}

	if v.downstream != nil {
		return v.downstream.Handle(columns)
	}

	return nil
}

// row-based json format consumer class
type JSONRowConsumer struct {
	collectionSchema *schemapb.CollectionSchema     // collection schema
	rowIDAllocator   *allocator.IDAllocator         // autoid allocator
	validators       map[string]*Validator          // validators for each field
	rowCounter       int64                          // how many rows have been consumed
	shardNum         int32                          // sharding number of the collection
	segmentsData     []map[string]storage.FieldData // in-memory segments data
	segmentSize      int32                          // maximum size of a segment in MB
	primaryKey       string                         // name of primary key

	callFlushFunc func(fields map[string]storage.FieldData) error // call back function to flush segment
}

func initSegmentData(collectionSchema *schemapb.CollectionSchema) map[string]storage.FieldData {
	segmentData := make(map[string]storage.FieldData)
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		switch schema.DataType {
		case schemapb.DataType_Bool:
			segmentData[schema.GetName()] = &storage.BoolFieldData{
				Data:    make([]bool, 0),
				NumRows: []int64{0},
			}
		case schemapb.DataType_Float:
			segmentData[schema.GetName()] = &storage.FloatFieldData{
				Data:    make([]float32, 0),
				NumRows: []int64{0},
			}
		case schemapb.DataType_Double:
			segmentData[schema.GetName()] = &storage.DoubleFieldData{
				Data:    make([]float64, 0),
				NumRows: []int64{0},
			}
		case schemapb.DataType_Int8:
			segmentData[schema.GetName()] = &storage.Int8FieldData{
				Data:    make([]int8, 0),
				NumRows: []int64{0},
			}
		case schemapb.DataType_Int16:
			segmentData[schema.GetName()] = &storage.Int16FieldData{
				Data:    make([]int16, 0),
				NumRows: []int64{0},
			}
		case schemapb.DataType_Int32:
			segmentData[schema.GetName()] = &storage.Int32FieldData{
				Data:    make([]int32, 0),
				NumRows: []int64{0},
			}
		case schemapb.DataType_Int64:
			segmentData[schema.GetName()] = &storage.Int64FieldData{
				Data:    make([]int64, 0),
				NumRows: []int64{0},
			}
		case schemapb.DataType_BinaryVector:
			dim, _ := getFieldDimension(schema)
			segmentData[schema.GetName()] = &storage.BinaryVectorFieldData{
				Data:    make([]byte, 0),
				NumRows: []int64{0},
				Dim:     dim,
			}
		case schemapb.DataType_FloatVector:
			dim, _ := getFieldDimension(schema)
			segmentData[schema.GetName()] = &storage.FloatVectorFieldData{
				Data:    make([]float32, 0),
				NumRows: []int64{0},
				Dim:     dim,
			}
		case schemapb.DataType_String:
			segmentData[schema.GetName()] = &storage.StringFieldData{
				Data:    make([]string, 0),
				NumRows: []int64{0},
			}
		default:
			log.Error("JSON row consumer error: unsupported data type", zap.Int("DataType", int(schema.DataType)))
			return nil
		}
	}

	return segmentData
}

func NewJSONRowConsumer(collectionSchema *schemapb.CollectionSchema, idAlloc *allocator.IDAllocator, shardNum int32, segmentSize int32,
	flushFunc func(fields map[string]storage.FieldData) error) *JSONRowConsumer {
	if collectionSchema == nil {
		log.Error("JSON row consumer: collection schema is nil")
		return nil
	}

	v := &JSONRowConsumer{
		collectionSchema: collectionSchema,
		rowIDAllocator:   idAlloc,
		validators:       make(map[string]*Validator),
		shardNum:         shardNum,
		segmentSize:      segmentSize,
		rowCounter:       0,
		callFlushFunc:    flushFunc,
	}

	initValidators(collectionSchema, v.validators)

	v.segmentsData = make([]map[string]storage.FieldData, 0, shardNum)
	for i := 0; i < int(shardNum); i++ {
		segmentData := initSegmentData(collectionSchema)
		if segmentData == nil {
			return nil
		}
		v.segmentsData = append(v.segmentsData, segmentData)
	}

	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		if schema.GetIsPrimaryKey() {
			v.primaryKey = schema.GetName()
			break
		}
	}
	// primary key not found
	if v.primaryKey == "" {
		log.Error("JSON row consumer: collection schema has no primary key")
		return nil
	}
	// primary key is autoid, id generator is required
	if v.validators[v.primaryKey].autoID && idAlloc == nil {
		log.Error("JSON row consumer: ID allocator is nil")
		return nil
	}

	return v
}

func (v *JSONRowConsumer) flush(force bool) error {
	// force flush all data
	if force {
		for i := 0; i < len(v.segmentsData); i++ {
			segmentData := v.segmentsData[i]
			rowNum := segmentData[v.primaryKey].RowNum()
			if rowNum > 0 {
				log.Debug("JSON row consumer: force flush segment", zap.Int("rows", rowNum))
				v.callFlushFunc(segmentData)
			}
		}

		return nil
	}

	// segment size can be flushed
	for i := 0; i < len(v.segmentsData); i++ {
		segmentData := v.segmentsData[i]
		memSize := 0
		for _, field := range segmentData {
			memSize += field.GetMemorySize()
		}
		if memSize >= int(v.segmentSize)*1024*1024 {
			log.Debug("JSON row consumer: flush fulled segment", zap.Int("bytes", memSize))
			v.callFlushFunc(segmentData)
			v.segmentsData[i] = initSegmentData(v.collectionSchema)
		}
	}

	return nil
}

func (v *JSONRowConsumer) Handle(rows []map[string]interface{}) error {
	if v.validators == nil || len(v.validators) == 0 {
		return errors.New("JSON row consumer is not initialized")
	}

	// flush in necessery
	if rows == nil {
		err := v.flush(true)
		log.Debug("JSON row consumer finished")
		return err
	}

	err := v.flush(false)
	if err != nil {
		return err
	}

	// prepare autoid
	primaryValidator := v.validators[v.primaryKey]
	var rowIDBegin typeutil.UniqueID
	var rowIDEnd typeutil.UniqueID
	if primaryValidator.autoID {
		var err error
		rowIDBegin, rowIDEnd, err = v.rowIDAllocator.Alloc(uint32(len(rows)))
		if err != nil {
			return errors.New("JSON row consumer: " + err.Error())
		}
		if rowIDEnd-rowIDBegin != int64(len(rows)) {
			return errors.New("JSON row consumer: failed to allocate ID for " + strconv.Itoa(len(rows)) + " rows")
		}
	}

	// consume rows
	for i := 0; i < len(rows); i++ {
		row := rows[i]

		// firstly get/generate the row id
		var id int64
		if primaryValidator.autoID {
			id = rowIDBegin + int64(i)
		} else {
			value := row[v.primaryKey]
			id = int64(value.(float64))
		}

		// hash to a shard number
		hash, _ := typeutil.Hash32Int64(id)
		shard := hash % uint32(v.shardNum)
		pkArray := v.segmentsData[shard][v.primaryKey].(*storage.Int64FieldData)
		pkArray.Data = append(pkArray.Data, id)

		// convert value and consume
		for name, validator := range v.validators {
			if validator.primaryKey {
				continue
			}
			value := row[name]
			if err := validator.convertFunc(value, v.segmentsData[shard][name]); err != nil {
				return errors.New("JSON row consumer: " + err.Error() + " at the row " + strconv.FormatInt(v.rowCounter, 10))
			}
		}
	}

	v.rowCounter += int64(len(rows))

	return nil
}

// column-based json format consumer class
type JSONColumnConsumer struct {
	collectionSchema *schemapb.CollectionSchema   // collection schema
	validators       map[string]*Validator        // validators for each field
	fieldsData       map[string]storage.FieldData // in-memory fields data
	primaryKey       string                       // name of primary key

	callFlushFunc func(fields map[string]storage.FieldData) error // call back function to flush segment
}

func NewJSONColumnConsumer(collectionSchema *schemapb.CollectionSchema,
	flushFunc func(fields map[string]storage.FieldData) error) *JSONColumnConsumer {
	if collectionSchema == nil {
		return nil
	}

	v := &JSONColumnConsumer{
		collectionSchema: collectionSchema,
		validators:       make(map[string]*Validator),
		callFlushFunc:    flushFunc,
	}
	initValidators(collectionSchema, v.validators)
	v.fieldsData = initSegmentData(collectionSchema)

	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		if schema.GetIsPrimaryKey() {
			v.primaryKey = schema.GetName()
			break
		}
	}

	return v
}

func (v *JSONColumnConsumer) flush() error {
	// check row count, should be equal
	rowCount := 0
	for name, field := range v.fieldsData {
		// skip the autoid field
		if name == v.primaryKey && v.validators[v.primaryKey].autoID {
			continue
		}
		cnt := field.RowNum()
		// skip 0 row fields since a data file may only import one column(there are several data files imported)
		if cnt == 0 {
			continue
		}

		// only check non-zero row fields
		if rowCount == 0 {
			rowCount = cnt
		} else if rowCount != cnt {
			return errors.New("JSON column consumer: " + name + " row count " + strconv.Itoa(cnt) + " doesn't equal " + strconv.Itoa(rowCount))
		}
	}

	if rowCount == 0 {
		return errors.New("JSON column consumer: row count is 0")
	}
	log.Debug("JSON column consumer: rows parsed", zap.Int("rowCount", rowCount))

	// output the fileds data, let outside split them into segments
	return v.callFlushFunc(v.fieldsData)
}

func (v *JSONColumnConsumer) Handle(columns map[string][]interface{}) error {
	if v.validators == nil || len(v.validators) == 0 {
		return errors.New("JSON column consumer is not initialized")
	}

	// flush at the end
	if columns == nil {
		err := v.flush()
		log.Debug("JSON column consumer finished")
		return err
	}

	// consume columns data
	for name, values := range columns {
		validator, ok := v.validators[name]
		if !ok {
			// not a valid field name
			break
		}

		if validator.primaryKey && validator.autoID {
			// autoid is no need to provide
			break
		}

		// convert and consume data
		for i := 0; i < len(values); i++ {
			if err := validator.convertFunc(values[i], v.fieldsData[name]); err != nil {
				return errors.New("JSON column consumer: " + err.Error() + " of field " + name)
			}
		}
	}

	return nil
}
