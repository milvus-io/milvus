// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// BinlogFile class is a wrapper of storage.BinlogReader, to read binlog file, block by block.
// Note: for bulkoad function, we only handle normal insert log and delta log.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
// Typically, an insert log file size is 16MB.
type BinlogFile struct {
	chunkManager storage.ChunkManager  // storage interfaces to read binlog files
	reader       *storage.BinlogReader // binlog reader
}

func NewBinlogFile(chunkManager storage.ChunkManager) (*BinlogFile, error) {
	if chunkManager == nil {
		log.Warn("Binlog file: chunk manager pointer is nil")
		return nil, errors.New("chunk manager pointer is nil")
	}

	binlogFile := &BinlogFile{
		chunkManager: chunkManager,
	}

	return binlogFile, nil
}

func (p *BinlogFile) Open(filePath string) error {
	p.Close()
	if len(filePath) == 0 {
		log.Warn("Binlog file: binlog path is empty")
		return errors.New("binlog path is empty")
	}

	// TODO add context
	bytes, err := p.chunkManager.Read(context.TODO(), filePath)
	if err != nil {
		log.Warn("Binlog file: failed to open binlog", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to open binlog %s", filePath)
	}

	p.reader, err = storage.NewBinlogReader(bytes)
	if err != nil {
		log.Warn("Binlog file: failed to initialize binlog reader", zap.String("filePath", filePath), zap.Error(err))
		return fmt.Errorf("failed to initialize binlog reader for binlog %s, error: %w", filePath, err)
	}

	log.Info("Binlog file: open binlog successfully", zap.String("filePath", filePath))
	return nil
}

// Close close the reader object, outer caller must call this method in defer
func (p *BinlogFile) Close() {
	if p.reader != nil {
		p.reader.Close()
		p.reader = nil
	}
}

func (p *BinlogFile) DataType() schemapb.DataType {
	if p.reader == nil {
		return schemapb.DataType_None
	}

	return p.reader.PayloadDataType
}

// ReadBool method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadBool() ([]bool, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]bool, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_Bool {
			log.Warn("Binlog file: binlog data type is not bool")
			return nil, errors.New("binlog data type is not bool")
		}

		data, err := event.PayloadReaderInterface.GetBoolFromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read bool data", zap.Error(err))
			return nil, fmt.Errorf("failed to read bool data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadInt8 method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadInt8() ([]int8, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]int8, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_Int8 {
			log.Warn("Binlog file: binlog data type is not int8")
			return nil, errors.New("binlog data type is not int8")
		}

		data, err := event.PayloadReaderInterface.GetInt8FromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read int8 data", zap.Error(err))
			return nil, fmt.Errorf("failed to read int8 data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadInt16 method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadInt16() ([]int16, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]int16, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_Int16 {
			log.Warn("Binlog file: binlog data type is not int16")
			return nil, errors.New("binlog data type is not int16")
		}

		data, err := event.PayloadReaderInterface.GetInt16FromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read int16 data", zap.Error(err))
			return nil, fmt.Errorf("failed to read int16 data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadInt32 method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadInt32() ([]int32, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]int32, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_Int32 {
			log.Warn("Binlog file: binlog data type is not int32")
			return nil, errors.New("binlog data type is not int32")
		}

		data, err := event.PayloadReaderInterface.GetInt32FromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read int32 data", zap.Error(err))
			return nil, fmt.Errorf("failed to read int32 data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadInt64 method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadInt64() ([]int64, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]int64, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_Int64 {
			log.Warn("Binlog file: binlog data type is not int64")
			return nil, errors.New("binlog data type is not int64")
		}

		data, err := event.PayloadReaderInterface.GetInt64FromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read int64 data", zap.Error(err))
			return nil, fmt.Errorf("failed to read int64 data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadFloat method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadFloat() ([]float32, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]float32, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_Float {
			log.Warn("Binlog file: binlog data type is not float")
			return nil, errors.New("binlog data type is not float")
		}

		data, err := event.PayloadReaderInterface.GetFloatFromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read float data", zap.Error(err))
			return nil, fmt.Errorf("failed to read float data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadDouble method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadDouble() ([]float64, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]float64, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_Double {
			log.Warn("Binlog file: binlog data type is not double")
			return nil, errors.New("binlog data type is not double")
		}

		data, err := event.PayloadReaderInterface.GetDoubleFromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read double data", zap.Error(err))
			return nil, fmt.Errorf("failed to read double data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadVarchar method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadVarchar() ([]string, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([]string, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		// special case: delete event data type is varchar
		if event.TypeCode != storage.InsertEventType && event.TypeCode != storage.DeleteEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if (p.DataType() != schemapb.DataType_VarChar) && (p.DataType() != schemapb.DataType_String) {
			log.Warn("Binlog file: binlog data type is not varchar")
			return nil, errors.New("binlog data type is not varchar")
		}

		data, err := event.PayloadReaderInterface.GetStringFromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read varchar data", zap.Error(err))
			return nil, fmt.Errorf("failed to read varchar data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadJSON method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
func (p *BinlogFile) ReadJSON() ([][]byte, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, errors.New("binlog reader not yet initialized")
	}

	result := make([][]byte, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_JSON {
			log.Warn("Binlog file: binlog data type is not JSON")
			return nil, errors.New("binlog data type is not JSON")
		}

		data, err := event.PayloadReaderInterface.GetJSONFromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read JSON data", zap.Error(err))
			return nil, fmt.Errorf("failed to read JSON data, error: %w", err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ReadBinaryVector method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
// return vectors data and the dimension
func (p *BinlogFile) ReadBinaryVector() ([]byte, int, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, 0, errors.New("binlog reader not yet initialized")
	}

	dim := 0
	result := make([]byte, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, 0, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, 0, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_BinaryVector {
			log.Warn("Binlog file: binlog data type is not binary vector")
			return nil, 0, errors.New("binlog data type is not binary vector")
		}

		data, dimenson, err := event.PayloadReaderInterface.GetBinaryVectorFromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read binary vector data", zap.Error(err))
			return nil, 0, fmt.Errorf("failed to read binary vector data, error: %w", err)
		}

		dim = dimenson
		result = append(result, data...)
	}

	return result, dim, nil
}

// ReadFloatVector method reads all the blocks of a binlog by a data type.
// A binlog is designed to support multiple blocks, but so far each binlog always contains only one block.
// return vectors data and the dimension
func (p *BinlogFile) ReadFloatVector() ([]float32, int, error) {
	if p.reader == nil {
		log.Warn("Binlog file: binlog reader not yet initialized")
		return nil, 0, errors.New("binlog reader not yet initialized")
	}

	dim := 0
	result := make([]float32, 0)
	for {
		event, err := p.reader.NextEventReader()
		if err != nil {
			log.Warn("Binlog file: failed to iterate events reader", zap.Error(err))
			return nil, 0, fmt.Errorf("failed to iterate events reader, error: %w", err)
		}

		// end of the file
		if event == nil {
			break
		}

		if event.TypeCode != storage.InsertEventType {
			log.Warn("Binlog file: binlog file is not insert log")
			return nil, 0, errors.New("binlog file is not insert log")
		}

		if p.DataType() != schemapb.DataType_FloatVector {
			log.Warn("Binlog file: binlog data type is not float vector")
			return nil, 0, errors.New("binlog data type is not float vector")
		}

		data, dimension, err := event.PayloadReaderInterface.GetFloatVectorFromPayload()
		if err != nil {
			log.Warn("Binlog file: failed to read float vector data", zap.Error(err))
			return nil, 0, fmt.Errorf("failed to read float vector data, error: %w", err)
		}

		dim = dimension
		result = append(result, data...)
	}

	return result, dim, nil
}
