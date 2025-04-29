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

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

// BinlogReader is an object to read binlog file. Binlog file's format can be
// found in design docs.
type BinlogReader struct {
	magicNumber int32
	descriptorEvent
	buffer      *bytes.Buffer
	eventReader *EventReader
	isClose     bool
}

// NextEventReader iters all events reader to read the binlog file.
func (reader *BinlogReader) NextEventReader() (*EventReader, error) {
	if reader.isClose {
		return nil, errors.New("bin log reader is closed")
	}
	if reader.buffer.Len() <= 0 {
		return nil, nil
	}
	// close the previous reader
	if reader.eventReader != nil {
		reader.eventReader.Close()
	}
	nullable, err := reader.descriptorEvent.GetNullable()
	if err != nil {
		return nil, err
	}
	reader.eventReader, err = newEventReader(reader.descriptorEvent.PayloadDataType, reader.buffer, nullable)
	if err != nil {
		return nil, err
	}
	return reader.eventReader, nil
}

func readMagicNumber(buffer io.Reader) (int32, error) {
	var magicNumber int32
	if err := binary.Read(buffer, common.Endian, &magicNumber); err != nil {
		return -1, err
	}
	if magicNumber != MagicNumber {
		return -1, fmt.Errorf("parse magic number failed, expected: %d, actual: %d", MagicNumber, magicNumber)
	}

	return magicNumber, nil
}

// ReadDescriptorEvent reads a descriptorEvent from buffer
func ReadDescriptorEvent(buffer io.Reader) (*descriptorEvent, error) {
	header, err := readDescriptorEventHeader(buffer)
	if err != nil {
		return nil, err
	}
	data, err := readDescriptorEventData(buffer)
	if err != nil {
		return nil, err
	}

	return &descriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}, nil
}

// Close closes the BinlogReader object.
// It mainly calls the Close method of the internal events, reclaims resources, and marks itself as closed.
func (reader *BinlogReader) Close() {
	if reader == nil {
		return
	}
	if reader.isClose {
		return
	}
	if reader.eventReader != nil {
		reader.eventReader.Close()
	}
	reader.isClose = true
}

type BinlogReaderOption func(base *BinlogReader) error

func WithReaderDecryptionContext(ezID, collectionID int64) BinlogReaderOption {
	return func(base *BinlogReader) error {
		edek, ok := base.descriptorEvent.GetEdek()
		if !ok {
			return nil
		}

		decryptor, err := hookutil.GetCipher().GetDecryptor(ezID, collectionID, []byte(edek))
		if err != nil {
			log.Error("failed to get decryptor", zap.Int64("ezID", ezID), zap.Int64("collectionID", collectionID), zap.Error(err))
			return err
		}

		cipherText := make([]byte, base.buffer.Len())
		if err := binary.Read(base.buffer, common.Endian, cipherText); err != nil {
			return err
		}

		log.Debug("Binlog reader starts to decypt cipher text",
			zap.Int64("collectionID", collectionID),
			zap.Int64("fieldID", base.descriptorEvent.FieldID),
			zap.Int("cipher size", len(cipherText)),
		)
		decrypted, err := decryptor.Decrypt(cipherText)
		if err != nil {
			log.Error("failed to decrypt", zap.Int64("ezID", ezID), zap.Int64("collectionID", collectionID), zap.Error(err))
			return err
		}
		log.Debug("Binlog reader decrypted cipher text",
			zap.Int64("collectionID", collectionID),
			zap.Int64("fieldID", base.descriptorEvent.FieldID),
			zap.Int("cipher size", len(cipherText)),
			zap.Int("plain size", len(decrypted)),
		)
		base.buffer = bytes.NewBuffer(decrypted)
		return nil
	}
}

// NewBinlogReader creates binlogReader to read binlog file.
func NewBinlogReader(data []byte, opts ...BinlogReaderOption) (*BinlogReader, error) {
	buffer := bytes.NewBuffer(data)
	if _, err := readMagicNumber(buffer); err != nil {
		return nil, err
	}

	descriptor, err := ReadDescriptorEvent(buffer)
	if err != nil {
		return nil, err
	}

	reader := BinlogReader{
		isClose:         false,
		descriptorEvent: *descriptor,
		buffer:          buffer,
	}

	for _, opt := range opts {
		if err := opt(&reader); err != nil {
			return nil, err
		}
	}

	return &reader, nil
}
