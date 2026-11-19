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
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		return nil, merr.WrapErrStorageMsg("bin log reader is closed")
	}
	if reader.buffer.Len() <= 0 {
		return nil, nil
	}
	// close the previous reader
	if reader.eventReader != nil {
		reader.eventReader.Close()
	}
	nullable, err := reader.GetNullable()
	if err != nil {
		return nil, err
	}
	reader.eventReader, err = newEventReader(reader.PayloadDataType, reader.buffer, nullable)
	if err != nil {
		if merr.IsMilvusError(err) {
			return nil, err
		}
		// The event is decoded from an in-memory buffer: a raw failure is the
		// file's content, not a transport fault.
		return nil, merr.WrapErrDataIntegrityMsg("parse binlog event failed, error: %v", err)
	}
	return reader.eventReader, nil
}

func readMagicNumber(buffer io.Reader) (int32, error) {
	var magicNumber int32
	if err := binary.Read(buffer, common.Endian, &magicNumber); err != nil {
		return -1, err
	}
	if magicNumber != MagicNumber {
		return -1, merr.WrapErrServiceInternalMsg("parse magic number failed, expected: %d, actual: %d", MagicNumber, magicNumber)
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

type importBinlogDecryptionError struct{ error }

func (e *importBinlogDecryptionError) Unwrap() error { return e.error }

// IsImportBinlogDecryptionError identifies a recognized legacy file. Its key
// failure must reach the task even when mixed-format deltalogs are being probed.
func IsImportBinlogDecryptionError(err error) bool {
	var failure *importBinlogDecryptionError
	return errors.As(err, &failure)
}

func withImportBinlogDecryption(source *indexcgopb.StoragePluginContext) BinlogReaderOption {
	return func(file *BinlogReader) error {
		edek, hasEDEK := file.GetEdek()
		_, hasZone := file.Extras[ezIDKey]
		if !hasEDEK && !hasZone {
			return nil
		}
		zone, ok := file.GetEzID()
		if edek == "" || !ok || zone <= 0 {
			return &importBinlogDecryptionError{merr.WrapErrDataIntegrityMsg("backup binlog has invalid encryption metadata")}
		}
		if source == nil || zone != source.GetEncryptionZoneId() {
			return &importBinlogDecryptionError{merr.WrapErrDataIntegrityMsg("backup binlog EZ %d does not match the dispatched source", zone)}
		}
		if err := hookutil.RegisterImportReadKeys(source); err != nil {
			return &importBinlogDecryptionError{err}
		}
		// The source collection and EDEK are recorded in the file. The
		// collection in the task request belongs to the destination.
		if err := WithReaderDecryptionContext(zone, file.CollectionID)(file); err != nil {
			return &importBinlogDecryptionError{err}
		}
		return nil
	}
}

func WithReaderDecryptionContext(ezID, collectionID int64) BinlogReaderOption {
	return func(base *BinlogReader) error {
		edek, ok := base.GetEdek()
		if !ok {
			return nil
		}

		decryptor, err := hookutil.GetCipher().GetDecryptor(ezID, collectionID, []byte(edek))
		if err != nil {
			mlog.Error(context.TODO(), "failed to get decryptor", mlog.Int64("ezID", ezID), mlog.Int64("collectionID", collectionID), mlog.Err(err))
			return err
		}

		cipherText := make([]byte, base.buffer.Len())
		if err := binary.Read(base.buffer, common.Endian, cipherText); err != nil {
			return err
		}

		mlog.Debug(context.TODO(), "Binlog reader starts to decypt cipher text",
			mlog.Int64("collectionID", collectionID),
			mlog.Int64("fieldID", base.FieldID),
			mlog.Int("cipher size", len(cipherText)),
		)
		decrypted, err := decryptor.Decrypt(cipherText)
		if err != nil {
			mlog.Error(context.TODO(), "failed to decrypt", mlog.Int64("ezID", ezID), mlog.Int64("collectionID", collectionID), mlog.Err(err))
			return err
		}
		mlog.Debug(context.TODO(), "Binlog reader decrypted cipher text",
			mlog.Int64("collectionID", collectionID),
			mlog.Int64("fieldID", base.FieldID),
			mlog.Int("cipher size", len(cipherText)),
			mlog.Int("plain size", len(decrypted)),
		)
		base.buffer = bytes.NewBuffer(decrypted)
		return nil
	}
}

// NewBinlogReader creates binlogReader to read binlog file.
func NewBinlogReader(data []byte, opts ...BinlogReaderOption) (*BinlogReader, error) {
	buffer := bytes.NewBuffer(data)
	if _, err := readMagicNumber(buffer); err != nil {
		if merr.IsMilvusError(err) {
			return nil, err
		}
		// The buffer is already in memory: a raw read failure here is the file's
		// content, not a transport fault, so report it as data corruption
		// instead of an error a caller may retry forever.
		return nil, merr.WrapErrDataIntegrityMsg("parse binlog magic number failed, error: %v", err)
	}

	descriptor, err := ReadDescriptorEvent(buffer)
	if err != nil {
		if merr.IsMilvusError(err) {
			return nil, err
		}
		return nil, merr.WrapErrDataIntegrityMsg("parse binlog descriptor failed, error: %v", err)
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
