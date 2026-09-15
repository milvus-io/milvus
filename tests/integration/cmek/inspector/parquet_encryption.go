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

package inspector

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/metadata"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
)

var encryptedParquetMagic = []byte("PARE")

// InspectEncryptedParquet verifies the physical encryption envelope and returns
// its EDEK. It does not obtain a key or invoke a Milvus production reader.
func InspectEncryptedParquet(raw []byte, expectedEZID, expectedCollectionID int64) (string, error) {
	if len(raw) <= 8 || !bytes.Equal(raw[:4], encryptedParquetMagic) || !bytes.Equal(raw[len(raw)-4:], encryptedParquetMagic) {
		return "", fmt.Errorf("object does not use an encrypted footer")
	}
	footerSize := int(binary.LittleEndian.Uint32(raw[len(raw)-8 : len(raw)-4]))
	footerStart := len(raw) - 8 - footerSize
	if footerSize <= 0 || footerStart < 4 {
		return "", fmt.Errorf("encrypted footer has invalid size %d", footerSize)
	}
	cryptoMetadata, err := metadata.NewFileCryptoMetaData(raw[footerStart : len(raw)-8])
	if err != nil {
		return "", fmt.Errorf("parse encrypted Parquet crypto metadata: %w", err)
	}
	if cryptoMetadata.EncryptionAlgorithm().Algo != parquet.AesGcm {
		return "", fmt.Errorf("encrypted Parquet does not use AES_GCM_V1")
	}
	parts := strings.SplitN(string(cryptoMetadata.KeyMetadata()), "_", 3)
	if len(parts) != 3 || parts[2] == "" {
		return "", fmt.Errorf("footer key metadata must be <ezID>_<collectionID>_<EDEK>")
	}
	ezID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", fmt.Errorf("footer key metadata has invalid EZ id %q: %w", parts[0], err)
	}
	if ezID != expectedEZID {
		return "", fmt.Errorf("footer key metadata EZ id %d, want %d", ezID, expectedEZID)
	}
	collectionID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", fmt.Errorf("footer key metadata has invalid collection id %q: %w", parts[1], err)
	}
	if collectionID != expectedCollectionID {
		return "", fmt.Errorf("footer key metadata collection id %d, want %d", collectionID, expectedCollectionID)
	}
	plainReader, plainErr := file.NewParquetReader(bytes.NewReader(raw))
	if plainReader != nil {
		_ = plainReader.Close()
	}
	if plainErr == nil {
		return "", fmt.Errorf("encrypted Parquet is readable without CMEK decryption information")
	}
	return parts[2], nil
}

// ReadEncryptedParquet reads all row groups with an independent Arrow Go reader.
// A nil key leaves decryption unconfigured. The caller owns the returned table.
// Each call creates fresh decryption properties and readers.
func ReadEncryptedParquet(raw, key []byte) (arrow.Table, error) {
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	if key != nil {
		if len(key) != 16 && len(key) != 24 && len(key) != 32 {
			return nil, errors.New("invalid AES key length")
		}
		props.FileDecryptProps = parquet.NewFileDecryptionProperties(parquet.WithFooterKey(string(key)))
	}
	reader, err := openParquetFooter(raw, props)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{Parallel: false}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	return arrowReader.ReadTable(context.Background())
}

// Arrow Go panics on GCM footer authentication failures. Recover this exact
// error only while opening the footer; payload reads have no panic recovery.
func openParquetFooter(raw []byte, props *parquet.ReaderProperties) (reader *file.Reader, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			authError, ok := failure.(error)
			if !ok || authError.Error() != "cipher: message authentication failed" {
				panic(failure)
			}
			err = authError
		}
	}()
	return file.NewParquetReader(bytes.NewReader(raw), file.WithReadProps(props))
}
