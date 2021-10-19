// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/milvus-io/milvus/internal/kv"
)

// GetBinlogSize get size of a binlog file.
//		normal binlog file, error = nil;
//		key not exist, size = 0, error = nil;
//		key not in binlog format, size = (a not accurate number), error != nil;
//		failed to read event reader, size = (a not accurate number), error != nil;
func GetBinlogSize(kv kv.DataKV, key string) (int64, error) {
	total := int64(0)

	header := &baseEventHeader{}
	headerSize := binary.Size(header)

	startPos := binary.Size(MagicNumber)
	endPos := startPos + headerSize

	for {
		headerContent, err := kv.LoadPartial(key, int64(startPos), int64(endPos))
		if err != nil {
			// case 1: key not exist, total = 0;
			// case 2: all events have been read, total = (length of all events);
			// whatever the case is, the return value is reasonable.
			return total, nil
		}

		buffer := bytes.NewBuffer(headerContent)

		header, err := readEventHeader(buffer)
		if err != nil {
			// FIXME(dragondriver): should we return 0 here?
			return total, fmt.Errorf("failed to read event reader: %v", err)
		}

		if header.EventLength <= 0 || header.NextPosition < int32(endPos) {
			// key not in binlog format
			// FIXME(dragondriver): should we return 0 here?
			return total, fmt.Errorf("key not in binlog format")
		}

		total += int64(header.EventLength)
		// startPos = startPos + int(header.EventLength)
		// 		||
		// 		\/
		startPos = int(header.NextPosition)
		endPos = startPos + headerSize
	}
}
