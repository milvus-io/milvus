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

package binlog

import (
	"context"
	"encoding/json"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus-storage/go/common/log"
	"github.com/milvus-io/milvus/internal/storage"
	"go.uber.org/zap"
	"io"
)

type L0Reader interface {
	Read() (*storage.DeleteData, error)
}

type l0Reader struct {
	ctx     context.Context
	cm      storage.ChunkManager
	pkField *schemapb.FieldSchema

	bufferSize int
	deltaLogs  []string
	readIdx    int
}

func NewL0Reader(ctx context.Context,
	cm storage.ChunkManager,
	pkField *schemapb.FieldSchema,
	path string,
	bufferSize int,
) (*l0Reader, error) {
	r := &l0Reader{
		ctx:        ctx,
		cm:         cm,
		pkField:    pkField,
		bufferSize: bufferSize,
	}
	deltaLogs, _, err := storage.ListAllChunkWithPrefix(context.Background(), r.cm, path, true)
	if err != nil {
		return nil, err
	}
	if len(deltaLogs) == 0 {
		log.Info("no delta logs for l0 segments", zap.String("prefix", path))
	}
	r.deltaLogs = deltaLogs
	return r, nil
}

func (r *l0Reader) Read() (*storage.DeleteData, error) {
	deleteData := storage.NewDeleteData(nil, nil)
	for {
		if r.readIdx == len(r.deltaLogs) {
			return nil, io.EOF
		}
		path := r.deltaLogs[r.readIdx]
		br, err := newBinlogReader(r.ctx, r.cm, path)
		if err != nil {
			return nil, err
		}
		rowsSet, err := readData(br, storage.DeleteEventType)
		if err != nil {
			return nil, err
		}
		for _, rows := range rowsSet {
			for _, row := range rows.([]string) {
				dl := &storage.DeleteLog{}
				err = json.Unmarshal([]byte(row), dl)
				if err != nil {
					return nil, err
				}
			}
		}
		r.readIdx++
		if deleteData.Size() >= int64(r.bufferSize) {
			break
		}
	}
	return deleteData, nil
}
