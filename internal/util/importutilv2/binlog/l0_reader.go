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
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
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
	importFile *internalpb.ImportFile,
	bufferSize int,
) (*l0Reader, error) {
	r := &l0Reader{
		ctx:        ctx,
		cm:         cm,
		pkField:    pkField,
		bufferSize: bufferSize,
	}
	if len(importFile.GetPaths()) != 1 {
		return nil, merr.WrapErrImportFailed(
			fmt.Sprintf("there should be one prefix, but got %s", importFile.GetPaths()))
	}
	path := importFile.GetPaths()[0]
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
			if deleteData.RowCount != 0 {
				return deleteData, nil
			}
			return nil, io.EOF
		}
		path := r.deltaLogs[r.readIdx]

		bytes, err := r.cm.Read(r.ctx, path)
		if err != nil {
			return nil, err
		}
		blobs := []*storage.Blob{{
			Key:   path,
			Value: bytes,
		}}
		// TODO: support multiple delta logs
		reader, err := storage.CreateDeltalogReader(blobs)
		if err != nil {
			log.Error("malformed delta file", zap.Error(err))
			return nil, err
		}
		defer reader.Close()

		for {
			err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Error("error on importing L0 segment, fail to read deltalogs", zap.Error(err))
				return nil, err
			}

			dl := reader.Value()
			deleteData.Append(dl.Pk, dl.Ts)
		}

		r.readIdx++
		if deleteData.Size() >= int64(r.bufferSize) {
			break
		}
	}
	return deleteData, nil
}
