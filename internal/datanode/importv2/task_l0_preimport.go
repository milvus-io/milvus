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

package importv2

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type L0PreImportTask struct {
	*datapb.PreImportTask
	ctx          context.Context
	cancel       context.CancelFunc
	partitionIDs []int64
	vchannels    []string
	schema       *schemapb.CollectionSchema
	req          *datapb.PreImportRequest

	manager TaskManager
	cm      storage.ChunkManager
}

func NewL0PreImportTask(req *datapb.PreImportRequest,
	manager TaskManager,
	cm storage.ChunkManager,
) Task {
	fileStats := lo.Map(req.GetImportFiles(), func(file *internalpb.ImportFile, _ int) *datapb.ImportFileStats {
		return &datapb.ImportFileStats{
			ImportFile: file,
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	return &L0PreImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:        req.GetJobID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			State:        datapb.ImportTaskStateV2_Pending,
			FileStats:    fileStats,
		},
		ctx:          ctx,
		cancel:       cancel,
		partitionIDs: req.GetPartitionIDs(),
		vchannels:    req.GetVchannels(),
		schema:       req.GetSchema(),
		req:          req,
		manager:      manager,
		cm:           cm,
	}
}

func (t *L0PreImportTask) GetPartitionIDs() []int64 {
	return t.partitionIDs
}

func (t *L0PreImportTask) GetVchannels() []string {
	return t.vchannels
}

func (t *L0PreImportTask) GetType() TaskType {
	return L0PreImportTaskType
}

func (t *L0PreImportTask) GetSchema() *schemapb.CollectionSchema {
	return t.schema
}

func (t *L0PreImportTask) GetSlots() int64 {
	return t.req.GetTaskSlot()
}

// L0 preimport task buffer size is fixed
func (t *L0PreImportTask) GetBufferSize() int64 {
	return paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
}

func (t *L0PreImportTask) Cancel() {
	t.cancel()
}

func (t *L0PreImportTask) Clone() Task {
	ctx, cancel := context.WithCancel(t.ctx)
	return &L0PreImportTask{
		PreImportTask: typeutil.Clone(t.PreImportTask),
		ctx:           ctx,
		cancel:        cancel,
		partitionIDs:  t.GetPartitionIDs(),
		vchannels:     t.GetVchannels(),
		schema:        t.GetSchema(),
		req:           t.req,
		manager:       t.manager,
		cm:            t.cm,
	}
}

func (t *L0PreImportTask) Execute() []*conc.Future[any] {
	bufferSize := int(t.GetBufferSize())
	log.Info("start to preimport l0", WrapLogFields(t,
		zap.Int("bufferSize", bufferSize),
		zap.Int64("taskSlot", t.GetSlots()),
		zap.Any("files", t.req.GetImportFiles()),
		zap.Any("schema", t.GetSchema()),
	)...)
	t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_InProgress))
	files := lo.Map(t.GetFileStats(),
		func(fileStat *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
			return fileStat.GetImportFile()
		})

	fn := func(i int, file *internalpb.ImportFile) (err error) {
		defer func() {
			if err != nil {
				var reason string = err.Error()
				if len(t.GetFileStats()) == 1 {
					reason = fmt.Sprintf("error: %v, file: %s", err, t.GetFileStats()[0].GetImportFile().String())
				}
				log.Warn("l0 import task execute failed", WrapLogFields(t, zap.String("err", reason))...)
				t.manager.Update(t.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed), UpdateReason(reason))
			}
		}()
		pkField, err := typeutil.GetPrimaryFieldSchema(t.GetSchema())
		if err != nil {
			return
		}

		// Parse ts parameters from options
		tsStart, tsEnd, err := importutilv2.ParseTimeRange(t.req.GetOptions())
		if err != nil {
			return
		}

		reader, err := binlog.NewL0Reader(t.ctx, t.cm, pkField, file, bufferSize, tsStart, tsEnd)
		if err != nil {
			return
		}
		start := time.Now()
		err = t.readL0Stat(reader, i)
		if err != nil {
			return
		}
		log.Info("l0 preimport done", WrapLogFields(t,
			zap.Strings("l0 prefix", file.GetPaths()),
			zap.Duration("dur", time.Since(start)))...)
		return nil
	}

	futures := make([]*conc.Future[any], 0, len(files))
	for i, file := range files {
		i := i
		file := file
		f := GetExecPool().Submit(func() (any, error) {
			err := fn(i, file)
			return err, err
		})
		futures = append(futures, f)
	}
	return futures
}

func (t *L0PreImportTask) readL0Stat(reader binlog.L0Reader, fileIdx int) error {
	totalRows := 0
	totalSize := 0
	hashedStats := make(map[string]*datapb.PartitionImportStats)
	for {
		data, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		stats, err := GetDeleteStats(t, data)
		if err != nil {
			return err
		}
		MergeHashedStats(stats, hashedStats)
		rows := int(data.RowCount)
		size := int(data.Size())
		totalRows += rows
		totalSize += size
		log.Info("reading l0 stat...", WrapLogFields(t, zap.Int("readRows", rows), zap.Int("readSize", size))...)
	}

	stat := &datapb.ImportFileStats{
		TotalRows:       int64(totalRows),
		TotalMemorySize: int64(totalSize),
		HashedStats:     hashedStats,
	}
	t.manager.Update(t.GetTaskID(), UpdateFileStat(fileIdx, stat))
	return nil
}
