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

package delegator

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
)

// BufferForwarder is a util object to buffer delta data
// when buffer size reaches the designed size,
// it shall forward buffered data to worker via Delete API.
type BufferForwarder struct {
	bufferSize int64
	buffer     *storage.DeltaData

	doSync func(pks storage.PrimaryKeys, tss []uint64) error
}

// NewBufferedForwarder creates a BufferForwarder with max size
// and `doSync` op function
func NewBufferedForwarder(bufferSize int64, doSync func(pks storage.PrimaryKeys, tss []uint64) error) *BufferForwarder {
	return &BufferForwarder{
		bufferSize: bufferSize,
		buffer:     storage.NewDeltaData(1024),
		doSync:     doSync,
	}
}

// deleteViaWorker is the util func for doSync impl which calls worker.Delete
func deleteViaWorker(ctx context.Context,
	worker cluster.Worker,
	workerID int64,
	info *querypb.SegmentLoadInfo,
	deleteScope querypb.DataScope,
) func(pks storage.PrimaryKeys, tss []uint64) error {
	return func(pks storage.PrimaryKeys, tss []uint64) error {
		ids, err := storage.ParsePrimaryKeysBatch2IDs(pks)
		if err != nil {
			return err
		}
		return worker.Delete(ctx, &querypb.DeleteRequest{
			Base:         commonpbutil.NewMsgBase(commonpbutil.WithTargetID(workerID)),
			CollectionId: info.GetCollectionID(),
			PartitionId:  info.GetPartitionID(),
			SegmentId:    info.GetSegmentID(),
			PrimaryKeys:  ids,
			Timestamps:   tss,
			Scope:        deleteScope,
			UseLoad:      true,
		})
	}
}

func (bf *BufferForwarder) Buffer(pk storage.PrimaryKey, ts uint64) error {
	if err := bf.buffer.Append(pk, ts); err != nil {
		return err
	}

	if bf.buffer.MemSize() > bf.bufferSize {
		if err := bf.sync(); err != nil {
			return err
		}
	}
	return nil
}

func (bf *BufferForwarder) sync() error {
	if bf.buffer.DeleteRowCount() == 0 {
		return nil
	}

	if err := bf.doSync(bf.buffer.DeletePks(), bf.buffer.DeleteTimestamps()); err != nil {
		return err
	}

	bf.buffer.Reset()
	return nil
}

func (bf *BufferForwarder) Flush() error {
	return bf.sync()
}
