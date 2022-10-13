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

package datanode

import (
	"encoding/json"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
)

// Segment contains the latest segment infos from channel.
type Segment struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	sType        atomic.Value // datapb.SegmentType

	numRows     int64
	memorySize  int64
	compactedTo UniqueID

	startPos *internalpb.MsgPosition // TODO readonly
	endPos   *internalpb.MsgPosition

	pkFilter *bloom.BloomFilter //  bloom filter of pk inside a segment
	minPK    primaryKey         //	minimal pk value, shortcut for checking whether a pk is inside this segment
	maxPK    primaryKey         //  maximal pk value, same above
}

type addSegmentReq struct {
	segType                    datapb.SegmentType
	segID, collID, partitionID UniqueID
	numOfRows                  int64
	startPos, endPos           *internalpb.MsgPosition
	statsBinLogs               []*datapb.FieldBinlog
	recoverTs                  Timestamp
	importing                  bool
}

func (s *Segment) updatePk(pk primaryKey) error {
	if s.minPK == nil {
		s.minPK = pk
	} else if s.minPK.GT(pk) {
		s.minPK = pk
	}

	if s.maxPK == nil {
		s.maxPK = pk
	} else if s.maxPK.LT(pk) {
		s.maxPK = pk
	}

	return nil
}

func (s *Segment) isValid() bool {
	return s.getType() != datapb.SegmentType_Compacted
}

func (s *Segment) notFlushed() bool {
	return s.isValid() && s.getType() != datapb.SegmentType_Flushed
}

func (s *Segment) getType() datapb.SegmentType {
	return s.sType.Load().(datapb.SegmentType)
}

func (s *Segment) setType(t datapb.SegmentType) {
	s.sType.Store(t)
}

func (s *Segment) updatePKRange(ids storage.FieldData) error {
	switch pks := ids.(type) {
	case *storage.Int64FieldData:
		buf := make([]byte, 8)
		for _, pk := range pks.Data {
			id := storage.NewInt64PrimaryKey(pk)
			err := s.updatePk(id)
			if err != nil {
				return err
			}
			common.Endian.PutUint64(buf, uint64(pk))
			s.pkFilter.Add(buf)
		}
	case *storage.StringFieldData:
		for _, pk := range pks.Data {
			id := storage.NewVarCharPrimaryKey(pk)
			err := s.updatePk(id)
			if err != nil {
				return err
			}
			s.pkFilter.AddString(pk)
		}
	default:
		//TODO::
	}

	log.Info("update pk range",
		zap.Int64("collectionID", s.collectionID), zap.Int64("partitionID", s.partitionID), zap.Int64("segmentID", s.segmentID),
		zap.Int64("num_rows", s.numRows), zap.Any("minPK", s.minPK), zap.Any("maxPK", s.maxPK))

	return nil
}

func (s *Segment) getSegmentStatslog(pkID UniqueID, pkType schemapb.DataType) ([]byte, error) {
	pks := storage.PrimaryKeyStats{
		FieldID: pkID,
		PkType:  int64(pkType),
		MaxPk:   s.maxPK,
		MinPk:   s.minPK,
		BF:      s.pkFilter,
	}

	return json.Marshal(pks)
}
