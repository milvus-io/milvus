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
	"errors"
	"fmt"
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

	pkStat pkStatistics

	startPos *internalpb.MsgPosition // TODO readonly
	endPos   *internalpb.MsgPosition
}

// pkStatistics contains pk field statistic information
type pkStatistics struct {
	statsChanged bool               //  statistic changed
	pkFilter     *bloom.BloomFilter //  bloom filter of pk inside a segment
	minPK        primaryKey         //	minimal pk value, shortcut for checking whether a pk is inside this segment
	maxPK        primaryKey         //  maximal pk value, same above
}

// update set pk min/max value if input value is beyond former range.
func (st *pkStatistics) update(pk primaryKey) error {
	if st == nil {
		return errors.New("nil pk statistics")
	}
	if st.minPK == nil {
		st.minPK = pk
	} else if st.minPK.GT(pk) {
		st.minPK = pk
	}

	if st.maxPK == nil {
		st.maxPK = pk
	} else if st.maxPK.LT(pk) {
		st.maxPK = pk
	}

	return nil
}

func (st *pkStatistics) updatePKRange(ids storage.FieldData) error {
	switch pks := ids.(type) {
	case *storage.Int64FieldData:
		buf := make([]byte, 8)
		for _, pk := range pks.Data {
			id := storage.NewInt64PrimaryKey(pk)
			err := st.update(id)
			if err != nil {
				return err
			}
			common.Endian.PutUint64(buf, uint64(pk))
			st.pkFilter.Add(buf)
		}
	case *storage.StringFieldData:
		for _, pk := range pks.Data {
			id := storage.NewVarCharPrimaryKey(pk)
			err := st.update(id)
			if err != nil {
				return err
			}
			st.pkFilter.AddString(pk)
		}
	default:
		return fmt.Errorf("invalid data type for primary key: %T", ids)
	}

	// mark statistic updated
	st.statsChanged = true

	return nil
}

// getStatslog return marshaled statslog content if there is any change since last call.
// statslog is marshaled as json.
func (st *pkStatistics) getStatslog(segmentID, pkID UniqueID, pkType schemapb.DataType) ([]byte, error) {
	if !st.statsChanged {
		return nil, fmt.Errorf("%w segment %d", errSegmentStatsNotChanged, segmentID)
	}

	pks := storage.PrimaryKeyStats{
		FieldID: pkID,
		PkType:  int64(pkType),
		MaxPk:   st.maxPK,
		MinPk:   st.minPK,
		BF:      st.pkFilter,
	}

	bs, err := json.Marshal(pks)
	if err == nil {
		st.statsChanged = false
	}
	return bs, err
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
	return s.pkStat.update(pk)
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
	log := log.With(zap.Int64("collectionID", s.collectionID),
		zap.Int64("partitionID", s.partitionID),
		zap.Int64("segmentID", s.segmentID),
	)

	err := s.pkStat.updatePKRange(ids)
	if err != nil {
		log.Warn("failed to updatePKRange", zap.Error(err))
	}

	log.Info("update pk range",
		zap.Int64("num_rows", s.numRows), zap.Any("minPK", s.pkStat.minPK), zap.Any("maxPK", s.pkStat.maxPK))

	return nil
}

func (s *Segment) getSegmentStatslog(pkID UniqueID, pkType schemapb.DataType) ([]byte, error) {
	return s.pkStat.getStatslog(s.segmentID, pkID, pkType)
}
