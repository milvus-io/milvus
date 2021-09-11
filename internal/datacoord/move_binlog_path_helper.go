package datacoord

import (
	"fmt"
	"path"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"go.uber.org/zap"
)

type MoveBinlogPathHelper struct {
	kv   kv.TxnKV
	meta *meta
}

func NewMoveBinlogPathHelper(kv kv.TxnKV, meta *meta) *MoveBinlogPathHelper {
	return &MoveBinlogPathHelper{
		kv:   kv,
		meta: meta,
	}
}

func (h *MoveBinlogPathHelper) Execute() error {
	segmentIds := h.meta.ListSegmentIDs()

	if len(segmentIds) == 1 {
		log.Debug("there's 1 segment's binlogs to move", zap.Int64("segmentID", segmentIds[0]))
	} else {
		log.Debug(fmt.Sprintf("there are %d segments' binlogs to move", len(segmentIds)))
	}

	for _, id := range segmentIds {
		m := make(map[UniqueID][]string)
		p := path.Join(Params.SegmentBinlogSubPath, strconv.FormatInt(id, 10)) + "/" // prefix/id/ instead of prefix/id
		_, values, err := h.kv.LoadWithPrefix(p)
		if err != nil {
			log.Error("failed to load prefix", zap.String("prefix", p), zap.Error(err))
			return err
		}
		for _, v := range values {
			tMeta := &datapb.SegmentFieldBinlogMeta{}
			if err := proto.UnmarshalText(v, tMeta); err != nil {
				log.Error("failed to unmarshal", zap.Error(err))
				return err
			}
			m[tMeta.FieldID] = append(m[tMeta.FieldID], tMeta.BinlogPath)
		}

		if err := h.meta.MoveSegmentBinlogs(id, p, m); err != nil {
			log.Error("failed to save binlogs in meta", zap.Int64("segmentID", id), zap.Error(err))
			return err
		}

		log.Debug(fmt.Sprintf("success to move binlogs of segment %d", id))
	}

	return nil
}
