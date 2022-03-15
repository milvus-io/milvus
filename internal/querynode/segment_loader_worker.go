package querynode

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"go.uber.org/zap"
)

const (
	loadSegmentWorkType = iota + 1
	loadFieldWorkType
	loadBinlogsWorkType
)

type loadSegmentParams struct {
	segment  *Segment
	loadInfo *querypb.SegmentLoadInfo
}

func newLoadSegmentParamsHandler(loader *segmentLoader) concurrency.Handler {
	return func(payload interface{}) interface{} {
		params := payload.(*loadSegmentParams)

		segment := params.segment
		loadInfo := params.loadInfo
		collectionID := loadInfo.CollectionID
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID

		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")

		err := loader.loadSegmentInternal(segment, loadInfo)
		if err != nil {
			log.Error("load segment failed when load data into memory",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Int32("segment type", int32(segment.getType())),
				zap.Error(err))
			return err
		}
		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(tr.ElapseSpan().Milliseconds()))

		return nil
	}
}

func (params *loadSegmentParams) GetType() concurrency.WorkType {
	return loadSegmentWorkType
}

type loadFieldParams struct {
	segment *Segment
	field   *datapb.FieldBinlog
}

func newLoadFieldParamsHandler(loader *segmentLoader) concurrency.Handler {
	return func(payload interface{}) interface{} {
		params := payload.(*loadFieldParams)

		blobs, err := loader.loadFieldBinlogs(params.field)
		if err != nil {
			return err
		}

		log.Debug("log field binlogs done",
			zap.Int("len(blobs)", len(blobs)))

		iCodec := storage.InsertCodec{}
		_, _, insertData, err := iCodec.Deserialize(blobs)
		if err != nil {
			return err
		}

		log.Debug("deserialize blobs done",
			zap.Int("len(insertData)", len(insertData.Data)))

		return loader.loadSealedSegments(params.segment, insertData)
	}
}

func (params *loadFieldParams) GetType() concurrency.WorkType {
	return loadFieldWorkType
}

type loadBinlogsParams struct {
	path     string
	blobChan chan *storage.Blob
}

func newLoadBinlogsParamsHandler(loader *segmentLoader) concurrency.Handler {
	return func(paramsI interface{}) interface{} {
		loadBinlogsParams := paramsI.(*loadBinlogsParams)
		binLog, err := loader.cm.Read(loadBinlogsParams.path)
		if err != nil {
			return err
		}
		blob := &storage.Blob{
			Key:   loadBinlogsParams.path,
			Value: binLog,
		}
		loadBinlogsParams.blobChan <- blob

		return nil
	}
}

func (params *loadBinlogsParams) GetType() concurrency.WorkType {
	return loadBinlogsWorkType
}
