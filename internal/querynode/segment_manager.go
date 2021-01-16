package querynode

import (
	"context"
	"errors"
	"fmt"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/querynode/client"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type segmentManager struct {
	replica collectionReplica

	// TODO: replace by client instead of grpc client
	dataClient         datapb.DataServiceClient
	indexBuilderClient indexpb.IndexServiceClient

	queryNodeClient *client.Client
	kv              kv.Base // minio kv
	iCodec          storage.InsertCodec
}

func (s *segmentManager) loadSegment(segmentID UniqueID, hasBeenBuiltIndex bool, indexID UniqueID, vecFieldIDs []int64) error {
	// 1. load segment
	req := &datapb.InsertBinlogPathRequest{
		SegmentID: segmentID,
	}

	pathResponse, err := s.dataClient.GetInsertBinlogPaths(context.TODO(), req)
	if err != nil {
		return err
	}

	if len(pathResponse.FieldIDs) != len(pathResponse.Paths) {
		return errors.New("illegal InsertBinlogPathsResponse")
	}

	for fieldID, i := range pathResponse.FieldIDs {
		paths := pathResponse.Paths[i].Values
		blobs := make([]*storage.Blob, 0)
		for _, path := range paths {
			binLog, err := s.kv.Load(path)
			if err != nil {
				// TODO: return or continue?
				return err
			}
			blobs = append(blobs, &storage.Blob{
				Key:   "", // TODO: key???
				Value: []byte(binLog),
			})
		}
		_, _, insertData, err := s.iCodec.Deserialize(blobs)
		if err != nil {
			// TODO: return or continue
			return err
		}
		if len(insertData.Data) != 1 {
			return errors.New("we expect only one field in deserialized insert data")
		}

		for _, value := range insertData.Data {
			switch fieldData := value.(type) {
			case storage.BoolFieldData:
				numRows := fieldData.NumRows
				data := fieldData.Data
				fmt.Println(numRows, data, fieldID)
				// TODO: s.replica.addSegment()
			case storage.Int8FieldData:
				// TODO: s.replica.addSegment()
			case storage.Int16FieldData:
				// TODO: s.replica.addSegment()
			case storage.Int32FieldData:
				// TODO: s.replica.addSegment()
			case storage.Int64FieldData:
				// TODO: s.replica.addSegment()
			case storage.FloatFieldData:
				// TODO: s.replica.addSegment()
			case storage.DoubleFieldData:
				// TODO: s.replica.addSegment()
			default:
				// TODO: what if the index has not been built ?
				// does the info from hasBeenBuiltIndex is synced with the dataService?
				return errors.New("unsupported field data type")
			}
		}
	}

	// 2. load index
	// does the info from hasBeenBuiltIndex is synced with the dataService?
	if !hasBeenBuiltIndex {
		req := &indexpb.IndexFilePathRequest{
			IndexID: indexID,
		}
		pathResponse, err := s.indexBuilderClient.GetIndexFilePaths(context.TODO(), req)
		if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			return err
		}
		targetSegment, err := s.replica.getSegmentByID(segmentID)
		if err != nil {
			return err
		}
		for _, vecFieldID := range vecFieldIDs {
			targetIndexParam, ok := targetSegment.indexParam[vecFieldID]
			if !ok {
				return errors.New(fmt.Sprint("cannot found index params in segment ", segmentID, " with field = ", vecFieldID))
			}
			err := s.queryNodeClient.LoadIndex(pathResponse.IndexFilePaths, segmentID, vecFieldID, "", targetIndexParam)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *segmentManager) releaseSegment(in *queryPb.ReleaseSegmentRequest) error {
	// TODO: implement
	// TODO: release specific field, we need segCore supply relevant interface
	return nil
}
