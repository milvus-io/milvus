package querynode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

const indexCheckInterval = 1

type loadService struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *minioKV.MinIOKV

	queryNodeID UniqueID
	replica     collectionReplica

	fieldIndexes   map[string][]*internalpb2.IndexStats
	fieldStatsChan chan []*internalpb2.FieldStats

	loadIndexReqChan   chan []msgstream.TsMsg
	loadIndexMsgStream msgstream.MsgStream

	segManager *segmentManager
}

func (lis *loadService) consume() {
	for {
		select {
		case <-lis.ctx.Done():
			return
		default:
			messages := lis.loadIndexMsgStream.Consume()
			if messages == nil || len(messages.Msgs) <= 0 {
				log.Println("null msg pack")
				continue
			}
			lis.loadIndexReqChan <- messages.Msgs
		}
	}
}

func (lis *loadService) indexListener() {
	for {
		select {
		case <-lis.ctx.Done():
			return
		case <-time.After(indexCheckInterval * time.Second):
			collectionIDs, segmentIDs := lis.replica.getSealedSegments()
			for i := range collectionIDs {
				// we don't need index id yet
				_, buildID, err := lis.segManager.getIndexInfo(collectionIDs[i], segmentIDs[i])
				if err != nil {
					indexPaths, err := lis.segManager.getIndexPaths(buildID)
					if err != nil {
						log.Println(err)
						continue
					}
					err = lis.segManager.loadIndex(segmentIDs[i], indexPaths)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}
		}
	}
}

func (lis *loadService) start() {
	lis.loadIndexMsgStream.Start()
	go lis.consume()
	go lis.indexListener()

	for {
		select {
		case <-lis.ctx.Done():
			return
		case messages := <-lis.loadIndexReqChan:
			for _, msg := range messages {
				err := lis.execute(msg)
				if err != nil {
					log.Println(err)
					continue
				}
			}

			// sendQueryNodeStats
			err := lis.sendQueryNodeStats()
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
}

func (lis *loadService) execute(msg msgstream.TsMsg) error {
	indexMsg, ok := msg.(*msgstream.LoadIndexMsg)
	if !ok {
		return errors.New("type assertion failed for LoadIndexMsg")
	}
	// 1. use msg's index paths to get index bytes
	var err error
	var indexBuffer [][]byte
	var indexParams indexParam
	fn := func() error {
		indexBuffer, indexParams, err = lis.loadIndex(indexMsg.IndexPaths)
		if err != nil {
			return err
		}
		return nil
	}
	err = util.Retry(5, time.Millisecond*200, fn)
	if err != nil {
		return err
	}
	ok, err = lis.checkIndexReady(indexParams, indexMsg)
	if err != nil {
		return err
	}
	if ok {
		// no error
		return errors.New("")
	}
	// 2. use index bytes and index path to update segment
	err = lis.updateSegmentIndex(indexParams, indexBuffer, indexMsg)
	if err != nil {
		return err
	}
	//3. update segment index stats
	err = lis.updateSegmentIndexStats(indexParams, indexMsg)
	if err != nil {
		return err
	}
	fmt.Println("load index done")
	return nil
}

func (lis *loadService) close() {
	if lis.loadIndexMsgStream != nil {
		lis.loadIndexMsgStream.Close()
	}
	lis.cancel()
}

func (lis *loadService) printIndexParams(index []*commonpb.KeyValuePair) {
	fmt.Println("=================================================")
	for i := 0; i < len(index); i++ {
		fmt.Println(index[i])
	}
}

func (lis *loadService) indexParamsEqual(index1 []*commonpb.KeyValuePair, index2 []*commonpb.KeyValuePair) bool {
	if len(index1) != len(index2) {
		return false
	}

	for i := 0; i < len(index1); i++ {
		kv1 := *index1[i]
		kv2 := *index2[i]
		if kv1.Key != kv2.Key || kv1.Value != kv2.Value {
			return false
		}
	}

	return true
}

func (lis *loadService) fieldsStatsIDs2Key(collectionID UniqueID, fieldID UniqueID) string {
	return strconv.FormatInt(collectionID, 10) + "/" + strconv.FormatInt(fieldID, 10)
}

func (lis *loadService) fieldsStatsKey2IDs(key string) (UniqueID, UniqueID, error) {
	ids := strings.Split(key, "/")
	if len(ids) != 2 {
		return 0, 0, errors.New("illegal fieldsStatsKey")
	}
	collectionID, err := strconv.ParseInt(ids[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	fieldID, err := strconv.ParseInt(ids[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return collectionID, fieldID, nil
}

func (lis *loadService) updateSegmentIndexStats(indexParams indexParam, indexMsg *msgstream.LoadIndexMsg) error {
	targetSegment, err := lis.replica.getSegmentByID(indexMsg.SegmentID)
	if err != nil {
		return err
	}

	fieldStatsKey := lis.fieldsStatsIDs2Key(targetSegment.collectionID, indexMsg.FieldID)
	_, ok := lis.fieldIndexes[fieldStatsKey]
	newIndexParams := make([]*commonpb.KeyValuePair, 0)
	for k, v := range indexParams {
		newIndexParams = append(newIndexParams, &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		})
	}

	// sort index params by key
	sort.Slice(newIndexParams, func(i, j int) bool { return newIndexParams[i].Key < newIndexParams[j].Key })
	if !ok {
		lis.fieldIndexes[fieldStatsKey] = make([]*internalpb2.IndexStats, 0)
		lis.fieldIndexes[fieldStatsKey] = append(lis.fieldIndexes[fieldStatsKey],
			&internalpb2.IndexStats{
				IndexParams:        newIndexParams,
				NumRelatedSegments: 1,
			})
	} else {
		isNewIndex := true
		for _, index := range lis.fieldIndexes[fieldStatsKey] {
			if lis.indexParamsEqual(newIndexParams, index.IndexParams) {
				index.NumRelatedSegments++
				isNewIndex = false
			}
		}
		if isNewIndex {
			lis.fieldIndexes[fieldStatsKey] = append(lis.fieldIndexes[fieldStatsKey],
				&internalpb2.IndexStats{
					IndexParams:        newIndexParams,
					NumRelatedSegments: 1,
				})
		}
	}
	return targetSegment.setIndexParam(indexMsg.FieldID, indexMsg.IndexParams)
}

func (lis *loadService) loadIndex(indexPath []string) ([][]byte, indexParam, error) {
	index := make([][]byte, 0)

	var indexParams indexParam
	for _, p := range indexPath {
		fmt.Println("load path = ", indexPath)
		indexPiece, err := (*lis.client).Load(p)
		if err != nil {
			return nil, nil, err
		}
		// get index params when detecting indexParamPrefix
		if path.Base(p) == storage.IndexParamsFile {
			indexCodec := storage.NewIndexCodec()
			_, indexParams, err = indexCodec.Deserialize([]*storage.Blob{
				{
					Key:   storage.IndexParamsFile,
					Value: []byte(indexPiece),
				},
			})
			if err != nil {
				return nil, nil, err
			}
		} else {
			index = append(index, []byte(indexPiece))
		}
	}

	if len(indexParams) <= 0 {
		return nil, nil, errors.New("cannot find index param")
	}
	return index, indexParams, nil
}

func (lis *loadService) updateSegmentIndex(indexParams indexParam, bytesIndex [][]byte, loadIndexMsg *msgstream.LoadIndexMsg) error {
	segment, err := lis.replica.getSegmentByID(loadIndexMsg.SegmentID)
	if err != nil {
		return err
	}

	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}
	err = loadIndexInfo.appendFieldInfo(loadIndexMsg.FieldName, loadIndexMsg.FieldID)
	if err != nil {
		return err
	}
	for k, v := range indexParams {
		err = loadIndexInfo.appendIndexParam(k, v)
		if err != nil {
			return err
		}
	}
	err = loadIndexInfo.appendIndex(bytesIndex, loadIndexMsg.IndexPaths)
	if err != nil {
		return err
	}
	return segment.updateSegmentIndex(loadIndexInfo)
}

func (lis *loadService) sendQueryNodeStats() error {
	resultFieldsStats := make([]*internalpb2.FieldStats, 0)
	for fieldStatsKey, indexStats := range lis.fieldIndexes {
		colID, fieldID, err := lis.fieldsStatsKey2IDs(fieldStatsKey)
		if err != nil {
			return err
		}
		fieldStats := internalpb2.FieldStats{
			CollectionID: colID,
			FieldID:      fieldID,
			IndexStats:   indexStats,
		}
		resultFieldsStats = append(resultFieldsStats, &fieldStats)
	}

	lis.fieldStatsChan <- resultFieldsStats
	fmt.Println("sent field stats")
	return nil
}

func (lis *loadService) checkIndexReady(indexParams indexParam, loadIndexMsg *msgstream.LoadIndexMsg) (bool, error) {
	segment, err := lis.replica.getSegmentByID(loadIndexMsg.SegmentID)
	if err != nil {
		return false, err
	}
	if !segment.matchIndexParam(loadIndexMsg.FieldID, indexParams) {
		return false, nil
	}
	return true, nil

}

func newLoadService(ctx context.Context, masterClient MasterServiceInterface, dataClient DataServiceInterface, indexClient IndexServiceInterface, replica collectionReplica, dmStream msgstream.MsgStream) *loadService {
	ctx1, cancel := context.WithCancel(ctx)

	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		CreateBucket:      true,
		BucketName:        Params.MinioBucketName,
	}

	MinioKV, err := minioKV.NewMinIOKV(ctx1, option)
	if err != nil {
		panic(err)
	}

	// init msgStream
	receiveBufSize := Params.LoadIndexReceiveBufSize
	pulsarBufSize := Params.LoadIndexPulsarBufSize

	msgStreamURL := Params.PulsarAddress

	consumeChannels := Params.LoadIndexChannelNames
	consumeSubName := Params.MsgChannelSubName

	loadIndexStream := pulsarms.NewPulsarMsgStream(ctx, receiveBufSize)
	loadIndexStream.SetPulsarClient(msgStreamURL)
	unmarshalDispatcher := util.NewUnmarshalDispatcher()
	loadIndexStream.CreatePulsarConsumers(consumeChannels, consumeSubName, unmarshalDispatcher, pulsarBufSize)

	var stream msgstream.MsgStream = loadIndexStream

	// init index load requests channel size by message receive buffer size
	indexLoadChanSize := receiveBufSize

	// init segment manager
	loadIndexReqChan := make(chan []msgstream.TsMsg, indexLoadChanSize)
	manager := newSegmentManager(ctx1, masterClient, dataClient, indexClient, replica, dmStream, loadIndexReqChan)

	return &loadService{
		ctx:    ctx1,
		cancel: cancel,
		client: MinioKV,

		replica:        replica,
		queryNodeID:    Params.QueryNodeID,
		fieldIndexes:   make(map[string][]*internalpb2.IndexStats),
		fieldStatsChan: make(chan []*internalpb2.FieldStats, 1),

		loadIndexReqChan:   loadIndexReqChan,
		loadIndexMsgStream: stream,

		segManager: manager,
	}
}
