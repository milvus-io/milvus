package proxy

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	pb "github.com/czs007/suvlim/internal/proto/message"
	"github.com/golang/protobuf/proto"
	"log"
	"sort"
	"sync"
)

type queryReq struct {
	pb.QueryReqMsg
	result []*pb.QueryResult
	wg     sync.WaitGroup
	proxy  *proxyServer
}

// BaseRequest interfaces
func (req *queryReq) Type() pb.ReqType {
	return req.ReqType
}

func (req *queryReq) PreExecute() pb.Status {
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (req *queryReq) Execute() pb.Status {
	req.proxy.reqSch.queryChan <- req
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (req *queryReq) PostExecute() pb.Status { // send into pulsar
	req.wg.Add(1)
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (req *queryReq) WaitToFinish() pb.Status { // wait unitl send into pulsar
	req.wg.Wait()
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (s *proxyServer) restartQueryRoutine(buf_size int) error {
	s.reqSch.queryChan = make(chan *queryReq, buf_size)
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: s.pulsarAddr})
	if err != nil {
		return nil
	}
	query, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: s.queryTopic})
	if err != nil {
		return err
	}

	result, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:                       s.resultTopic,
		SubscriptionName:            s.resultGroup,
		Type:                        pulsar.KeyShared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		return err
	}

	resultMap := make(map[uint64]*queryReq)

	go func() {
		defer result.Close()
		defer query.Close()
		defer pulsarClient.Close()
		for {
			select {
			case <-s.ctx.Done():
				return
			case qm := <-s.reqSch.queryChan:
				ts, st := s.getTimestamp(1)
				if st.ErrorCode != pb.ErrorCode_SUCCESS {
					log.Printf("get time stamp failed, error code = %d, msg = %s", st.ErrorCode, st.Reason)
					break
				}

				q := pb.QueryReqMsg{
					CollectionName: qm.CollectionName,
					VectorParam:    qm.VectorParam,
					PartitionTags:  qm.PartitionTags,
					Dsl:            qm.Dsl,
					ExtraParams:    qm.ExtraParams,
					Timestamp:      uint64(ts[0]),
					ProxyId:        qm.ProxyId,
					QueryId:        qm.QueryId,
					ReqType:        qm.ReqType,
				}

				qb, err := proto.Marshal(&q)
				if err != nil {
					log.Printf("Marshal QueryReqMsg failed, error = %v", err)
					continue
				}
				if _, err := query.Send(s.ctx, &pulsar.ProducerMessage{Payload: qb}); err != nil {
					log.Printf("post into puslar failed, error = %v", err)
				}
				s.reqSch.q_timestamp_mux.Lock()
				if s.reqSch.q_timestamp <= ts[0] {
					s.reqSch.q_timestamp = ts[0]
				} else {
					log.Printf("there is some wrong with q_timestamp, it goes back, current = %d, previous = %d", ts[0], s.reqSch.q_timestamp)
				}
				s.reqSch.q_timestamp_mux.Unlock()
				resultMap[qm.QueryId] = qm
				//log.Printf("start search, query id = %d", qm.QueryId)
			case cm, ok := <-result.Chan():
				if !ok {
					log.Printf("consumer of result topic has closed")
					return
				}
				var rm pb.QueryResult
				if err := proto.Unmarshal(cm.Message.Payload(), &rm); err != nil {
					log.Printf("Unmarshal QueryReqMsg failed, error = %v", err)
					break
				}
				if rm.ProxyId != s.proxyId {
					break
				}
				qm, ok := resultMap[rm.QueryId]
				if !ok {
					log.Printf("unknown query id = %d", rm.QueryId)
					break
				}
				qm.result = append(qm.result, &rm)
				if len(qm.result) == s.numReaderNode {
					qm.wg.Done()
					delete(resultMap, rm.QueryId)
				}
				result.AckID(cm.ID())
			}

		}
	}()
	return nil
}

func (s *proxyServer) reduceResult(query *queryReq) *pb.QueryResult {
	if s.numReaderNode == 1 {
		return query.result[0]
	}
	var result []*pb.QueryResult
	for _, r := range query.result {
		if r.Status.ErrorCode == pb.ErrorCode_SUCCESS {
			result = append(result, r)
		}
	}
	if len(result) == 0 {
		return query.result[0]
	}
	if len(result) == 1 {
		return result[0]
	}

	var entities []*struct {
		Ids       int64
		ValidRow  bool
		RowsData  *pb.RowData
		Scores    float32
		Distances float32
	}
	var rows int

	result_err := func(msg string) *pb.QueryResult {
		return &pb.QueryResult{
			Status: &pb.Status{
				ErrorCode: pb.ErrorCode_UNEXPECTED_ERROR,
				Reason:    msg,
			},
		}
	}

	for _, r := range result {
		if len(r.Entities.Ids) > rows {
			rows = len(r.Entities.Ids)
		}
		if len(r.Entities.Ids) != len(r.Entities.ValidRow) {
			return result_err(fmt.Sprintf("len(Entities.Ids)=%d, len(Entities.ValidRow)=%d", len(r.Entities.Ids), len(r.Entities.ValidRow)))
		}
		if len(r.Entities.Ids) != len(r.Entities.RowsData) {
			return result_err(fmt.Sprintf("len(Entities.Ids)=%d, len(Entities.RowsData)=%d", len(r.Entities.Ids), len(r.Entities.RowsData)))
		}
		if len(r.Entities.Ids) != len(r.Scores) {
			return result_err(fmt.Sprintf("len(Entities.Ids)=%d, len(Scores)=%d", len(r.Entities.Ids), len(r.Scores)))
		}
		if len(r.Entities.Ids) != len(r.Distances) {
			return result_err(fmt.Sprintf("len(Entities.Ids)=%d, len(Distances)=%d", len(r.Entities.Ids), len(r.Distances)))
		}
		for i := 0; i < len(r.Entities.Ids); i++ {
			entity := struct {
				Ids       int64
				ValidRow  bool
				RowsData  *pb.RowData
				Scores    float32
				Distances float32
			}{
				Ids:       r.Entities.Ids[i],
				ValidRow:  r.Entities.ValidRow[i],
				RowsData:  r.Entities.RowsData[i],
				Scores:    r.Scores[i],
				Distances: r.Distances[i],
			}
			entities = append(entities, &entity)
		}
	}
	sort.Slice(entities, func(i, j int) bool {
		if entities[i].ValidRow == true {
			if entities[j].ValidRow == false {
				return true
			}
			return entities[i].Scores > entities[j].Scores
		} else {
			return false
		}
	})
	rIds := make([]int64, 0, rows)
	rValidRow := make([]bool, 0, rows)
	rRowsData := make([]*pb.RowData, 0, rows)
	rScores := make([]float32, 0, rows)
	rDistances := make([]float32, 0, rows)
	for i := 0; i < rows; i++ {
		rIds = append(rIds, entities[i].Ids)
		rValidRow = append(rValidRow, entities[i].ValidRow)
		rRowsData = append(rRowsData, entities[i].RowsData)
		rScores = append(rScores, entities[i].Scores)
		rDistances = append(rDistances, entities[i].Distances)
	}

	return &pb.QueryResult{
		Status: &pb.Status{
			ErrorCode: pb.ErrorCode_SUCCESS,
		},
		Entities: &pb.Entities{
			Status: &pb.Status{
				ErrorCode: pb.ErrorCode_SUCCESS,
			},
			Ids:      rIds,
			ValidRow: rValidRow,
			RowsData: rRowsData,
		},
		RowNum:      int64(rows),
		Scores:      rScores,
		Distances:   rDistances,
		ExtraParams: result[0].ExtraParams,
		QueryId:     query.QueryId,
		ProxyId:     query.ProxyId,
	}
}
