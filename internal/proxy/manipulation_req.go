package proxy

import (
	"github.com/apache/pulsar-client-go/pulsar"
	pb "github.com/czs007/suvlim/internal/proto/message"
	"github.com/golang/protobuf/proto"
	"log"
	"sync"
)

type manipulationReq struct {
	pb.ManipulationReqMsg
	wg    sync.WaitGroup
	proxy *proxyServer
}

// TsMsg interfaces
func (req *manipulationReq) Ts() Timestamp {
	return Timestamp(req.Timestamp)
}
func (req *manipulationReq) SetTs(ts Timestamp) {
	req.Timestamp = uint64(ts)
}

// BaseRequest interfaces
func (req *manipulationReq) Type() pb.ReqType {
	return req.ReqType
}

func (req *manipulationReq) PreExecute() pb.Status {
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (req *manipulationReq) Execute() pb.Status {
	req.proxy.reqSch.manipulationsChan <- req
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (req *manipulationReq) PostExecute() pb.Status { // send into pulsar
	req.wg.Add(1)
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (req *manipulationReq) WaitToFinish() pb.Status { // wait unitl send into pulsar
	req.wg.Wait()
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (s *proxyServer) restartManipulationRoutine(buf_size int) error {
	s.reqSch.manipulationsChan = make(chan *manipulationReq, buf_size)
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: s.pulsarAddr})
	if err != nil {
		return err
	}
	readers := make([]pulsar.Producer, len(s.readerTopics))
	for i, t := range s.readerTopics {
		p, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: t})
		if err != nil {
			return err
		}
		readers[i] = p
	}
	deleter, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: s.deleteTopic})
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-s.ctx.Done():
				deleter.Close()
				for _, r := range readers {
					r.Close()
				}
				pulsarClient.Close()
				return
			case ip := <-s.reqSch.manipulationsChan:
				ts, st := s.getTimestamp(1)
				if st.ErrorCode != pb.ErrorCode_SUCCESS {
					log.Printf("get time stamp failed, error code = %d, msg = %s, drop inset rows = %d", st.ErrorCode, st.Reason, len(ip.RowsData))
					continue
				}

				mq := pb.ManipulationReqMsg{
					CollectionName: ip.CollectionName,
					PartitionTag:   ip.PartitionTag,
					PrimaryKeys:    ip.PrimaryKeys,
					RowsData:       ip.RowsData,
					Timestamp:      uint64(ts[0]),
					SegmentId:      ip.SegmentId,
					ChannelId:      ip.ChannelId,
					ReqType:        ip.ReqType,
					ProxyId:        ip.ProxyId,
					ExtraParams:    ip.ExtraParams,
				}

				mb, err := proto.Marshal(&mq)
				if err != nil {
					log.Printf("Marshal ManipulationReqMsg failed, error = %v", err)
					continue
				}

				switch ip.ReqType {
				case pb.ReqType_kInsert:
					if _, err := readers[mq.ChannelId].Send(s.ctx, &pulsar.ProducerMessage{Payload: mb}); err != nil {
						log.Printf("post into puslar failed, error = %v", err)
					}
					break
				case pb.ReqType_kDeleteEntityByID:
					if _, err = deleter.Send(s.ctx, &pulsar.ProducerMessage{Payload: mb}); err != nil {
						log.Printf("post into pulsar filed, error = %v", err)
					}
				default:
					log.Printf("post unexpect ReqType = %d", ip.ReqType)
					break
				}
				s.reqSch.m_timestamp_mux.Lock()
				if s.reqSch.m_timestamp <= ts[0] {
					s.reqSch.m_timestamp = ts[0]
				} else {
					log.Printf("there is some wrong with m_timestamp, it goes back, current = %d, previous = %d", ts[0], s.reqSch.m_timestamp)
				}
				s.reqSch.m_timestamp_mux.Unlock()
				ip.wg.Done()
			}
		}
	}()
	return nil
}
