package proxy

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/message"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"log"
	"sync"
)

type manipulationReq struct {
	stats []commonpb.Status
	msgs  []*pb.ManipulationReqMsg
	wg    sync.WaitGroup
	proxy *proxyServer
}

// TsMsg interfaces
func (req *manipulationReq) Ts() (typeutil.Timestamp, error) {
	if req.msgs == nil {
		return 0, errors.New("No typed manipulation request message in ")
	}
	return typeutil.Timestamp(req.msgs[0].Timestamp), nil
}
func (req *manipulationReq) SetTs(ts typeutil.Timestamp) {
	for _, msg := range req.msgs {
		msg.Timestamp = uint64(ts)
	}
}

// BaseRequest interfaces
func (req *manipulationReq) Type() pb.ReqType {
	if req.msgs == nil {
		return 0
	}
	return req.msgs[0].ReqType
}

// TODO: use a ProcessReq function to wrap details?
// like func (req *manipulationReq) ProcessReq() commonpb.Status{
//	req.PreExecute()
//  req.Execute()
//  req.PostExecute()
//  req.WaitToFinish()
//}

func (req *manipulationReq) PreExecute() commonpb.Status {
	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
}

func (req *manipulationReq) Execute() commonpb.Status {
	req.proxy.reqSch.manipulationsChan <- req
	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
}

func (req *manipulationReq) PostExecute() commonpb.Status { // send into pulsar
	req.wg.Add(1)
	return commonpb.Status{ErrorCode: commonpb.ErrorCode_SUCCESS}
}

func (req *manipulationReq) WaitToFinish() commonpb.Status { // wait until send into pulsar
	req.wg.Wait()

	for _, stat := range req.stats{
		if stat.ErrorCode != commonpb.ErrorCode_SUCCESS{
			return stat
		}
	}
	// update timestamp if necessary
	ts, _ := req.Ts()
	req.proxy.reqSch.mTimestampMux.Lock()
	defer req.proxy.reqSch.mTimestampMux.Unlock()
	if req.proxy.reqSch.mTimestamp <= ts {
		req.proxy.reqSch.mTimestamp = ts
	} else {
		log.Printf("there is some wrong with m_timestamp, it goes back, current = %d, previous = %d", ts, req.proxy.reqSch.mTimestamp)
	}
	return req.stats[0]
}

func (s *proxyServer) restartManipulationRoutine(bufSize int) error {
	s.reqSch.manipulationsChan = make(chan *manipulationReq, bufSize)
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
				ts, err := s.getTimestamp(1)
				if err != nil {
					log.Printf("get time stamp failed")
					ip.stats[0] = commonpb.Status{ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR}
					ip.wg.Done()
					break
				}
				ip.SetTs(ts[0])

				wg := sync.WaitGroup{}
				for i, mq := range ip.msgs {
					mq := mq
					i := i
					wg.Add(1)
					go func() {
						defer wg.Done()
						mb, err := proto.Marshal(mq)
						if err != nil {
							log.Printf("Marshal ManipulationReqMsg failed, error = %v", err)
							ip.stats[i] = commonpb.Status{
								ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
								Reason:    fmt.Sprintf("Marshal ManipulationReqMsg failed, error=%v", err),
							}
							return
						}

						switch ip.Type() {
						case pb.ReqType_kInsert:
							if _, err := readers[mq.ChannelId].Send(s.ctx, &pulsar.ProducerMessage{Payload: mb}); err != nil {
								log.Printf("post into puslar failed, error = %v", err)
								ip.stats[i] = commonpb.Status{
									ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
									Reason:    fmt.Sprintf("Post into puslar failed, error=%v", err.Error()),
								}
								return
							}
						case pb.ReqType_kDeleteEntityByID:
							if _, err = deleter.Send(s.ctx, &pulsar.ProducerMessage{Payload: mb}); err != nil {
								log.Printf("post into pulsar filed, error = %v", err)
								ip.stats[i] = commonpb.Status{
									ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
									Reason:    fmt.Sprintf("Post into puslar failed, error=%v", err.Error()),
								}
								return
							}
						default:
							log.Printf("post unexpect ReqType = %d", ip.Type())
							return
						}
					}()
				}
				wg.Wait()
				ip.wg.Done()
				break
			}
		}
	}()
	return nil
}
