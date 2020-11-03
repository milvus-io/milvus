package reader

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
)

func StartQueryNode(ctx context.Context, pulsarURL string) {
	mc := msgclient.ReaderMessageClient{}
	mc.InitClient(ctx, pulsarURL)

	mc.ReceiveMessage()
	qn := CreateQueryNode(ctx, 0, 0, &mc)

	// Segments Services
	go qn.SegmentStatisticService()

	wg := sync.WaitGroup{}
	err := qn.InitFromMeta()

	if err != nil {
		log.Printf("Init query node from meta failed")
		return
	}

	wg.Add(3)
	go qn.RunMetaService(&wg)
	go qn.RunInsertDelete(&wg)
	go qn.RunSearch(&wg)
	wg.Wait()
	qn.Close()
}

func (node *QueryNode) RunInsertDelete(wg *sync.WaitGroup) {
	const Debug = true
	const CountInsertMsgBaseline = 1000 * 1000
	var BaselineCounter int64 = 0

	if Debug {
		for {
			select {
			case <-node.ctx.Done():
				wg.Done()
				return
			default:
				var msgLen = node.PrepareBatchMsg()
				var timeRange = TimeRange{node.messageClient.TimeSyncStart(), node.messageClient.TimeSyncEnd()}
				assert.NotEqual(nil, 0, timeRange.timestampMin)
				assert.NotEqual(nil, 0, timeRange.timestampMax)

				if node.msgCounter.InsertCounter/CountInsertMsgBaseline != BaselineCounter {
					node.WriteQueryLog()
					BaselineCounter = node.msgCounter.InsertCounter / CountInsertMsgBaseline
				}

				if msgLen[0] == 0 && len(node.buffer.InsertDeleteBuffer) <= 0 {
					node.queryNodeTimeSync.updateSearchServiceTime(timeRange)
					continue
				}

				node.QueryNodeDataInit()
				node.MessagesPreprocess(node.messageClient.InsertOrDeleteMsg, timeRange)
				//fmt.Println("MessagesPreprocess Done")
				node.WriterDelete()
				node.PreInsertAndDelete()
				//fmt.Println("PreInsertAndDelete Done")
				node.DoInsertAndDelete()
				//fmt.Println("DoInsertAndDelete Done")
				node.queryNodeTimeSync.updateSearchServiceTime(timeRange)
			}
		}
	} else {
		for {
			select {
			case <-node.ctx.Done():
				wg.Done()
				return
			default:
				var msgLen = node.PrepareBatchMsg()
				var timeRange = TimeRange{node.messageClient.TimeSyncStart(), node.messageClient.TimeSyncEnd()}
				assert.NotEqual(nil, 0, timeRange.timestampMin)
				assert.NotEqual(nil, 0, timeRange.timestampMax)

				if msgLen[0] == 0 && len(node.buffer.InsertDeleteBuffer) <= 0 {
					node.queryNodeTimeSync.updateSearchServiceTime(timeRange)
					continue
				}

				node.QueryNodeDataInit()
				node.MessagesPreprocess(node.messageClient.InsertOrDeleteMsg, timeRange)
				//fmt.Println("MessagesPreprocess Done")
				node.WriterDelete()
				node.PreInsertAndDelete()
				//fmt.Println("PreInsertAndDelete Done")
				node.DoInsertAndDelete()
				//fmt.Println("DoInsertAndDelete Done")
				node.queryNodeTimeSync.updateSearchServiceTime(timeRange)
			}
		}
	}
	wg.Done()
}

func (node *QueryNode) RunSearch(wg *sync.WaitGroup) {
	for {
		select {
		case <-node.ctx.Done():
			wg.Done()
			return
		case msg := <-node.messageClient.GetSearchChan():
			node.messageClient.SearchMsg = node.messageClient.SearchMsg[:0]
			node.messageClient.SearchMsg = append(node.messageClient.SearchMsg, msg)
			//for  {
			//if node.messageClient.SearchMsg[0].Timestamp < node.queryNodeTimeSync.ServiceTimeSync {
			var status = node.Search(node.messageClient.SearchMsg)
			fmt.Println("Do Search done")
			if status.ErrorCode != 0 {
				fmt.Println("Search Failed")
				node.PublishFailedSearchResult()
			}
			//break
			//}
			//}
		default:
		}
	}
	wg.Done()
}
