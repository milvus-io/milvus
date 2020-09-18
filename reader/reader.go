package reader

import (
	"github.com/czs007/suvlim/reader/message_client"
	"log"
	"sync"
)

func StartQueryNode(pulsarURL string, numOfQueryNode int, messageClientID int) {
	if messageClientID >= numOfQueryNode {
		log.Printf("Illegal channel id")
		return
	}

	mc := message_client.MessageClient{
		MessageClientID: messageClientID,
	}
	mc.InitClient(pulsarURL, numOfQueryNode)

	mc.ReceiveMessage()
	qn := CreateQueryNode(0, 0, &mc)
	qn.InitQueryNodeCollection()

	// Segments Services
	// go qn.SegmentManagementService()
	go qn.SegmentStatisticService()

	wg := sync.WaitGroup{}
	wg.Add(2)
	go qn.RunInsertDelete(&wg)
	go qn.RunSearch(&wg)
	wg.Wait()
	qn.Close()
}
