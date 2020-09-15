package reader

import (
	"github.com/czs007/suvlim/reader/message_client"
	"sync"
)

func StartQueryNode(pulsarURL string) {
	mc := message_client.MessageClient{}
	mc.InitClient(pulsarURL)

	mc.ReceiveMessage()
	qn := CreateQueryNode(0, 0, &mc)
	qn.InitQueryNodeCollection()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go qn.RunInsertDelete(&wg)
	go qn.RunSearch(&wg)
	wg.Wait()
	qn.Close()
}
