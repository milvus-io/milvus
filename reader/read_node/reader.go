package reader

import (
	"context"
	"github.com/czs007/suvlim/reader/message_client"
	"sync"
)

func StartQueryNode(pulsarURL string) {
	mc := message_client.MessageClient{}
	mc.InitClient(pulsarURL)

	mc.ReceiveMessage()
	qn := CreateQueryNode(0, 0, &mc)
	ctx := context.Background()

	// Segments Services
	//go qn.SegmentManagementService()
	go qn.SegmentStatisticService()

	wg := sync.WaitGroup{}
	qn.InitFromMeta()
	wg.Add(3)
	go qn.RunMetaService(ctx, &wg)
	go qn.RunInsertDelete(&wg)
	go qn.RunSearch(&wg)
	wg.Wait()
	qn.Close()
}
