package reader

import (
	"context"
	"github.com/zilliztech/milvus-distributed/internal/msgclient"
	"log"
	"sync"
)

func StartQueryNode(ctx context.Context, pulsarURL string) {
	mc := msgclient.ReaderMessageClient{}
	mc.InitClient(ctx, pulsarURL)

	mc.ReceiveMessage()
	qn := CreateQueryNode(ctx, 0, 0, &mc)

	// Segments Services
	go qn.SegmentManagementService()
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
