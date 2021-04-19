package reader

import (
	"fmt"
	"sync"
	"time"
)

func startQueryNode() {
	qn := NewQueryNode(0, 0)
	qn.InitQueryNodeCollection()
	go qn.SegmentService()
	qn.StartMessageClient()

	var wg sync.WaitGroup
	for {
		time.Sleep(200 * time.Millisecond)
		qn.PrepareBatchMsg()
		qn.doQueryNode(&wg)
		fmt.Println("do a batch in 200ms")
	}
}
