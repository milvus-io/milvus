package main

import (
	reader "github.com/czs007/suvlim/reader/read_node"
	"sync"
)

func main() {
	pulsarURL := "pulsar://localhost:6650"

	numOfQueryNode := 2

	go reader.StartQueryNode(pulsarURL, numOfQueryNode, 0)
	reader.StartQueryNode(pulsarURL, numOfQueryNode, 1)


}

func main2() {
	wg := sync.WaitGroup{}
	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()
	wg.Add(1)
	reader.StartQueryNode2()
	wg.Wait()
}
