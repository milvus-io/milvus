package main

import (
	"github.com/czs007/suvlim/pkg/master"
)

// func main() {
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	cfg := config.NewConfig()
//	s, err := server.CreateServer(ctx, cfg)
//	if err != nil {
//		panic(err)
//	}
//	err = s.Run()
//	if err != nil {
//		fmt.Println(err)
//	}
// }

func init() {
	//	go mock.FakePulsarProducer()
}
func main() {
	//master.SegmentStatsController()
	master.CollectionController()
}
