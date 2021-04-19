package main

import (
	"flag"
	"fmt"
	"github.com/zilliztech/milvus-distributed/internal/conf"
	"github.com/zilliztech/milvus-distributed/internal/master"
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
        var yamlFile string
        flag.StringVar(&yamlFile, "yaml", "", "yaml file")
        flag.Parse()
        // flag.Usage()
        fmt.Println("yaml file: ", yamlFile)
        conf.LoadConfig(yamlFile)

	master.Run()
	//master.SegmentStatsController()
	//master.CollectionController()
}
