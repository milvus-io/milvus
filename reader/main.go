package main

import (
	"flag"
	"fmt"
	"github.com/czs007/suvlim/conf"
	reader "github.com/czs007/suvlim/reader/read_node"
	"strconv"
)

func main() {
    var yamlFile string
	flag.StringVar(&yamlFile, "yaml", "", "yaml file")
	flag.Parse()
	// flag.Usage()
	fmt.Println("yaml file: ", yamlFile)
	conf.LoadConfig(yamlFile)
	
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	reader.StartQueryNode(pulsarAddr)
}

