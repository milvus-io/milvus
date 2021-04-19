package main

import (
	"github.com/czs007/suvlim/conf"
	reader "github.com/czs007/suvlim/reader/read_node"
	"strconv"
)

func main() {
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	reader.StartQueryNode(pulsarAddr)
}

