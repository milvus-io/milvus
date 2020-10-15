package reader

import (
	"github.com/czs007/suvlim/conf"
	"strconv"
	"testing"
)

func TestReader_startQueryNode(t *testing.T) {
	//pulsarURL := "pulsar://localhost:6650"
	pulsarAddr := "pulsar://"
	pulsarAddr += conf.Config.Pulsar.Address
	pulsarAddr += ":"
	pulsarAddr += strconv.FormatInt(int64(conf.Config.Pulsar.Port), 10)
	println(pulsarAddr)
	StartQueryNode(pulsarAddr)

	//go StartQueryNode(pulsarAddr,  0)
	//StartQueryNode(pulsarAddr, 1)


}
