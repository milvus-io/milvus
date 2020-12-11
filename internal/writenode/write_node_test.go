package writenode

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func makeNewChannelNames(names []string, suffix string) []string {
	var ret []string
	for _, name := range names {
		ret = append(ret, name+suffix)
	}
	return ret
}

func refreshChannelNames() {
	suffix := "-test-write-node" + strconv.FormatInt(rand.Int63n(100), 10)
	Params.DDChannelNames = makeNewChannelNames(Params.DDChannelNames, suffix)
	Params.InsertChannelNames = makeNewChannelNames(Params.InsertChannelNames, suffix)
}

func TestMain(m *testing.M) {
	Params.Init()
	refreshChannelNames()
	p := Params
	fmt.Println(p)
	exitCode := m.Run()
	os.Exit(exitCode)
}
