package tsoutil

import (
	"fmt"
	"path"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	gparams "github.com/zilliztech/milvus-distributed/internal/util/paramtableutil"
	"go.etcd.io/etcd/clientv3"
)

const (
	physicalShiftBits = 18
	logicalBits       = (1 << physicalShiftBits) - 1
)

func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << physicalShiftBits) + logical)
}

// ParseTS parses the ts to (physical,logical).
func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBits
	physical := ts >> physicalShiftBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

func NewTSOKVBase(subPath string) *kv.EtcdKV {
	etcdAddr, err := gparams.GParams.Load("etcd.address")
	if err != nil {
		panic(err)
	}
	etcdPort, err := gparams.GParams.Load("etcd.port")
	if err != nil {
		panic(err)
	}
	etcdAddr = etcdAddr + ":" + etcdPort
	fmt.Println("etcdAddr ::: ", etcdAddr)
	client, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})

	etcdRootPath, err := gparams.GParams.Load("etcd.rootpath")
	if err != nil {
		panic(err)
	}
	return kv.NewEtcdKV(client, path.Join(etcdRootPath, subPath))
}
