package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/milvus-io/milvus/internal/datacoord"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type etcdEndPoints []string

func (i *etcdEndPoints) String() string {
	return strings.Join(*i, ",")
}

func (i *etcdEndPoints) Set(value string) error {
	*i = append(*i, value)
	return nil
}
func main() {
	var etcdEndPoints etcdEndPoints
	var metaRootPath, segmentBinlogSubPath string
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	flagSet.Var(&etcdEndPoints, "etcdEndPoints", "endpoints of etcd")
	flagSet.StringVar(&metaRootPath, "metaRootPath", "", "root path of meta on etcd")
	flagSet.StringVar(&segmentBinlogSubPath, "segmentBinlogSubPath", "", "binlog path prefix on etcd")
	flagSet.Usage = func() {
		fmt.Fprintf(flagSet.Output(), "All flags is optional. If you did not change it in config files, you do not need to set the flag.\n")
		flagSet.PrintDefaults()
	}

	if len(os.Args) > 0 {
		flagSet.Parse(os.Args[1:])
	}

	datacoord.Params.Init()
	if len(etcdEndPoints) != 0 {
		datacoord.Params.EtcdEndpoints = etcdEndPoints
	}
	if len(metaRootPath) != 0 {
		datacoord.Params.MetaRootPath = metaRootPath
	}
	if len(segmentBinlogSubPath) != 0 {
		datacoord.Params.SegmentBinlogSubPath = segmentBinlogSubPath
	}

	etcdKV, err := etcdkv.NewEtcdKV(datacoord.Params.EtcdEndpoints, datacoord.Params.MetaRootPath)
	if err != nil {
		log.Error("failed to connect to etcd", zap.Error(err))
		return
	}

	meta, err := datacoord.NewMeta(etcdKV)
	if err != nil {
		log.Error("failed to create meta", zap.Error(err))
		return
	}

	helper := datacoord.NewMoveBinlogPathHelper(etcdKV, meta)
	if err = helper.Execute(); err != nil {
		return
	}

	log.Info("finished")
}
