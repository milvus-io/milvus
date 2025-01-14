package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

var (
	etcdAddr = flag.String("etcd", "127.0.0.1:2379", "Etcd Endpoint to connect")
	rootPath = flag.String("rootPath", "by-dev/meta/datacoord-meta/s", "Datacoord Segment root path to iterate")

	collectionID  = flag.Int64("collection", 0, "Collection ID to filter with")
	partitionID   = flag.Int64("partition", 0, "Partition ID to filter with")
	segmentID     = flag.Int64("segment", 0, "Segment ID to filter with")
	channel       = flag.String("channel", "", "Channel name to filter with")
	detailBinlogs = flag.Bool("detail", false, "Display detail binlog path content")
)

func main() {
	flag.Parse()

	etcdCli, err := etcd.GetRemoteEtcdClient([]string{*etcdAddr})
	if err != nil {
		log.Fatal("failed to connect to etcd", zap.Error(err))
	}

	etcdkv := etcdkv.NewEtcdKV(etcdCli, *rootPath)

	keys, values, err := etcdkv.LoadWithPrefix(context.TODO(), "/")
	if err != nil {
		log.Fatal("failed to list ", zap.Error(err))
	}
	for i := range keys {
		info := &datapb.SegmentInfo{}
		err = proto.Unmarshal([]byte(values[i]), info)
		if err != nil {
			continue
		}
		if *collectionID > 0 && info.CollectionID != *collectionID {
			continue
		}

		if *partitionID > 0 && info.PartitionID != *partitionID {
			continue
		}

		if *segmentID > 0 && info.ID != *segmentID {
			continue
		}

		if len(*channel) > 0 && !strings.Contains(info.InsertChannel, *channel) {
			continue
		}

		printSegmentInfo(info)
	}
}

const (
	tsPrintFormat = "2006-01-02 15:04:05.999 -0700"
)

func printSegmentInfo(info *datapb.SegmentInfo) {
	fmt.Println("================================================================================")
	fmt.Printf("Segment ID: %d\n", info.ID)
	fmt.Printf("Segment State:%v\n", info.State)
	fmt.Printf("Collection ID: %d\t\tPartitionID: %d\n", info.CollectionID, info.PartitionID)
	fmt.Printf("Insert Channel:%s\n", info.InsertChannel)
	fmt.Printf("Num of Rows: %d\t\tMax Row Num: %d\n", info.NumOfRows, info.MaxRowNum)
	lastExpireTime, _ := tsoutil.ParseTS(info.LastExpireTime)
	fmt.Printf("Last Expire Time: %s\n", lastExpireTime.Format(tsPrintFormat))
	if info.StartPosition != nil {
		startTime, _ := tsoutil.ParseTS(info.StartPosition.Timestamp)
		fmt.Printf("Start Position ID: %v, time: %s\n", info.StartPosition.MsgID, startTime.Format(tsPrintFormat))
	} else {
		fmt.Println("Start Position: nil")
	}
	if info.DmlPosition != nil {
		dmlTime, _ := tsoutil.ParseTS(info.DmlPosition.Timestamp)
		fmt.Printf("Dml Position ID: %v, time: %s\n", info.GetStartPosition().GetMsgID(), dmlTime.Format(tsPrintFormat))
	} else {
		fmt.Println("Dml Position: nil")
	}
	fmt.Printf("Binlog Nums %d\tStatsLog Nums: %d\tDeltaLog Nums:%d\n",
		len(info.Binlogs), len(info.Statslogs), len(info.Deltalogs))

	if *detailBinlogs {
		fmt.Println("**************************************")
		fmt.Println("Binlogs:")
		sort.Slice(info.Binlogs, func(i, j int) bool {
			return info.Binlogs[i].FieldID < info.Binlogs[j].FieldID
		})
		for _, log := range info.Binlogs {
			fmt.Printf("Field %d: %v\n", log.FieldID, log.Binlogs)
		}

		fmt.Println("**************************************")
		fmt.Println("Statslogs:")
		sort.Slice(info.Statslogs, func(i, j int) bool {
			return info.Statslogs[i].FieldID < info.Statslogs[j].FieldID
		})
		for _, log := range info.Statslogs {
			fmt.Printf("Field %d: %v\n", log.FieldID, log.Binlogs)
		}

		fmt.Println("**************************************")
		fmt.Println("Delta Logs:")
		for _, log := range info.GetDeltalogs() {
			for _, l := range log.GetBinlogs() {
				fmt.Printf("Entries: %d From: %v - To: %v\n", l.EntriesNum, l.TimestampFrom, l.TimestampTo)
				fmt.Printf("Path: %v\n", l.LogPath)
			}
		}
	}

	fmt.Println("================================================================================")
}
