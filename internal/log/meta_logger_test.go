package log

import (
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestMetaLogger(t *testing.T) {
	ts := newTestLogSpy(t)
	conf := &Config{Level: "debug", DisableTimestamp: true, DisableCaller: true}
	logger, _, _ := InitTestLogger(ts, conf)
	replaceLeveledLoggers(logger)

	NewMetaLogger().WithCollectionID(0).
		WithCollectionMeta(&model.Collection{}).
		WithIndexMeta(&model.Index{}).
		WithCollectionName("coll").
		WithPartitionID(0).
		WithPartitionName("part").
		WithFieldID(0).
		WithFieldName("field").
		WithIndexID(0).
		WithIndexName("idx").
		WithBuildID(0).
		WithBuildIDS([]int64{0, 0}).
		WithSegmentID(0).
		WithIndexFiles([]string{"idx", "idx"}).
		WithIndexVersion(0).
		WithTSO(0).
		WithAlias("alias").
		WithOperation(DropCollection).Info()

	ts.assertMessagesContains("CollectionID=0")
	ts.assertMessagesContains("CollectionName=coll")
	ts.assertMessagesContains("PartitionID=0")
	ts.assertMessagesContains("PartitionName=part")
	ts.assertMessagesContains("FieldID=0")
	ts.assertMessagesContains("FieldName=field")
	ts.assertMessagesContains("IndexID=0")
	ts.assertMessagesContains("IndexName=idx")
	ts.assertMessagesContains("BuildID=0")
	ts.assertMessagesContains("\"[0,0]\"")
	ts.assertMessagesContains("SegmentID=0")
	ts.assertMessagesContains("IndexFiles=\"[idx,idx]\"")
	ts.assertMessagesContains("IndexVersion=0")
	ts.assertMessagesContains("TSO=0")
	ts.assertMessagesContains("Alias=alias")
	ts.assertMessagesContains("Operation=1")
}
