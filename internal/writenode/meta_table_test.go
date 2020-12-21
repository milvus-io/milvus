package writenode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"
)

func createMetaTable(t *testing.T) *metaTable {
	etcdAddr := Params.EtcdAddress
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root")

	_, err = cli.Delete(context.TODO(), "/etcd/test/root", clientv3.WithPrefix())
	assert.Nil(t, err)

	meta, err := NewMetaTable(etcdKV)
	assert.Nil(t, err)
	return meta
}

func TestMetaTable_AddSegmentFlush(t *testing.T) {
	meta := createMetaTable(t)
	defer meta.client.Close()
	err := meta.AddSegmentFlush(1)
	assert.Nil(t, err)

	err = meta.AddSegmentFlush(2)
	assert.Nil(t, err)

	err = meta.AddSegmentFlush(2)
	assert.NotNil(t, err)

	err = meta.reloadFromKV()
	assert.Nil(t, err)

}

func TestMetaTable_SetFlushTime(t *testing.T) {
	meta := createMetaTable(t)
	defer meta.client.Close()

	var segmentID UniqueID = 1

	err := meta.AddSegmentFlush(segmentID)
	assert.Nil(t, err)

	tsOpen := Timestamp(1000)
	err = meta.SetFlushOpenTime(segmentID, tsOpen)
	assert.Nil(t, err)

	exp, err := meta.getFlushOpenTime(segmentID)
	assert.Nil(t, err)
	assert.Equal(t, tsOpen, exp)

	tsClose := Timestamp(10001)
	err = meta.SetFlushCloseTime(segmentID, tsClose)
	assert.Nil(t, err)

	exp, err = meta.getFlushCloseTime(segmentID)
	assert.Nil(t, err)
	assert.Equal(t, tsClose, exp)
}

func TestMetaTable_AppendBinlogPaths(t *testing.T) {
	meta := createMetaTable(t)
	defer meta.client.Close()
	var segmentID UniqueID = 1
	err := meta.AddSegmentFlush(segmentID)
	assert.Nil(t, err)

	exp := map[int32][]string{
		1: {"a", "b", "c"},
		2: {"b", "a", "c"},
	}
	for fieldID, dataPaths := range exp {
		for _, dp := range dataPaths {
			err = meta.AppendBinlogPaths(segmentID, fieldID, []string{dp})
			assert.Nil(t, err)
		}
	}

	ret, err := meta.getBinlogPaths(segmentID)
	assert.Nil(t, err)
	assert.Equal(t, exp, ret)

}

func TestMetaTable_CompleteFlush(t *testing.T) {
	meta := createMetaTable(t)
	defer meta.client.Close()

	var segmentID UniqueID = 1

	err := meta.AddSegmentFlush(segmentID)
	assert.Nil(t, err)

	ret, err := meta.checkFlushComplete(segmentID)
	assert.Nil(t, err)
	assert.Equal(t, false, ret)

	meta.CompleteFlush(segmentID)

	ret, err = meta.checkFlushComplete(segmentID)
	assert.Nil(t, err)
	assert.Equal(t, true, ret)

}
