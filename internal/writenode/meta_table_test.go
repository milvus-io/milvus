package writenode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"
)

func TestMetaTable_all(t *testing.T) {

	etcdAddr := Params.EtcdAddress
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	require.NoError(t, err)
	etcdKV := etcdkv.NewEtcdKV(cli, "/etcd/test/root/writer")

	_, err = cli.Delete(context.TODO(), "/etcd/test/root/writer", clientv3.WithPrefix())
	require.NoError(t, err)

	meta, err := NewMetaTable(etcdKV)
	assert.NoError(t, err)
	defer meta.client.Close()

	t.Run("TestMetaTable_addSegmentFlush_and_OpenTime", func(t *testing.T) {
		tsOpen := Timestamp(100)
		err := meta.addSegmentFlush(101, tsOpen)
		assert.NoError(t, err)
		exp, err := meta.getFlushOpenTime(101)
		assert.NoError(t, err)
		assert.Equal(t, tsOpen, exp)

		tsOpen = Timestamp(200)
		err = meta.addSegmentFlush(102, tsOpen)
		assert.NoError(t, err)
		exp, err = meta.getFlushOpenTime(102)
		assert.NoError(t, err)
		assert.Equal(t, tsOpen, exp)

		tsOpen = Timestamp(200)
		err = meta.addSegmentFlush(103, tsOpen)
		assert.NoError(t, err)
		exp, err = meta.getFlushOpenTime(103)
		assert.NoError(t, err)
		assert.Equal(t, tsOpen, exp)

		err = meta.reloadSegMetaFromKV()
		assert.NoError(t, err)
	})

	t.Run("TestMetaTable_AppendSegBinlogPaths", func(t *testing.T) {
		segmentID := UniqueID(201)
		tsOpen := Timestamp(1000)
		err := meta.addSegmentFlush(segmentID, tsOpen)
		assert.Nil(t, err)

		exp := map[int32][]string{
			1: {"a", "b", "c"},
			2: {"b", "a", "c"},
		}
		for fieldID, dataPaths := range exp {
			for _, dp := range dataPaths {
				err = meta.AppendSegBinlogPaths(tsOpen, segmentID, fieldID, []string{dp})
				assert.Nil(t, err)
				err = meta.AppendSegBinlogPaths(tsOpen, segmentID, fieldID, []string{dp})
				assert.Nil(t, err)
			}
		}

		ret, err := meta.getSegBinlogPaths(segmentID)
		assert.Nil(t, err)
		assert.Equal(t,
			map[int32][]string{
				1: {"a", "a", "b", "b", "c", "c"},
				2: {"b", "b", "a", "a", "c", "c"}},
			ret)
	})

	t.Run("TestMetaTable_AppendDDLBinlogPaths", func(t *testing.T) {

		collID2Paths := map[UniqueID][]string{
			301: {"a", "b", "c"},
			302: {"c", "b", "a"},
		}

		for collID, dataPaths := range collID2Paths {
			for _, dp := range dataPaths {
				err = meta.AppendDDLBinlogPaths(collID, []string{dp})
				assert.Nil(t, err)
			}
		}

		for k, v := range collID2Paths {
			ret, err := meta.getDDLBinlogPaths(k)
			assert.Nil(t, err)
			assert.Equal(t, map[UniqueID][]string{k: v}, ret)
		}
	})

	t.Run("TestMetaTable_CompleteFlush_and_CloseTime", func(t *testing.T) {

		var segmentID UniqueID = 401
		openTime := Timestamp(1000)
		closeTime := Timestamp(10000)

		err := meta.addSegmentFlush(segmentID, openTime)
		assert.NoError(t, err)

		ret, err := meta.checkFlushComplete(segmentID)
		assert.NoError(t, err)
		assert.Equal(t, false, ret)

		meta.CompleteFlush(closeTime, segmentID)

		ret, err = meta.checkFlushComplete(segmentID)
		assert.NoError(t, err)
		assert.Equal(t, true, ret)
		ts, err := meta.getFlushCloseTime(segmentID)
		assert.NoError(t, err)
		assert.Equal(t, closeTime, ts)
	})

}
