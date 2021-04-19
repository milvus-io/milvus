package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	memkv "github.com/zilliztech/milvus-distributed/internal/kv/mem"
)

func TestMetaTable_SegmentFlush(t *testing.T) {

	kvMock := memkv.NewMemoryKV()
	meta, err := NewMetaTable(kvMock)
	assert.NoError(t, err)
	defer meta.client.Close()

	t.Run("TestMetaTable_addSegmentFlush", func(t *testing.T) {
		err := meta.addSegmentFlush(101)
		assert.NoError(t, err)

		err = meta.addSegmentFlush(102)
		assert.NoError(t, err)

		err = meta.addSegmentFlush(103)
		assert.NoError(t, err)

		err = meta.reloadSegMetaFromKV()
		assert.NoError(t, err)
	})

	t.Run("TestMetaTable_AppendSegBinlogPaths", func(t *testing.T) {
		segmentID := UniqueID(201)
		err := meta.addSegmentFlush(segmentID)
		assert.Nil(t, err)

		exp := map[int64][]string{
			1: {"a", "b", "c"},
			2: {"b", "a", "c"},
		}
		for fieldID, dataPaths := range exp {
			for _, dp := range dataPaths {
				err = meta.AppendSegBinlogPaths(segmentID, fieldID, []string{dp})
				assert.Nil(t, err)
				err = meta.AppendSegBinlogPaths(segmentID, fieldID, []string{dp})
				assert.Nil(t, err)
			}
		}

		ret, err := meta.getSegBinlogPaths(segmentID)
		assert.Nil(t, err)
		assert.Equal(t,
			map[int64][]string{
				1: {"a", "a", "b", "b", "c", "c"},
				2: {"b", "b", "a", "a", "c", "c"}},
			ret)
	})

	t.Run("TestMetaTable_CompleteFlush", func(t *testing.T) {

		var segmentID UniqueID = 401

		err := meta.addSegmentFlush(segmentID)
		assert.NoError(t, err)

		ret, err := meta.checkFlushComplete(segmentID)
		assert.NoError(t, err)
		assert.Equal(t, false, ret)

		meta.CompleteFlush(segmentID)

		ret, err = meta.checkFlushComplete(segmentID)
		assert.NoError(t, err)
		assert.Equal(t, true, ret)
	})

}

func TestMetaTable_DDLFlush(t *testing.T) {
	kvMock := memkv.NewMemoryKV()
	meta, err := NewMetaTable(kvMock)
	assert.NoError(t, err)
	defer meta.client.Close()

	t.Run("TestMetaTable_AppendDDLBinlogPaths", func(t *testing.T) {

		assert.False(t, meta.hasDDLFlushMeta(301))
		assert.False(t, meta.hasDDLFlushMeta(302))

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

		assert.True(t, meta.hasDDLFlushMeta(301))
		assert.True(t, meta.hasDDLFlushMeta(302))
	})
}
