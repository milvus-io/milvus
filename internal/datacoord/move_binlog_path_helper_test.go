package datacoord

import (
	"path"
	"testing"

	"github.com/gogo/protobuf/proto"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestMoveBinlogPathHelper_Start(t *testing.T) {
	Params.Init()
	t.Run("test normal move", func(t *testing.T) {
		var err error
		kv := memkv.NewMemoryKV()

		segment := &datapb.SegmentInfo{ID: 0}
		err = kv.Save(buildSegmentPath(0, 0, 0), proto.MarshalTextString(segment))
		assert.Nil(t, err)

		binlogMeta := &datapb.SegmentFieldBinlogMeta{
			FieldID:    0,
			BinlogPath: "path1",
		}
		err = kv.Save(path.Join(Params.SegmentBinlogSubPath, "0", "0", "path1"), proto.MarshalTextString(binlogMeta))
		assert.Nil(t, err)

		meta, err := NewMeta(kv)

		assert.Nil(t, err)
		helper := NewMoveBinlogPathHelper(kv, meta)
		err = helper.Execute()
		assert.Nil(t, err)

		pbstr, err := kv.Load(buildSegmentPath(0, 0, 0))
		assert.Nil(t, err)
		err = proto.UnmarshalText(pbstr, segment)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, len(segment.Binlogs))
		assert.EqualValues(t, 0, segment.Binlogs[0].FieldID)
		assert.EqualValues(t, []string{"path1"}, segment.Binlogs[0].Binlogs)
	})
}
