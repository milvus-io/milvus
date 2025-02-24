package segcore

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/cgopb"
)

func TestConsumeCStatusIntoError(t *testing.T) {
	err := ConsumeCStatusIntoError(nil)
	assert.NoError(t, err)
}

func TestGetLocalUsedSize(t *testing.T) {
	size, err := GetLocalUsedSize("")
	assert.NoError(t, err)
	assert.NotNil(t, size)
}

func TestProtoLayout(t *testing.T) {
	layout := CreateProtoLayout()
	testProto := cgopb.IndexStats{
		MemSize: 1024,
		SerializedIndexInfos: []*cgopb.SerializedIndexFileInfo{
			{
				FileName: "test",
				FileSize: 768,
			},
		},
	}
	msg, err := proto.Marshal(&testProto)
	defer runtime.KeepAlive(msg)
	assert.NoError(t, err)
	SetProtoLayout(layout, msg)

	resultProto := cgopb.IndexStats{}
	UnmarshalProtoLayout(layout, &resultProto)

	assert.True(t, proto.Equal(&testProto, &resultProto))
	layout.blob = nil
	layout.size = 0
	ReleaseProtoLayout(layout)
}
