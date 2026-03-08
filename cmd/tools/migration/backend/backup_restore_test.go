package backend

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/prototext"
)

func TestBackupCodec_Serialize(t *testing.T) {
	header := &BackupHeader{
		Version:   int32(BackupHeaderVersionV1),
		Instance:  "/by-dev",
		MetaPath:  "meta",
		Entries:   0,
		Component: "",
		Extra:     nil,
	}
	kvs := map[string]string{
		"1": "1",
		"2": "2",
		"3": "3",
	}
	codec := &BackupCodec{}
	file, err := codec.Serialize(header, kvs)
	assert.NoError(t, err)
	gotHeader, gotEntries, err := codec.DeSerialize(file)
	assert.NoError(t, err)
	assert.Equal(t, prototext.Format(header), prototext.Format(gotHeader))
	assert.True(t, reflect.DeepEqual(kvs, gotEntries))
}
