package backend

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackupCodec_Serialize(t *testing.T) {
	header := &BackupHeader{
		Version:   BackupHeaderVersionV1,
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
	assert.True(t, reflect.DeepEqual(header, gotHeader))
	assert.True(t, reflect.DeepEqual(kvs, gotEntries))
}
