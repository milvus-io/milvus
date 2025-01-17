package segcore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/proto/datapb"
)

func TestLoadFieldDataRequest(t *testing.T) {
	req := &LoadFieldDataRequest{
		Fields: []LoadFieldDataInfo{{
			Field: &datapb.FieldBinlog{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 100,
						LogPath:    "1",
					}, {
						EntriesNum: 101,
						LogPath:    "2",
					},
				},
			},
		}},
		RowCount: 100,
		MMapDir:  "1234567890",
	}
	creq, err := req.getCLoadFieldDataRequest()
	assert.NoError(t, err)
	assert.NotNil(t, creq)
	creq.Release()
}
