package util

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func TestGetSegmentInsertFilesPreservesLogPath(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		RootPath: "file",
	}
	insertBinlogs := []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogPath:    "file/insert_log/10/20/30/1/40",
			EntriesNum: 100,
		}},
	}}

	got := GetSegmentInsertFiles(insertBinlogs, storageConfig, 10, 20, 30)

	assert.Equal(t, "file/insert_log/10/20/30/1/40", got.GetFieldInsertFiles()[0].GetFilePaths()[0])
}

func TestGetSegmentInsertFilesFallsBackToLogID(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{
		RootPath: "file",
	}
	insertBinlogs := []*datapb.FieldBinlog{{
		FieldID: 1,
		Binlogs: []*datapb.Binlog{{
			LogID:      40,
			EntriesNum: 100,
		}},
	}}

	got := GetSegmentInsertFiles(insertBinlogs, storageConfig, 10, 20, 30)

	assert.Equal(t, "file/insert_log/10/20/30/1/40", got.GetFieldInsertFiles()[0].GetFilePaths()[0])
}
