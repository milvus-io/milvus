package storage

import (
	"testing"

	"github.com/milvus-io/milvus/internal/common"

	"github.com/stretchr/testify/assert"
)

func TestParseSegmentIDByBinlog(t *testing.T) {

	type testCase struct {
		name             string
		input            string
		rootPath         string
		expectError      bool
		expectID         UniqueID
		isIgnorableError bool
	}

	cases := []testCase{
		{
			name:        "normal case",
			input:       "files/insertLog/123/456/1/101/10000001",
			rootPath:    "files",
			expectError: false,
			expectID:    1,
		},
		{
			name:        "normal case long id",
			input:       "files/insertLog/123/456/434828745294479362/101/10000001",
			rootPath:    "files",
			expectError: false,
			expectID:    434828745294479362,
		},
		{
			name:        "bad format",
			input:       "files/123",
			rootPath:    "files",
			expectError: true,
		},
		{
			name:        "empty input",
			input:       "",
			rootPath:    "files",
			expectError: true,
		},
		{
			name:        "non-number segmentid",
			input:       "files/insertLog/123/456/segment_id/101/10000001",
			rootPath:    "files",
			expectError: true,
		},
		{
			name:        "file name doesn't exists",
			input:       "tenant1/files/delta_log/609/610/457/793",
			rootPath:    "tenant1/files",
			expectError: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := ParseSegmentIDByBinlog(tc.rootPath, tc.input)
			if tc.expectError {
				assert.Error(t, err)
				if tc.isIgnorableError {
					assert.True(t, common.IsIgnorableError(err))
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectID, id)
			}
		})
	}
}
