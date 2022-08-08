package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSegmentIDByBinlog(t *testing.T) {

	type testCase struct {
		name        string
		input       string
		expectError bool
		expectID    UniqueID
	}

	cases := []testCase{
		{
			name:        "normal case",
			input:       "files/insertLog/123/456/1/101/10000001",
			expectError: false,
			expectID:    1,
		},
		{
			name:        "normal case long id",
			input:       "files/insertLog/123/456/434828745294479362/101/10000001",
			expectError: false,
			expectID:    434828745294479362,
		},
		{
			name:        "bad format",
			input:       "files/123",
			expectError: true,
		},
		{
			name:        "empty input",
			input:       "",
			expectError: true,
		},
		{
			name:        "non-number segmentid",
			input:       "files/insertLog/123/456/segment_id/101/10000001",
			expectError: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := ParseSegmentIDByBinlog(tc.input)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectID, id)
			}
		})
	}
}
