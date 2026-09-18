package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompactionTierConstants(t *testing.T) {
	idealSize := int64(1024)

	assert.Equal(t, int64(256), compactionMiddleSize(idealSize))
	assert.Equal(t, int64(870), compactionFullThreshold(idealSize))
	assert.Equal(t, int64(217), compactionFragmentThreshold(idealSize))
}

func TestTierClassification(t *testing.T) {
	idealSize := int64(1024)

	tests := []struct {
		name         string
		residualSize int64
		wantFull     bool
		wantFragment bool
	}{
		{"full segment (1024)", 1024, true, false},
		{"full segment (870)", 870, true, false},
		{"between segment (500)", 500, false, false},
		{"between segment (257)", 257, false, false},
		{"middle segment (256)", 256, false, false},
		{"middle segment (217)", 217, false, false},
		{"fragment (216)", 216, false, true},
		{"fragment (100)", 100, false, true},
		{"fragment (1)", 1, false, true},
		{"fragment (0)", 0, false, true},
		{"at boundary (869)", 869, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantFull, isFullSegment(idealSize, tt.residualSize), "isFullSegment")
			assert.Equal(t, tt.wantFragment, isFragmentSegment(idealSize, tt.residualSize), "isFragmentSegment")
		})
	}
}
