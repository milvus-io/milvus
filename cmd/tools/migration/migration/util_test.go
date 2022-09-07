package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitBySeparator(t *testing.T) {
	tsKey := "435783141193354561_ts435783141193154564"
	k, ts, err := SplitBySeparator(tsKey)
	assert.NoError(t, err)
	assert.Equal(t, "435783141193354561", k)
	assert.Equal(t, Timestamp(435783141193154564), ts)
}
