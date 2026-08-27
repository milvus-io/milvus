package taskcommon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResource(t *testing.T) {
	assert.True(t, Resource{}.IsZero())
	assert.False(t, Resource{CPU: 1}.IsZero())
	assert.False(t, Resource{Memory: 1}.IsZero())

	sum := Resource{CPU: 1, Memory: 10}.Add(Resource{CPU: 2, Memory: 20})
	assert.Equal(t, Resource{CPU: 3, Memory: 30}, sum)

	diff := Resource{CPU: 3, Memory: 30}.Sub(Resource{CPU: 1, Memory: 10})
	assert.Equal(t, Resource{CPU: 2, Memory: 20}, diff)

	// Sub never goes negative: a release that exceeds what was booked clamps to zero.
	clamped := Resource{CPU: 1, Memory: 10}.Sub(Resource{CPU: 5, Memory: 50})
	assert.Equal(t, Resource{}, clamped)

	assert.Equal(t, "cpu=2 memory=20", Resource{CPU: 2, Memory: 20}.String())
}
