
package mock

import (
	"fmt"
	"testing"
)

func TestFakeCreateCollectionByGRPC(t *testing.T) {
	t.Skip("to fix test")

	reason, segmentID := FakeCreateCollectionByGRPC()
	if reason != "" {
		t.Error(reason)
	}
	fmt.Println(segmentID)
}
