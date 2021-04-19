
package mock

import (
	"fmt"
	"testing"
)

func TestFakeCreateCollectionByGRPC(t *testing.T) {
	reason, segmentID := FakeCreateCollectionByGRPC()
	if reason != "" {
		t.Error(reason)
	}
	fmt.Println(segmentID)
}
