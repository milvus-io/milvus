package mock

import (
	"fmt"
	"testing"
)

func TestFakeCreateCollectionByGRPC(t *testing.T) {
	collectionName, segmentID := FakeCreateCollectionByGRPC()
	if collectionName != "grpc-client-test" {
		t.Error("Collection name wrong")
	}
	fmt.Println(collectionName)
	fmt.Println(segmentID)
}
