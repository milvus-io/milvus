package storage

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

func TestAzureZeroByteBlob(t *testing.T) {
	zero := int64(0)
	data := int64(496)

	if !azureZeroByteBlob(&container.BlobItem{
		Properties: &container.BlobProperties{
			ContentLength: &zero,
		},
	}) {
		t.Fatal("zero-byte blob should be skipped")
	}

	if azureZeroByteBlob(&container.BlobItem{
		Properties: &container.BlobProperties{
			ContentLength: &data,
		},
	}) {
		t.Fatal("non-empty blob should be kept")
	}

	if azureZeroByteBlob(nil) ||
		azureZeroByteBlob(&container.BlobItem{}) ||
		azureZeroByteBlob(&container.BlobItem{
			Properties: &container.BlobProperties{},
		}) {
		t.Fatal("missing size should be kept")
	}
}
