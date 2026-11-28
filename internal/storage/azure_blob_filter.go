package storage

import "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

func azureZeroByteBlob(item *container.BlobItem) bool {
	return item != nil &&
		item.Properties != nil &&
		item.Properties.ContentLength != nil &&
		*item.Properties.ContentLength == 0
}
