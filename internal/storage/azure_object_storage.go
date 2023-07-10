package storage

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strings"
)

type azureObjectStorage struct {
	cli                     *azblob.Client
	containerName, rootPath string
}

func newDefaultAzureBlobStorage(c *config) (ObjectStorage, error) {
	accountName := "devstoreaccount1"
	accountKey  := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	serverURL   := "http://localhost:10000/devstoreaccount1/"
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serverURL, cred, nil)
	if err != nil {
		return nil, err
	}
	return &azureObjectStorage{
		cli:           client,
		containerName: c.bucketName,
		rootPath:      c.rootPath,
	},nil

}

func (a *azureObjectStorage) CreateBucket(ctx context.Context) error {
	_, err := a.cli.CreateContainer(ctx, a.containerName, nil)
	if err != nil {
		err, ok := err.(*azcore.ResponseError)
		if !ok || err.ErrorCode != string(bloberror.ResourceAlreadyExists) {
			return err // not normal error, or contain create fail not by already exist
		}
	}
	return nil
}

func (a *azureObjectStorage) Stat(ctx context.Context, name string) (*fileInfo, error) {
	client := a.cli.ServiceClient().NewContainerClient(a.containerName).NewBlobClient(name)
	properties, err := client.GetProperties(ctx, nil)
	if err != nil {
		if err, ok := err.(*azcore.ResponseError); ok {
			if err.StatusCode == http.StatusNotFound {
				return nil, ErrNoSuchKey
			}
		}
		log.Warn("failed to stat object", zap.String("bucket", a.containerName), zap.String("path", name), zap.Error(err))
		return nil, err
	}
	return &fileInfo{
		Size:         *properties.ContentLength,
		LastModified: *properties.LastModified,
		Key:          name,
		Err:          nil,
	}, nil
}

func (a *azureObjectStorage) Get(ctx context.Context, name string, off, length int64) (FileReader, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}
	// off = 0
	obj, err := a.Stat(ctx, name)
	if err != nil {
		return nil, err
	}
	//if length == blob.CountToEnd { // length == 0, mean get all
	//	length = size - off
	//}
	size := obj.Size

	if size <= off+length { // exceed ex. [size = 10, off = 5, length = 10] -> length = 5 (5~9)
		length = size - off
	}

	resp, err := a.cli.DownloadStream(ctx, a.containerName, name, &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{
			Offset: off,
			Count:  length,
		},
	})
	return resp.Body, err
}

func (a *azureObjectStorage) Write(ctx context.Context, name string, data []byte) error {
	_, err := a.cli.UploadBuffer(ctx, a.containerName, name, data, nil)
	if err != nil {
		return err
	}
	return nil

}

func (a *azureObjectStorage) Delete(ctx context.Context, name string) error {
	if _, err := a.cli.DeleteBlob(ctx, a.containerName, name, nil); err != nil {
		return err
	}
	return nil
}

type azureIterator struct {
	*runtime.Pager[azblob.ListBlobsFlatResponse]
	prefix    string
	recursive bool
	t         []*container.BlobItem
}

func (a *azureIterator) HasNext(context.Context) bool {
	return len(a.t) > 0 || a.More()
}

func (a *azureIterator) Next(ctx context.Context) (res fileInfo) {
	if len(a.t) == 0 {
		a.t = nil
		resp, err := a.NextPage(ctx)
		if err != nil {
			res.Err = err
			return
		}
		a.t = make([]*container.BlobItem, 0, len(resp.Segment.BlobItems))
		for _, v := range resp.Segment.BlobItems {
			if !a.recursive && strings.Contains(strings.Trim(*v.Name, a.prefix), "/") { // folder
				continue
			}
			a.t = append(a.t, v)
		}
	}
	item := a.t[0]
	a.t = a.t[1:]
	res.Key = *item.Name
	res.LastModified = *item.Properties.LastModified
	res.Size = *item.Properties.ContentLength
	return
}

func (a *azureObjectStorage) ListWithPrefix(ctx context.Context, preFix string, recursive bool) (iterator, error) {
	pager := a.cli.NewListBlobsFlatPager(a.containerName, &azblob.ListBlobsFlatOptions{ // should add root path? ex. prefix = rootPath + prefix
		Prefix: &preFix, // this sdk don't have delimiter option
	})

	return &azureIterator{
		Pager:     pager,
		recursive: recursive,
		prefix:    preFix,
	}, nil
}
