package storage

import (
	"context"
	"errors"

	S3Driver "github.com/zilliztech/milvus-distributed/internal/storage/internal/S3"
	minIODriver "github.com/zilliztech/milvus-distributed/internal/storage/internal/minio"
	tikvDriver "github.com/zilliztech/milvus-distributed/internal/storage/internal/tikv"
	storagetype "github.com/zilliztech/milvus-distributed/internal/storage/type"
)

func NewStore(ctx context.Context, option storagetype.Option) (storagetype.Store, error) {
	var err error
	var store storagetype.Store
	switch option.Type {
	case storagetype.TIKVDriver:
		store, err = tikvDriver.NewTikvStore(ctx, option)
		if err != nil {
			panic(err.Error())
		}
		return store, nil
	case storagetype.MinIODriver:
		store, err = minIODriver.NewMinioDriver(ctx, option)
		if err != nil {
			//panic(err.Error())
			return nil, err
		}
		return store, nil
	case storagetype.S3DRIVER:
		store, err = S3Driver.NewS3Driver(ctx, option)
		if err != nil {
			//panic(err.Error())
			return nil, err
		}
		return store, nil
	}
	return nil, errors.New("unsupported driver")
}
