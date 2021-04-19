package storage

import (
	"context"
	"errors"

	S3Driver "github.com/zilliztech/milvus-distributed/internal/storage/internal/S3"
	minIODriver "github.com/zilliztech/milvus-distributed/internal/storage/internal/minio"
	tikvDriver "github.com/zilliztech/milvus-distributed/internal/storage/internal/tikv"
	storagetype "github.com/zilliztech/milvus-distributed/internal/storage/type"
)

func NewStore(ctx context.Context, driver storagetype.DriverType) (storagetype.Store, error) {
	var err error
	var store storagetype.Store
	switch driver {
	case storagetype.TIKVDriver:
		store, err = tikvDriver.NewTikvStore(ctx)
		if err != nil {
			panic(err.Error())
		}
		return store, nil
	case storagetype.MinIODriver:
		store, err = minIODriver.NewMinioDriver(ctx)
		if err != nil {
			//panic(err.Error())
			return nil, err
		}
		return store, nil
	case storagetype.S3DRIVER:
		store, err = S3Driver.NewS3Driver(ctx)
		if err != nil {
			//panic(err.Error())
			return nil, err
		}
		return store, nil
	}
	return nil, errors.New("unsupported driver")
}
