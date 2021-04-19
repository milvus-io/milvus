package storage

import (
	"context"
	"errors"
	minIODriver "github.com/czs007/suvlim/storage/internal/minio"
	tikvDriver "github.com/czs007/suvlim/storage/internal/tikv"
	S3Driver "github.com/czs007/suvlim/storage/internal/S3"
	"github.com/czs007/suvlim/storage/pkg/types"
)

func NewStore(ctx context.Context, driver types.DriverType) (types.Store, error) {
	var err error
	var store types.Store
	switch driver{
	case types.TIKVDriver:
		store, err = tikvDriver.NewTikvStore(ctx)
		if err != nil {
			panic(err.Error())
		}
		return store, nil
	case types.MinIODriver:
		store, err = minIODriver.NewMinioDriver(ctx)
		if err != nil {
			//panic(err.Error())
			return nil, err
		}
		return store, nil
	case types.S3DRIVER:
		store , err = S3Driver.NewS3Driver(ctx)
	}
	return nil, errors.New("unsupported driver")
}