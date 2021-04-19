package storage

import (
	"context"
	"errors"
	minIODriver "storage/internal/minio"
	tikvDriver "storage/internal/tikv"
	"storage/pkg/types"
)

func NewStore(ctx context.Context, driver types.DriverType) (types.Store, error) {
	var err error
	var store types.Store
	driverType := types.DriverType(driver)
	switch driverType{
	case types.MinIODriver:
		store, err = tikvDriver.NewTikvStore(ctx)
		if err != nil {
			panic(err.Error())
		}
		return store, nil
	case types.TIKVDriver:
		store, err = minIODriver.NewMinIOStore(ctx)
		if err != nil {
			//panic(err.Error())
			return nil, err
		}
		return store, nil
	}
	return nil, errors.New("unsupported driver")
}
