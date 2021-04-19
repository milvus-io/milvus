package minio_driver

import (
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "storage/internal/minio/codec"
	"storage/internal/tikv/codec"
	. "storage/pkg/types"
)

type minioDriver struct {
	driver *minioStore
}

func NewMinioDriver(ctx context.Context) (*minioDriver, error) {
	// to-do read conf
	var endPoint = "127.0.0.1:9000"
	var accessKeyID = "testminio"
	var secretAccessKey = "testminio"
	var useSSL = false

	minioClient, err := minio.New(endPoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		return nil, err
	}

	bucketExists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	if !bucketExists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}
	return &minioDriver{
		&minioStore{
			client: minioClient,
		},
	}, nil
}

func (s *minioDriver) put(ctx context.Context, key Key, value Value, timestamp Timestamp, suffix string) error {
	minioKey, err := MvccEncode(key, timestamp, suffix)
	if err != nil {
		return err
	}

	err = s.driver.PUT(ctx, []byte(minioKey), value)
	return err
}

func (s *minioDriver) scanLE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error) {
	keyEnd, err := MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, nil, nil, err
	}

	keys, values, err := s.driver.Scan(ctx, key, []byte(keyEnd), -1, keyOnly)
	if err != nil {
		return nil, nil, nil, err
	}

	timestamps := make([]Timestamp, len(keys))
	for _, key := range keys {
		_, timestamp, _ := codec.MvccDecode(key)
		timestamps = append(timestamps, timestamp)
	}

	return timestamps, keys, values, nil
}

func (s *minioDriver) scanGE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error) {
	keyStart, err := MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, nil, nil, err
	}

	keys, values, err := s.driver.Scan(ctx, key, []byte(keyStart), -1, keyOnly)
	if err != nil {
		return nil, nil, nil, err
	}

	timestamps := make([]Timestamp, len(keys))
	for _, key := range keys {
		_, timestamp, _ := codec.MvccDecode(key)
		timestamps = append(timestamps, timestamp)
	}

	return timestamps, keys, values, nil
}

//scan(ctx context.Context, key Key, start Timestamp, end Timestamp, withValue bool) ([]Timestamp, []Key, []Value, error)
func (s *minioDriver) deleteLE(ctx context.Context, key Key, timestamp Timestamp) error {
	keyEnd, err := MvccEncode(key, timestamp, "delete")
	if err != nil {
		return err
	}
	err = s.driver.DeleteRange(ctx, key, []byte(keyEnd))
	return err
}
func (s *minioDriver) deleteGE(ctx context.Context, key Key, timestamp Timestamp) error {
	keys, _, err := s.driver.GetByPrefix(ctx, key, true)
	if err != nil {
		return err
	}
	keyStart, err := MvccEncode(key, timestamp, "")
	err = s.driver.DeleteRange(ctx, []byte(keyStart), keys[len(keys)-1])
	return err
}
func (s *minioDriver) deleteRange(ctx context.Context, key Key, start Timestamp, end Timestamp) error {
	keyStart, err := MvccEncode(key, start, "")
	if err != nil {
		return err
	}
	keyEnd, err := MvccEncode(key, end, "")
	if err != nil {
		return err
	}
	err = s.driver.DeleteRange(ctx, keyStart, keyEnd)
	return err
}

func (s *minioDriver) GetRow(ctx context.Context, key Key, timestamp Timestamp) (Value, error) {
	minioKey, err := MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, err
	}
	_, values, err := s.driver.Scan(ctx, key, minioKey, 1, false)
	return values[0], err
}
func (s *minioDriver) GetRows(ctx context.Context, keys []Key, timestamp Timestamp) ([]Value, error){
	values := make([]Value, len(keys))
	for _, key := range keys{
		value, err := s.GetRow(ctx, key, timestamp)
		if err!= nil{
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (s *minioDriver) PutRow(ctx context.Context, key Key, value Value, segment string, timestamp Timestamp) error{
	minioKey, err := MvccEncode(key, timestamp, segment)
	if err != nil{
		return err
	}
	err = s.driver.PUT(ctx, minioKey, value)
	return err
}
func (s *minioDriver) PutRows(ctx context.Context, keys []Key, values []Value, segments []string, timestamp Timestamp) error{
	maxThread := 100
	batchSize := 1
	keysLength := len(keys)

	if keysLength / batchSize > maxThread {
		batchSize = keysLength / maxThread
	}

	batchNums := keysLength / batchSize

	if keysLength % batchSize != 0 {
		batchNums = keysLength / batchSize + 1
	}

	errCh := make(chan error)
	f := func(ctx2 context.Context, keys2 []Key, values2 []Value, segments2 []string, timestamp2 Timestamp) {
		for i := 0; i < len(keys2); i++{
			err := s.PutRow(ctx2, keys2[i], values2[i], segments2[i], timestamp2)
			errCh <- err
		}
	}
	for i := 0; i < batchNums; i++ {
		j := i
		go func() {
			start, end := j*batchSize, (j+1)*batchSize
			if len(keys) < end {
				end = len(keys)
			}
			f(ctx, keys[start:end], values[start:end], segments[start:end], timestamp)
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <- errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *minioDriver) DeleteRow(ctx context.Context, key Key, timestamp Timestamp) error{
	minioKey, err := MvccEncode(key, timestamp, "delete")
	if err != nil{
		return err
	}
	value := []byte("0")
	err = s.driver.PUT(ctx, minioKey, value)
	return err
}

func (s *minioDriver) DeleteRows(ctx context.Context, keys []Key, timestamp Timestamp) error{
	maxThread := 100
	batchSize := 1
	keysLength := len(keys)

	if keysLength / batchSize > maxThread {
		batchSize = keysLength / maxThread
	}

	batchNums := keysLength / batchSize

	if keysLength % batchSize != 0 {
		batchNums = keysLength / batchSize + 1
	}

	errCh := make(chan error)
	f := func(ctx2 context.Context, keys2 []Key, timestamp2 Timestamp) {
		for i := 0; i < len(keys2); i++{
			err := s.DeleteRow(ctx2, keys2[i], timestamp2)
			errCh <- err
		}
	}
	for i := 0; i < batchNums; i++ {
		j := i
		go func() {
			start, end := j*batchSize, (j+1)*batchSize
			if len(keys) < end {
				end = len(keys)
			}
			f(ctx, keys[start:end], timestamp)
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <- errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *minioDriver) PutLog(ctx context.Context, key Key, value Value, timestamp Timestamp, channel int) error{
	logKey := LogEncode(key, timestamp, channel)
	err := s.driver.PUT(ctx, logKey, value)
	return err
}

func (s *minioDriver) FetchLog(ctx context.Context, start Timestamp, end Timestamp, channels []int) () error{

	return nil
}

func (s *minioDriver) GetSegmentIndex(ctx context.Context, segment string) (SegmentIndex, error){

	return nil, nil
}

func (s *minioDriver) PutSegmentIndex(ctx context.Context, segment string, index SegmentIndex) error{
	return nil

}

func (s *minioDriver) DeleteSegmentIndex(ctx context.Context, segment string) error{

	return nil
}

func (s *minioDriver) GetSegmentDL(ctx context.Context, segment string) (SegmentDL, error){
	return nil, nil
}

func (s *minioDriver) SetSegmentDL(ctx context.Context, segment string, log SegmentDL) error{

	return nil
}

func (s *minioDriver) DeleteSegmentDL(ctx context.Context, segment string) error{

	return nil
}
