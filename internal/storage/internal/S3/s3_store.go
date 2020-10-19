package S3_driver

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/zilliztech/milvus-distributed/internal/storage/internal/minio/codec"
	. "github.com/zilliztech/milvus-distributed/internal/storage/type"
)

type S3Driver struct {
	driver *S3Store
}

func NewS3Driver(ctx context.Context) (*S3Driver, error) {
	// to-do read conf

	S3Client, err := NewS3Store(aws.Config{
		Region: aws.String(endpoints.CnNorthwest1RegionID)})

	if err != nil {
		return nil, err
	}

	return &S3Driver{
		S3Client,
	}, nil
}

func (s *S3Driver) put(ctx context.Context, key Key, value Value, timestamp Timestamp, suffix string) error {
	minioKey, err := codec.MvccEncode(key, timestamp, suffix)
	if err != nil {
		return err
	}

	err = s.driver.Put(ctx, minioKey, value)
	return err
}

func (s *S3Driver) scanLE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error) {
	keyEnd, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, nil, nil, err
	}

	keys, values, err := s.driver.Scan(ctx, key, []byte(keyEnd), -1, keyOnly)
	if err != nil {
		return nil, nil, nil, err
	}

	var timestamps []Timestamp
	for _, key := range keys {
		_, timestamp, _, _ := codec.MvccDecode(key)
		timestamps = append(timestamps, timestamp)
	}

	return timestamps, keys, values, nil
}

func (s *S3Driver) scanGE(ctx context.Context, key Key, timestamp Timestamp, keyOnly bool) ([]Timestamp, []Key, []Value, error) {
	keyStart, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, nil, nil, err
	}

	keys, values, err := s.driver.Scan(ctx, key, keyStart, -1, keyOnly)
	if err != nil {
		return nil, nil, nil, err
	}

	var timestamps []Timestamp
	for _, key := range keys {
		_, timestamp, _, _ := codec.MvccDecode(key)
		timestamps = append(timestamps, timestamp)
	}

	return timestamps, keys, values, nil
}

//scan(ctx context.Context, key Key, start Timestamp, end Timestamp, withValue bool) ([]Timestamp, []Key, []Value, error)
func (s *S3Driver) deleteLE(ctx context.Context, key Key, timestamp Timestamp) error {
	keyEnd, err := codec.MvccEncode(key, timestamp, "delete")
	if err != nil {
		return err
	}
	err = s.driver.DeleteRange(ctx, key, keyEnd)
	return err
}
func (s *S3Driver) deleteGE(ctx context.Context, key Key, timestamp Timestamp) error {
	keys, _, err := s.driver.GetByPrefix(ctx, key, true)
	if err != nil {
		return err
	}
	keyStart, err := codec.MvccEncode(key, timestamp, "")
	err = s.driver.DeleteRange(ctx, []byte(keyStart), keys[len(keys)-1])
	return err
}
func (s *S3Driver) deleteRange(ctx context.Context, key Key, start Timestamp, end Timestamp) error {
	keyStart, err := codec.MvccEncode(key, start, "")
	if err != nil {
		return err
	}
	keyEnd, err := codec.MvccEncode(key, end, "")
	if err != nil {
		return err
	}
	err = s.driver.DeleteRange(ctx, keyStart, keyEnd)
	return err
}

func (s *S3Driver) GetRow(ctx context.Context, key Key, timestamp Timestamp) (Value, error) {
	minioKey, err := codec.MvccEncode(key, timestamp, "")
	if err != nil {
		return nil, err
	}

	keys, values, err := s.driver.Scan(ctx, append(key, byte('_')), minioKey, 1, false)
	if values == nil || keys == nil{
		return nil, err
	}

	_, _, suffix, err := codec.MvccDecode(keys[0])
	if err != nil{
		return nil, err
	}
	if suffix == "delete"{
		return nil, nil
	}

	return values[0], err
}
func (s *S3Driver) GetRows(ctx context.Context, keys []Key, timestamps []Timestamp) ([]Value, error){
	var values []Value
	for i, key := range keys{
		value, err := s.GetRow(ctx, key, timestamps[i])
		if err!= nil{
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (s *S3Driver) PutRow(ctx context.Context, key Key, value Value, segment string, timestamp Timestamp) error{
	minioKey, err := codec.MvccEncode(key, timestamp, segment)
	if err != nil{
		return err
	}
	err = s.driver.Put(ctx, minioKey, value)
	return err
}
func (s *S3Driver) PutRows(ctx context.Context, keys []Key, values []Value, segments []string, timestamps []Timestamp) error{
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
	f := func(ctx2 context.Context, keys2 []Key, values2 []Value, segments2 []string, timestamps2 []Timestamp) {
		for i := 0; i < len(keys2); i++{
			err := s.PutRow(ctx2, keys2[i], values2[i], segments2[i], timestamps2[i])
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
			f(ctx, keys[start:end], values[start:end], segments[start:end], timestamps[start:end])
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <- errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *S3Driver) GetSegments(ctx context.Context, key Key, timestamp Timestamp) ([]string, error){
	keyEnd, err := codec.MvccEncode(key, timestamp, "")
	if err != nil{
		return nil, err
	}
	keys, _, err := s.driver.Scan(ctx, append(key, byte('_')), keyEnd, -1,true)
	if err != nil {
		return nil, err
	}
	segmentsSet := map[string]bool{}
	for _, key := range keys {
		_, _, segment, err := codec.MvccDecode(key)
		if err != nil {
			panic("must no error")
		}
		if segment != "delete" {
			segmentsSet[segment] = true
		}
	}

	var segments []string
	for k, v := range segmentsSet {
		if v == true {
			segments = append(segments, k)
		}
	}
	return segments, err
}

func (s *S3Driver) DeleteRow(ctx context.Context, key Key, timestamp Timestamp) error{
	minioKey, err := codec.MvccEncode(key, timestamp, "delete")
	if err != nil{
		return err
	}
	value := []byte("0")
	err = s.driver.Put(ctx, minioKey, value)
	return err
}

func (s *S3Driver) DeleteRows(ctx context.Context, keys []Key, timestamps []Timestamp) error{
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
	f := func(ctx2 context.Context, keys2 []Key, timestamps2 []Timestamp) {
		for i := 0; i < len(keys2); i++{
			err := s.DeleteRow(ctx2, keys2[i], timestamps2[i])
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
			f(ctx, keys[start:end], timestamps[start:end])
		}()
	}

	for i := 0; i < len(keys); i++ {
		if err := <- errCh; err != nil {
			return err
		}
	}
	return nil
}

func (s *S3Driver) PutLog(ctx context.Context, key Key, value Value, timestamp Timestamp, channel int) error{
	logKey := codec.LogEncode(key, timestamp, channel)
	err := s.driver.Put(ctx, logKey, value)
	return err
}

func (s *S3Driver) GetLog(ctx context.Context, start Timestamp, end Timestamp, channels []int) ([]Value, error) {
	keys, values, err := s.driver.GetByPrefix(ctx, []byte("log_"), false)
	if err != nil {
		return nil, err
	}

	var resultValues []Value
	for i, key := range keys{
		_, ts, channel, err := codec.LogDecode(string(key))
		if err != nil {
			return nil, err
		}
		if ts >= start && ts <= end  {
			for j := 0; j < len(channels); j++ {
				if channel == channels[j] {
					resultValues = append(resultValues, values[i])
				}
			}
		}
	}

	return resultValues, nil
}

func (s *S3Driver) GetSegmentIndex(ctx context.Context, segment string) (SegmentIndex, error){

	return s.driver.Get(ctx, codec.SegmentEncode(segment, "index"))
}

func (s *S3Driver) PutSegmentIndex(ctx context.Context, segment string, index SegmentIndex) error{

	return s.driver.Put(ctx, codec.SegmentEncode(segment, "index"), index)
}

func (s *S3Driver) DeleteSegmentIndex(ctx context.Context, segment string) error{

	return s.driver.Delete(ctx, codec.SegmentEncode(segment, "index"))
}

func (s *S3Driver) GetSegmentDL(ctx context.Context, segment string) (SegmentDL, error){

	return s.driver.Get(ctx, codec.SegmentEncode(segment, "DL"))
}

func (s *S3Driver) PutSegmentDL(ctx context.Context, segment string, log SegmentDL) error{

	return s.driver.Put(ctx, codec.SegmentEncode(segment, "DL"), log)
}

func (s *S3Driver) DeleteSegmentDL(ctx context.Context, segment string) error{

	return s.driver.Delete(ctx, codec.SegmentEncode(segment, "DL"))
}
