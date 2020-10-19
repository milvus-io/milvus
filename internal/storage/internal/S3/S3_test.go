package S3_driver_test

import (
	"context"
	s3_driver "github.com/czs007/suvlim/internal/storage/internal/S3"
	"github.com/stretchr/testify/assert"
	"testing"
)

var ctx = context.Background()
var client, err = s3_driver.NewS3Driver(ctx)


func TestS3Driver_PutRowAndGetRow(t *testing.T) {
	err = client.PutRow(ctx, []byte("bar"), []byte("abcdefghijklmnoopqrstuvwxyz"), "SegmentA", 1)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("bar"), []byte("djhfkjsbdfbsdughorsgsdjhgoisdgh"), "SegmentA", 2)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("bar"), []byte("123854676ershdgfsgdfk,sdhfg;sdi8"), "SegmentB", 3)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("bar1"), []byte("testkeybarorbar_1"), "SegmentC", 3)
	assert.Nil(t, err)
	object, _ := client.GetRow(ctx, []byte("bar"), 1)
	assert.Equal(t, "abcdefghijklmnoopqrstuvwxyz", string(object))
	object, _ = client.GetRow(ctx, []byte("bar"), 2)
	assert.Equal(t, "djhfkjsbdfbsdughorsgsdjhgoisdgh", string(object))
	object, _ = client.GetRow(ctx, []byte("bar"), 5)
	assert.Equal(t, "123854676ershdgfsgdfk,sdhfg;sdi8", string(object))
	object, _ = client.GetRow(ctx, []byte("bar1"), 5)
	assert.Equal(t, "testkeybarorbar_1", string(object))
}

func TestS3Driver_DeleteRow(t *testing.T){
	err = client.DeleteRow(ctx, []byte("bar"), 5)
	assert.Nil(t, err)
	object, _ := client.GetRow(ctx, []byte("bar"), 6)
	assert.Nil(t, object)
	err = client.DeleteRow(ctx, []byte("bar1"), 5)
	assert.Nil(t, err)
	object2, _ := client.GetRow(ctx, []byte("bar1"), 6)
	assert.Nil(t, object2)
}

func TestS3Driver_GetSegments(t *testing.T) {
	err = client.PutRow(ctx, []byte("seg"), []byte("abcdefghijklmnoopqrstuvwxyz"), "SegmentA", 1)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("seg"), []byte("djhfkjsbdfbsdughorsgsdjhgoisdgh"), "SegmentA", 2)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("seg"), []byte("123854676ershdgfsgdfk,sdhfg;sdi8"), "SegmentB", 3)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("seg2"), []byte("testkeybarorbar_1"), "SegmentC", 1)
	assert.Nil(t, err)

	segements, err := client.GetSegments(ctx, []byte("seg"), 4)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(segements))
	if segements[0] == "SegmentA" {
		assert.Equal(t, "SegmentA", segements[0])
		assert.Equal(t, "SegmentB", segements[1])
	} else {
		assert.Equal(t, "SegmentB", segements[0])
		assert.Equal(t, "SegmentA", segements[1])
	}
}

func TestS3Driver_PutRowsAndGetRows(t *testing.T){
	keys := [][]byte{[]byte("foo"), []byte("bar")}
	values := [][]byte{[]byte("The key is foo!"), []byte("The key is bar!")}
	segments := []string{"segmentA", "segmentB"}
	timestamps := []uint64{1, 2}
	err = client.PutRows(ctx, keys, values, segments, timestamps)
	assert.Nil(t, err)

	objects, err := client.GetRows(ctx, keys, timestamps)
	assert.Nil(t, err)
	assert.Equal(t, "The key is foo!", string(objects[0]))
	assert.Equal(t, "The key is bar!", string(objects[1]))
}

func TestS3Driver_DeleteRows(t *testing.T){
	keys := [][]byte{[]byte("foo"), []byte("bar")}
	timestamps := []uint64{3, 3}
	err := client.DeleteRows(ctx, keys, timestamps)
	assert.Nil(t, err)

	objects, err := client.GetRows(ctx, keys, timestamps)
	assert.Nil(t, err)
	assert.Nil(t, objects[0])
	assert.Nil(t, objects[1])
}

func TestS3Driver_PutLogAndGetLog(t *testing.T) {
	err = client.PutLog(ctx, []byte("insert"), []byte("This is insert log!"), 1, 11)
	assert.Nil(t, err)
	err = client.PutLog(ctx, []byte("delete"), []byte("This is delete log!"),  2, 10)
	assert.Nil(t, err)
	err = client.PutLog(ctx, []byte("update"), []byte("This is update log!"),  3, 9)
	assert.Nil(t, err)
	err = client.PutLog(ctx, []byte("select"), []byte("This is select log!"),  4, 8)
	assert.Nil(t, err)

	channels := []int{5, 8, 9, 10, 11, 12, 13}
	logValues, err := client.GetLog(ctx, 0, 5, channels)
	assert.Nil(t, err)
	assert.Equal(t, "This is select log!", string(logValues[0]))
	assert.Equal(t, "This is update log!", string(logValues[1]))
	assert.Equal(t, "This is delete log!", string(logValues[2]))
	assert.Equal(t, "This is insert log!", string(logValues[3]))
}

func TestS3Driver_Segment(t *testing.T) {
	err := client.PutSegmentIndex(ctx, "segmentA", []byte("This is segmentA's index!"))
	assert.Nil(t, err)

	segmentIndex, err := client.GetSegmentIndex(ctx, "segmentA")
	assert.Equal(t, "This is segmentA's index!", string(segmentIndex))

	err = client.DeleteSegmentIndex(ctx, "segmentA")
	assert.Nil(t, err)
}

func TestS3Driver_SegmentDL(t *testing.T){
	err := client.PutSegmentDL(ctx, "segmentB", []byte("This is segmentB's delete log!"))
	assert.Nil(t, err)

	segmentDL, err := client.GetSegmentDL(ctx, "segmentB")
	assert.Nil(t, err)
	assert.Equal(t, "This is segmentB's delete log!", string(segmentDL))

	err = client.DeleteSegmentDL(ctx, "segmentB")
	assert.Nil(t, err)
}