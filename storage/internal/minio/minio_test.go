package minio_driver_test

import (
	"context"
	minio_driver "github.com/czs007/suvlim/storage/internal/minio"
	"github.com/stretchr/testify/assert"
	"testing"
)

var ctx = context.Background()
var client, err = minio_driver.NewMinioDriver(ctx)


func TestMinioDriver_PutRowAndGetRow(t *testing.T) {
	err = client.PutRow(ctx, []byte("bar"), []byte("abcdefghijklmnoopqrstuvwxyz"), "SegmentA", 1234567)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("bar"), []byte("djhfkjsbdfbsdughorsgsdjhgoisdgh"), "SegmentA", 1235567)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("bar"), []byte("123854676ershdgfsgdfk,sdhfg;sdi8"), "SegmentB", 1236567)
	assert.Nil(t, err)
	err = client.PutRow(ctx, []byte("bar_1"), []byte("testkeybarorbar_1"), "SegmentC", 1236567)
	assert.Nil(t, err)
	object, _ := client.GetRow(ctx, []byte("bar"), 1234999)
	assert.Equal(t, "abcdefghijklmnoopqrstuvwxyz", string(object))
	object, _ = client.GetRow(ctx, []byte("bar"), 1235999)
	assert.Equal(t, "djhfkjsbdfbsdughorsgsdjhgoisdgh", string(object))
	object, _ = client.GetRow(ctx, []byte("bar"), 1236567)
	assert.Equal(t, "123854676ershdgfsgdfk,sdhfg;sdi8", string(object))
	object, _ = client.GetRow(ctx, []byte("bar_1"), 1236800)
	assert.Equal(t, "testkeybarorbar_1", string(object))
}

func TestMinioDriver_DeleteRow(t *testing.T){
	err = client.DeleteRow(ctx, []byte("bar"), 1237000)
	assert.Nil(t, err)
	object, _ := client.GetRow(ctx, []byte("bar"), 1237000)
	assert.Nil(t, object)
	err = client.DeleteRow(ctx, []byte("bar_1"), 1237000)
	assert.Nil(t, err)
	object2, _ := client.GetRow(ctx, []byte("bar_1"), 1237000)
	assert.Nil(t, object2)
}

func TestMinioDriver_PutRowsAndGetRows(t *testing.T){
	keys := [][]byte{[]byte("foo"), []byte("bar")}
	values := [][]byte{[]byte("The key is foo!"), []byte("The key is bar!")}
	segment := "segmentA"
	err = client.PutRows(ctx, keys, values, segment, 555555)
	assert.Nil(t, err)

	objects, err := client.GetRows(ctx, keys, 666666)
	assert.Nil(t, err)
	assert.Equal(t, "The key is foo!", string(objects[0]))
	assert.Equal(t, "The key is bar!", string(objects[1]))
}

func TestMinioDriver_DeleteRows(t *testing.T){
	keys := [][]byte{[]byte("foo"), []byte("bar")}
	err := client.DeleteRows(ctx, keys, 777777)
	assert.Nil(t, err)

	objects, err := client.GetRows(ctx, keys, 777777)
	assert.Nil(t, err)
	assert.Nil(t, objects[0])
	assert.Nil(t, objects[1])
}

func TestMinioDriver_PutLogAndGetLog(t *testing.T) {
	err = client.PutLog(ctx, []byte("insert"), []byte("This is insert log!"), 1234567, 11)
	assert.Nil(t, err)
	err = client.PutLog(ctx, []byte("delete"), []byte("This is delete log!"),  1236567, 10)
	assert.Nil(t, err)
	err = client.PutLog(ctx, []byte("update"), []byte("This is update log!"),  1237567, 9)
	assert.Nil(t, err)
	err = client.PutLog(ctx, []byte("select"), []byte("This is select log!"),  1238567, 8)
	assert.Nil(t, err)

	channels := []int{5, 8, 9, 10, 11, 12, 13}
	logValues, err := client.GetLog(ctx, 1234567, 1245678, channels)
	assert.Nil(t, err)
	assert.Equal(t, "This is select log!", string(logValues[0]))
	assert.Equal(t, "This is update log!", string(logValues[1]))
	assert.Equal(t, "This is delete log!", string(logValues[2]))
	assert.Equal(t, "This is insert log!", string(logValues[3]))
}

func TestMinioDriver_Segment(t *testing.T) {
	err := client.PutSegmentIndex(ctx, "segmentA", []byte("This is segmentA's index!"))
	assert.Nil(t, err)

	segmentIndex, err := client.GetSegmentIndex(ctx, "segmentA")
	assert.Equal(t, "This is segmentA's index!", string(segmentIndex))

	err = client.DeleteSegmentIndex(ctx, "segmentA")
	assert.Nil(t, err)
}

func TestMinioDriver_SegmentDL(t *testing.T){
	err := client.PutSegmentDL(ctx, "segmentB", []byte("This is segmentB's delete log!"))
	assert.Nil(t, err)

	segmentDL, err := client.GetSegmentDL(ctx, "segmentB")
	assert.Nil(t, err)
	assert.Equal(t, "This is segmentB's delete log!", string(segmentDL))

	err = client.DeleteSegmentDL(ctx, "segmentB")
	assert.Nil(t, err)
}