package rocksmq

var Rmq *RocksMQ

type Consumer struct {
	GroupName   string
	ChannelName string
	MsgNum      chan int
}

func InitRmq(rocksdbName string, idAllocator IDAllocator) error {
	var err error
	Rmq, err = NewRocksMQ(rocksdbName, idAllocator)
	return err
}

func GetRmq() *RocksMQ {
	return Rmq
}
