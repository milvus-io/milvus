package rocksmq

var rmq *RocksMQ

func InitRmq(rocksdbName string, idAllocator IDAllocator) error {
	var err error
	rmq, err = NewRocksMQ(rocksdbName, idAllocator)
	return err
}

func GetRmq() *RocksMQ {
	return rmq
}
