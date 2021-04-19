package msgstream

func NewInputStream(pulsarAddress string,
	producerChannels []string,
	timeTick bool) *MsgStream {
	var stream MsgStream
	if timeTick {
		pulsarTtStream := PulsarTtMsgStream{}
		pulsarTtStream.SetPulsarCient(pulsarAddress)
		pulsarTtStream.SetProducers(producerChannels)
		stream = &pulsarTtStream
	} else {
		pulsarStream := PulsarMsgStream{}
		pulsarStream.SetPulsarCient(pulsarAddress)
		pulsarStream.SetProducers(producerChannels)
		stream = &pulsarStream
	}

	return &stream
}

func NewOutputStream(pulsarAddress string,
	pulsarBufSize int64,
	consumerChannelSize int64,
	consumerChannels []string,
	consumerSubName string,
	timeTick bool) *MsgStream {
	var stream MsgStream
	if timeTick {
		pulsarTtStream := PulsarTtMsgStream{}
		pulsarTtStream.SetPulsarCient(pulsarAddress)
		pulsarTtStream.SetConsumers(consumerChannels, consumerSubName, pulsarBufSize)
		pulsarTtStream.InitMsgPackBuf(consumerChannelSize)
		stream = &pulsarTtStream
	} else {
		pulsarStream := PulsarMsgStream{}
		pulsarStream.SetPulsarCient(pulsarAddress)
		pulsarStream.SetConsumers(consumerChannels, consumerSubName, pulsarBufSize)
		pulsarStream.InitMsgPackBuf(consumerChannelSize)
		stream = &pulsarStream
	}

	return &stream
}

func NewPipeStream(pulsarAddress string,
	pulsarBufSize int64,
	consumerChannelSize int64,
	producerChannels []string,
	consumerChannels []string,
	consumerSubName string,
	timeTick bool) *MsgStream {
	var stream MsgStream
	if timeTick {
		pulsarTtStream := PulsarTtMsgStream{}
		pulsarTtStream.SetPulsarCient(pulsarAddress)
		pulsarTtStream.SetProducers(producerChannels)
		pulsarTtStream.SetConsumers(consumerChannels, consumerSubName, pulsarBufSize)
		pulsarTtStream.InitMsgPackBuf(consumerChannelSize)
		stream = &pulsarTtStream
	} else {
		pulsarStream := PulsarMsgStream{}
		pulsarStream.SetPulsarCient(pulsarAddress)
		pulsarStream.SetProducers(producerChannels)
		pulsarStream.SetConsumers(consumerChannels, consumerSubName, pulsarBufSize)
		pulsarStream.InitMsgPackBuf(consumerChannelSize)
		stream = &pulsarStream
	}

	return &stream
}
