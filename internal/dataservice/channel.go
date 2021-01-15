package dataservice

import "fmt"

type (
	channelRange        []string
	insertChannelMapper struct {
		channelRanges []channelRange
	}
)

func (cr channelRange) Contains(channelName string) bool {
	for _, name := range cr {
		if name == channelName {
			return true
		}
	}
	return false
}

func newInsertChannelMapper() *insertChannelMapper {
	mapper := &insertChannelMapper{channelRanges: make([]channelRange, Params.QueryNodeNum)}
	channelNames, numOfChannels, numOfQueryNodes := Params.InsertChannelNames, len(Params.InsertChannelNames), Params.QueryNodeNum
	div, rem := numOfChannels/numOfQueryNodes, numOfChannels%numOfQueryNodes
	for i, j := 0, 0; i < numOfChannels; j++ {
		numOfRange := div
		if j < rem {
			numOfRange++
		}
		cRange := channelRange{}
		k := i + numOfRange
		for ; i < k; i++ {
			cRange = append(cRange, channelNames[i])
		}
		mapper.channelRanges = append(mapper.channelRanges, cRange)
	}
	return mapper
}

func (mapper *insertChannelMapper) GetChannelRange(channelName string) (channelRange, error) {
	for _, cr := range mapper.channelRanges {
		if cr.Contains(channelName) {
			return cr, nil
		}
	}
	return nil, fmt.Errorf("channel name %s not found", channelName)
}
