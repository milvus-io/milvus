package handler

// ChannelMappingHandler is the handler for channel mapping.
type ChannelMappingHandler struct {
	// source channel -> target channel
	channelMapping map[string]string
}

// NewChannelMappingHandler creates a new ChannelMappingHandler.
func NewChannelMappingHandler(channelMapping map[string]string) *ChannelMappingHandler {
	return &ChannelMappingHandler{
		channelMapping: channelMapping,
	}
}

// GetChannel returns the mapped target channel of the source channel.
func (h *ChannelMappingHandler) GetChannel(sourceChannel string) string {
	ch, ok := h.channelMapping[sourceChannel]
	if !ok {
		return sourceChannel
	}
	return ch
}

func (h *)
