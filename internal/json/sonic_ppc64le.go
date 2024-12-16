package json

import (
       "encoding/json"
)

var (
       // Marshal is exported by gin/json package.
       Marshal = json.Marshal
       // Unmarshal is exported by gin/json package.
       Unmarshal = json.Unmarshal
       // MarshalIndent is exported by gin/json package.
       MarshalIndent = json.MarshalIndent
       // NewDecoder is exported by gin/json package.
       NewDecoder = json.NewDecoder
       // NewEncoder is exported by gin/json package.
       NewEncoder = json.NewEncoder
)

type (
        Delim      = json.Delim
        Decoder    = json.Decoder
        Number     = json.Number
        RawMessage = json.RawMessage
)

