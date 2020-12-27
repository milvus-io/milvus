

## 8. Message Stream Service



#### 8.1 Overview



#### 8.2 API



```go
type OwnerDescription struct {
  Role string
  Address string
  //Token string
  DescriptionText string
}

type ChannelDescription struct {
  Owner OwnerDescription
}

type Client interface {
  CreateChannels(ownerDescription OwnerDescription, numChannels int) (ChannelID []string, error)
  DestoryChannels(channelID []string) error
  DescribeChannels(channelID []string) ([]ChannelDescription, error)
}
```

