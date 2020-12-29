

## 8. Message Stream Service



#### 8.1 Overview



#### 8.2 API

```go
type Client interface {
  CreateChannels(req CreateChannelRequest) (ChannelID []string, error)
  DestoryChannels(channelID []string) error
  DescribeChannels(channelID []string) (ChannelDescriptions, error)
}
```



* *CreateChannels*

```go
type OwnerDescription struct {
  Role string
  Address string
  //Token string
  DescriptionText string
}

type CreateChannelRequest struct {
  OwnerDescription OwnerDescription
  numChannels int
}
```



* *DescribeChannels*

```go
type ChannelDescription struct {
  Owner OwnerDescription
}

type ChannelDescriptions struct {
  Descriptions []ChannelDescription
}
```

