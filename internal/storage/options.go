package storage

// Option for setting params used by chunk manager client.
type config struct {
	address           string
	bucketName        string
	accessKeyID       string
	secretAccessKeyID string
	useSSL            bool
	createBucket      bool
	rootPath          string
	useIAM            bool
	cloudProvider     string
	iamEndpoint       string
	useVirtualHost    bool
	region            string
	requestTimeoutMs  int64
}

func newDefaultConfig() *config {
	return &config{}
}

// Option is used to config the retry function.
type Option func(*config)

func Address(addr string) Option {
	return func(c *config) {
		c.address = addr
	}
}

func BucketName(bucketName string) Option {
	return func(c *config) {
		c.bucketName = bucketName
	}
}

func AccessKeyID(accessKeyID string) Option {
	return func(c *config) {
		c.accessKeyID = accessKeyID
	}
}

func SecretAccessKeyID(secretAccessKeyID string) Option {
	return func(c *config) {
		c.secretAccessKeyID = secretAccessKeyID
	}
}

func UseSSL(useSSL bool) Option {
	return func(c *config) {
		c.useSSL = useSSL
	}
}

func CreateBucket(createBucket bool) Option {
	return func(c *config) {
		c.createBucket = createBucket
	}
}

func RootPath(rootPath string) Option {
	return func(c *config) {
		c.rootPath = rootPath
	}
}

func UseIAM(useIAM bool) Option {
	return func(c *config) {
		c.useIAM = useIAM
	}
}

func CloudProvider(cloudProvider string) Option {
	return func(c *config) {
		c.cloudProvider = cloudProvider
	}
}

func IAMEndpoint(iamEndpoint string) Option {
	return func(c *config) {
		c.iamEndpoint = iamEndpoint
	}
}

func UseVirtualHost(useVirtualHost bool) Option {
	return func(c *config) {
		c.useVirtualHost = useVirtualHost
	}
}

func Region(region string) Option {
	return func(c *config) {
		c.region = region
	}
}

func RequestTimeout(requestTimeoutMs int64) Option {
	return func(c *config) {
		c.requestTimeoutMs = requestTimeoutMs
	}
}
