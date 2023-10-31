package storage

func NewDefaultConfig() *Config {
	return &Config{}
}

// Option is used to storage.Config the retry function.
type Option func(*Config)

func Address(addr string) Option {
	return func(c *Config) {
		c.Address = addr
	}
}

func BucketName(bucketName string) Option {
	return func(c *Config) {
		c.BucketName = bucketName
	}
}

func AccessKeyID(accessKeyID string) Option {
	return func(c *Config) {
		c.AccessKeyID = accessKeyID
	}
}

func SecretAccessKeyID(secretAccessKeyID string) Option {
	return func(c *Config) {
		c.SecretAccessKeyID = secretAccessKeyID
	}
}

func UseSSL(useSSL bool) Option {
	return func(c *Config) {
		c.UseSSL = useSSL
	}
}

func CreateBucket(createBucket bool) Option {
	return func(c *Config) {
		c.CreateBucket = createBucket
	}
}

func RootPath(rootPath string) Option {
	return func(c *Config) {
		c.RootPath = rootPath
	}
}

func UseIAM(useIAM bool) Option {
	return func(c *Config) {
		c.UseIAM = useIAM
	}
}

func CloudProvider(cloudProvider string) Option {
	return func(c *Config) {
		c.CloudProvider = cloudProvider
	}
}

func IAMEndpoint(iamEndpoint string) Option {
	return func(c *Config) {
		c.IamEndpoint = iamEndpoint
	}
}

func UseVirtualHost(useVirtualHost bool) Option {
	return func(c *Config) {
		c.UseVirtualHost = useVirtualHost
	}
}

func Region(region string) Option {
	return func(c *Config) {
		c.Region = region
	}
}

func RequestTimeout(requestTimeoutMs int64) Option {
	return func(c *Config) {
		c.RequestTimeoutMs = requestTimeoutMs
	}
}
