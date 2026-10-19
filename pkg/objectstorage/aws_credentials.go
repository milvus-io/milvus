package objectstorage

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// NewAWSCredentials selects a single IAM source for explicit modes. In
// particular, a failed web identity request must never try the node's role.
func NewAWSCredentials(source, region, endpoint string) (*credentials.Credentials, error) {
	var provider aws.CredentialsProvider
	switch source {
	case "", "default":
		return credentials.NewIAM(endpoint), nil
	case "web_identity":
		role, token := os.Getenv("AWS_ROLE_ARN"), os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
		if role == "" || token == "" {
			return nil, fmt.Errorf("web_identity requires AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE")
		}
		if region == "" {
			region = os.Getenv("AWS_REGION")
		}
		if region == "" {
			region = os.Getenv("AWS_DEFAULT_REGION")
		}
		if region == "" {
			return nil, fmt.Errorf("web_identity requires an AWS region")
		}
		client := sts.New(sts.Options{Region: region})
		provider = stscreds.NewWebIdentityRoleProvider(client, role, stscreds.IdentityTokenFile(token), func(o *stscreds.WebIdentityRoleOptions) {
			o.RoleSessionName = os.Getenv("AWS_ROLE_SESSION_NAME")
		})
	case "instance_profile":
		provider = ec2rolecreds.New(func(o *ec2rolecreds.Options) {
			o.Client = imds.New(imds.Options{})
		})
	default:
		return nil, fmt.Errorf("unknown AWS IAM credential source %q", source)
	}
	return credentials.New(&awsCredentialProvider{provider: provider}), nil
}

// MinIO serializes Retrieve/IsExpired. Do not retain the request's context:
// credentials outlive the request that created the client.
type awsCredentialProvider struct {
	credentials.Expiry
	provider aws.CredentialsProvider
}

func (p *awsCredentialProvider) Retrieve() (credentials.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	value, err := p.provider.Retrieve(ctx)
	if err != nil {
		return credentials.Value{}, err
	}
	if value.AccessKeyID == "" || value.SecretAccessKey == "" {
		return credentials.Value{}, fmt.Errorf("selected AWS IAM source returned empty credentials")
	}
	if value.CanExpire {
		p.SetExpiration(value.Expires, credentials.DefaultExpiryWindow)
	}
	return credentials.Value{AccessKeyID: value.AccessKeyID, SecretAccessKey: value.SecretAccessKey,
		SessionToken: value.SessionToken, SignerType: credentials.SignatureV4}, nil
}

func validateIAMCredentialSource(c *Config) error {
	switch c.IAMCredentialSource {
	case "", "default":
		return nil
	case "web_identity", "instance_profile":
		if !c.UseIAM || ResolveCloudProvider(c) != CloudProviderAWS {
			return fmt.Errorf("explicit IAM credential source requires useIAM=true and cloudProvider=aws")
		}
		return nil
	default:
		return fmt.Errorf("unknown AWS IAM credential source %q", c.IAMCredentialSource)
	}
}
