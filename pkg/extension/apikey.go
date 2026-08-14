package extension

// APIKeyVerifier resolves an API key token to the milvus username it maps onto.
//
// A distribution that issues its own API keys installs one; with none installed
// milvus keeps its native token path, so a stock binary is unaffected.
type APIKeyVerifier interface {
	// Verify maps a raw token to a username. An error means the token is not
	// valid, and the caller reports an authentication failure. The error must
	// not contain the raw token: milvus logs it, and a token is a credential.
	Verify(rawToken string) (username string, err error)

	// RequireAPIKeyOnExternalListener reports whether the external listener must
	// refuse username and password authentication. A distribution whose users
	// authenticate only through its own keys returns true, so that a milvus
	// credential cannot be used to bypass the key system.
	RequireAPIKeyOnExternalListener() bool
}
