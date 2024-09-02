package tokenizerapi

//go:generate mockery --name=TokenStream --with-expecter
type TokenStream interface {
	Advance() bool
	Token() string
	Destroy()
}
