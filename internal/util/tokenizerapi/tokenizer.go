package tokenizerapi

//go:generate mockery --name=Tokenizer --with-expecter
type Tokenizer interface {
	NewTokenStream(text string) TokenStream
	Destroy()
}
