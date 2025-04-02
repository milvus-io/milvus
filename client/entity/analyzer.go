package entity

type Token struct {
	Text           string
	StartOffset    int64
	EndOffset      int64
	Position       int64
	PositionLength int64
	Hash           uint32
}

type AnalyzerResult struct {
	Tokens []*Token
}
