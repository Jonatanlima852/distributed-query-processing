package parser

type tokenType int

const (
	tokenIllegal tokenType = iota
	tokenEOF
	tokenIdent
	tokenNumber
	tokenString
	tokenComma
	tokenDot
	tokenStar
	tokenLParen
	tokenRParen
	tokenPlus
	tokenMinus
	tokenSlash
	tokenPercent
	tokenEqual
	tokenNotEqual
	tokenLess
	tokenLessEqual
	tokenGreater
	tokenGreaterEqual
	tokenKeyword
	tokenPlaceholder
)

type token struct {
	typ     tokenType
	literal string
	pos     int
}
