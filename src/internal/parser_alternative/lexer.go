package parser

import (
	"strings"
	"unicode"
)

type lexer struct {
	input []rune
	pos   int
}

func newLexer(input string) *lexer {
	return &lexer{
		input: []rune(input),
		pos:   0,
	}
}

func (l *lexer) nextToken() token {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return token{typ: tokenEOF, pos: l.pos}
	}

	ch := l.input[l.pos]
	switch ch {
	case ',':
		l.pos++
		return token{typ: tokenComma, literal: ",", pos: l.pos - 1}
	case '.':
		l.pos++
		return token{typ: tokenDot, literal: ".", pos: l.pos - 1}
	case '*':
		l.pos++
		return token{typ: tokenStar, literal: "*", pos: l.pos - 1}
	case '(':
		l.pos++
		return token{typ: tokenLParen, literal: "(", pos: l.pos - 1}
	case ')':
		l.pos++
		return token{typ: tokenRParen, literal: ")", pos: l.pos - 1}
	case '+':
		l.pos++
		return token{typ: tokenPlus, literal: "+", pos: l.pos - 1}
	case '-':
		l.pos++
		return token{typ: tokenMinus, literal: "-", pos: l.pos - 1}
	case '/':
		l.pos++
		return token{typ: tokenSlash, literal: "/", pos: l.pos - 1}
	case '%':
		l.pos++
		return token{typ: tokenPercent, literal: "%", pos: l.pos - 1}
	case '=':
		l.pos++
		return token{typ: tokenEqual, literal: "=", pos: l.pos - 1}
	case '!':
		if l.peek() == '=' {
			start := l.pos
			l.pos += 2
			return token{typ: tokenNotEqual, literal: "!=", pos: start}
		}
	case '<':
		start := l.pos
		if l.peek() == '=' {
			l.pos += 2
			return token{typ: tokenLessEqual, literal: "<=", pos: start}
		}
		if l.peek() == '>' {
			l.pos += 2
			return token{typ: tokenNotEqual, literal: "<>", pos: start}
		}
		l.pos++
		return token{typ: tokenLess, literal: "<", pos: start}
	case '>':
		start := l.pos
		if l.peek() == '=' {
			l.pos += 2
			return token{typ: tokenGreaterEqual, literal: ">=", pos: start}
		}
		l.pos++
		return token{typ: tokenGreater, literal: ">", pos: start}
	case ':':
		return l.readPlaceholder()
	case '\'':
		return l.readString()
	}

	if isIdentifierStart(ch) {
		return l.readIdentifier()
	}
	if unicode.IsDigit(ch) {
		return l.readNumber()
	}

	// Unknown rune
	l.pos++
	return token{typ: tokenIllegal, literal: string(ch), pos: l.pos - 1}
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		l.pos++
	}
}

func (l *lexer) peek() rune {
	if l.pos+1 >= len(l.input) {
		return 0
	}
	return l.input[l.pos+1]
}

func (l *lexer) readIdentifier() token {
	start := l.pos
	for l.pos < len(l.input) && isIdentifierPart(l.input[l.pos]) {
		l.pos++
	}
	literal := string(l.input[start:l.pos])
	upper := strings.ToUpper(literal)
	if isKeyword(upper) {
		return token{typ: tokenKeyword, literal: upper, pos: start}
	}
	return token{typ: tokenIdent, literal: literal, pos: start}
}

func (l *lexer) readNumber() token {
	start := l.pos
	hasDot := false
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '.' {
			if hasDot {
				break
			}
			hasDot = true
			l.pos++
			continue
		}
		if !unicode.IsDigit(ch) {
			break
		}
		l.pos++
	}
	return token{typ: tokenNumber, literal: string(l.input[start:l.pos]), pos: start}
}

func (l *lexer) readString() token {
	start := l.pos
	l.pos++ // skip opening '
	var builder strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			if l.peek() == '\'' {
				builder.WriteRune('\'')
				l.pos += 2
				continue
			}
			l.pos++
			break
		}
		builder.WriteRune(ch)
		l.pos++
	}
	return token{typ: tokenString, literal: builder.String(), pos: start}
}

func (l *lexer) readPlaceholder() token {
	start := l.pos
	l.pos++
	for l.pos < len(l.input) && isIdentifierPart(l.input[l.pos]) {
		l.pos++
	}
	return token{typ: tokenPlaceholder, literal: string(l.input[start:l.pos]), pos: start}
}

func isIdentifierStart(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_' || ch == '$'
}

func isIdentifierPart(ch rune) bool {
	return isIdentifierStart(ch) || unicode.IsDigit(ch)
}

var keywords = map[string]struct{}{
	"SELECT":   {},
	"DISTINCT": {},
	"FROM":     {},
	"WHERE":    {},
	"GROUP":    {},
	"BY":       {},
	"ORDER":    {},
	"LIMIT":    {},
	"AS":       {},
	"INNER":    {},
	"LEFT":     {},
	"RIGHT":    {},
	"FULL":     {},
	"OUTER":    {},
	"JOIN":     {},
	"ON":       {},
	"AND":      {},
	"OR":       {},
	"NOT":      {},
	"BETWEEN":  {},
	"ASC":      {},
	"DESC":     {},
	"TRUE":     {},
	"FALSE":    {},
	"NULL":     {},
	"IN":       {},
	"IS":       {},
}

func isKeyword(word string) bool {
	_, ok := keywords[word]
	return ok
}
