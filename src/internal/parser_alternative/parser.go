package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

// Parse converts a SQL string into a SelectStatement AST.
func Parse(sql string) (*query.SelectStatement, error) {
	p := newParser(sql)
	stmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if p.cur.typ != tokenEOF {
		return nil, fmt.Errorf("unexpected token %q", p.cur.literal)
	}
	return stmt, nil
}

type parser struct {
	lex  *lexer
	cur  token
	peek token
}

func newParser(input string) *parser {
	lex := newLexer(input)
	p := &parser{lex: lex}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *parser) nextToken() {
	p.cur = p.peek
	p.peek = p.lex.nextToken()
}

func (p *parser) expectKeyword(word string) error {
	if p.cur.typ == tokenKeyword && p.cur.literal == word {
		p.nextToken()
		return nil
	}
	return fmt.Errorf("expected keyword %s but found %s", word, p.cur.literal)
}

func (p *parser) consumeKeyword(word string) bool {
	if p.cur.typ == tokenKeyword && p.cur.literal == word {
		p.nextToken()
		return true
	}
	return false
}

func (p *parser) expect(tt tokenType) (token, error) {
	if p.cur.typ != tt {
		return token{}, fmt.Errorf("expected token %v but found %s", tt, p.cur.literal)
	}
	tok := p.cur
	p.nextToken()
	return tok, nil
}

func (p *parser) parseSelect() (*query.SelectStatement, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, err
	}
	stmt := &query.SelectStatement{}

	if p.consumeKeyword("DISTINCT") {
		stmt.Distinct = true
	}

	columns, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	if err := p.expectKeyword("FROM"); err != nil {
		return nil, err
	}
	from, err := p.parseFromClause()
	if err != nil {
		return nil, err
	}
	stmt.From = from

	if p.consumeKeyword("WHERE") {
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	if p.consumeKeyword("GROUP") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		groupExprs, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupExprs
	}

	if p.consumeKeyword("ORDER") {
		if err := p.expectKeyword("BY"); err != nil {
			return nil, err
		}
		orderExprs, err := p.parseOrderByList()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderExprs
	}

	if p.consumeKeyword("LIMIT") {
		if p.cur.typ != tokenNumber {
			return nil, fmt.Errorf("LIMIT expects numeric literal")
		}
		val, err := strconv.ParseInt(p.cur.literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value: %w", err)
		}
		stmt.Limit = &val
		p.nextToken()
	}

	return stmt, nil
}

func (p *parser) parseSelectList() ([]query.SelectItem, error) {
	var items []query.SelectItem
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		if p.cur.typ != tokenComma {
			break
		}
		p.nextToken()
	}
	return items, nil
}

func (p *parser) parseSelectItem() (query.SelectItem, error) {
	item := query.SelectItem{}
	if p.cur.typ == tokenStar {
		p.nextToken()
		item.Wildcard = &query.Wildcard{}
		return item, nil
	}
	expr, err := p.parseExpression(0)
	if err != nil {
		return query.SelectItem{}, err
	}
	item.Expr = expr

	if p.consumeKeyword("AS") {
		if p.cur.typ != tokenIdent {
			return query.SelectItem{}, fmt.Errorf("expected alias after AS")
		}
		item.Alias = p.cur.literal
		p.nextToken()
	} else if p.cur.typ == tokenIdent {
		// Bare alias (avoid consuming keywords such as FROM).
		if !isReservedWord(p.cur.literal) {
			item.Alias = p.cur.literal
			p.nextToken()
		}
	}
	return item, nil
}

func (p *parser) parseFromClause() ([]query.TableReference, error) {
	var refs []query.TableReference
	for {
		ref, err := p.parseTableReference()
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
		if p.cur.typ != tokenComma {
			break
		}
		p.nextToken()
	}
	return refs, nil
}

func (p *parser) parseTableReference() (query.TableReference, error) {
	if p.cur.typ != tokenIdent {
		return query.TableReference{}, fmt.Errorf("expected table name, found %s", p.cur.literal)
	}
	name := p.cur.literal
	p.nextToken()
	alias := ""
	if p.consumeKeyword("AS") {
		if p.cur.typ != tokenIdent {
			return query.TableReference{}, fmt.Errorf("expected alias after AS")
		}
		alias = p.cur.literal
		p.nextToken()
	} else if p.cur.typ == tokenIdent && !isReservedWord(p.cur.literal) {
		alias = p.cur.literal
		p.nextToken()
	}

	ref := query.TableReference{
		Name:  name,
		Alias: alias,
	}

	for isJoinStart(p.cur) {
		clause, err := p.parseJoinClause()
		if err != nil {
			return query.TableReference{}, err
		}
		ref.Joins = append(ref.Joins, clause)
	}
	return ref, nil
}

func (p *parser) parseJoinClause() (query.JoinClause, error) {
	joinType, err := p.consumeJoin()
	if err != nil {
		return query.JoinClause{}, err
	}
	if p.cur.typ != tokenIdent {
		return query.JoinClause{}, fmt.Errorf("expected joined table name")
	}
	name := p.cur.literal
	p.nextToken()
	alias := ""
	if p.consumeKeyword("AS") {
		if p.cur.typ != tokenIdent {
			return query.JoinClause{}, fmt.Errorf("expected alias after AS")
		}
		alias = p.cur.literal
		p.nextToken()
	} else if p.cur.typ == tokenIdent && !isReservedWord(p.cur.literal) {
		alias = p.cur.literal
		p.nextToken()
	}
	if err := p.expectKeyword("ON"); err != nil {
		return query.JoinClause{}, err
	}
	cond, err := p.parseExpression(0)
	if err != nil {
		return query.JoinClause{}, err
	}
	return query.JoinClause{
		Type:      joinType,
		Table:     name,
		Alias:     alias,
		Condition: cond,
	}, nil
}

func (p *parser) consumeJoin() (query.JoinType, error) {
	switch {
	case p.cur.typ == tokenKeyword && p.cur.literal == "JOIN":
		p.nextToken()
		return query.JoinTypeInner, nil
	case p.cur.typ == tokenKeyword && p.cur.literal == "INNER":
		p.nextToken()
		if err := p.expectKeyword("JOIN"); err != nil {
			return "", err
		}
		return query.JoinTypeInner, nil
	case p.cur.typ == tokenKeyword && p.cur.literal == "LEFT":
		p.nextToken()
		if p.cur.typ == tokenKeyword && p.cur.literal == "OUTER" {
			p.nextToken()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return "", err
		}
		return query.JoinTypeLeft, nil
	case p.cur.typ == tokenKeyword && p.cur.literal == "RIGHT":
		p.nextToken()
		if p.cur.typ == tokenKeyword && p.cur.literal == "OUTER" {
			p.nextToken()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return "", err
		}
		return query.JoinTypeRight, nil
	case p.cur.typ == tokenKeyword && p.cur.literal == "FULL":
		p.nextToken()
		if p.cur.typ == tokenKeyword && p.cur.literal == "OUTER" {
			p.nextToken()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return "", err
		}
		return query.JoinTypeFull, nil
	default:
		return "", fmt.Errorf("expected JOIN clause, found %s", p.cur.literal)
	}
}

func isJoinStart(tok token) bool {
	if tok.typ != tokenKeyword {
		return false
	}
	switch tok.literal {
	case "JOIN", "INNER", "LEFT", "RIGHT", "FULL":
		return true
	default:
		return false
	}
}

func (p *parser) parseExpressionList() ([]query.Expression, error) {
	var exprs []query.Expression
	for {
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.cur.typ != tokenComma {
			break
		}
		p.nextToken()
	}
	return exprs, nil
}

func (p *parser) parseOrderByList() ([]query.OrderExpression, error) {
	var result []query.OrderExpression
	for {
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		order := query.OrderExpression{
			Expr:      expr,
			Direction: query.SortAsc,
		}
		if p.cur.typ == tokenKeyword && (p.cur.literal == "ASC" || p.cur.literal == "DESC") {
			order.Direction = query.SortDirection(p.cur.literal)
			p.nextToken()
		}
		result = append(result, order)
		if p.cur.typ != tokenComma {
			break
		}
		p.nextToken()
	}
	return result, nil
}

func (p *parser) parseExpression(precedence int) (query.Expression, error) {
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}

	for {
		curPrec := p.currentPrecedence()
		if precedence >= curPrec {
			break
		}

		switch {
		case p.cur.typ == tokenKeyword && p.cur.literal == "BETWEEN":
			left, err = p.parseBetween(left, false)
		case p.cur.typ == tokenKeyword && p.cur.literal == "NOT" &&
			p.peek.typ == tokenKeyword && p.peek.literal == "BETWEEN":
			p.nextToken() // consume NOT
			left, err = p.parseBetween(left, true)
		default:
			left, err = p.parseInfix(left)
		}
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

func (p *parser) parsePrefix() (query.Expression, error) {
	switch {
	case p.cur.typ == tokenIdent:
		return p.parseIdentifierExpression()
	case p.cur.typ == tokenNumber:
		return p.parseNumberLiteral()
	case p.cur.typ == tokenString:
		val := columnar.NewStringValue(p.cur.literal)
		p.nextToken()
		return query.Literal{Value: val}, nil
	case p.cur.typ == tokenKeyword && (p.cur.literal == "TRUE" || p.cur.literal == "FALSE"):
		val := p.cur.literal == "TRUE"
		p.nextToken()
		return query.Literal{Value: columnar.NewBoolValue(val)}, nil
	case p.cur.typ == tokenKeyword && p.cur.literal == "NULL":
		p.nextToken()
		return query.NullLiteral{}, nil
	case p.cur.typ == tokenMinus || (p.cur.typ == tokenKeyword && p.cur.literal == "NOT"):
		operator := p.cur.literal
		p.nextToken()
		right, err := p.parseExpression(prefixPrecedence)
		if err != nil {
			return nil, err
		}
		return query.UnaryExpr{Operator: operator, Expr: right}, nil
	case p.cur.typ == tokenLParen:
		p.nextToken()
		expr, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return expr, nil
	case p.cur.typ == tokenStar:
		p.nextToken()
		return query.Wildcard{}, nil
	case p.cur.typ == tokenPlaceholder:
		literal := strings.TrimPrefix(p.cur.literal, ":")
		if literal == "" {
			return nil, fmt.Errorf("invalid placeholder %s", p.cur.literal)
		}
		p.nextToken()
		return query.Parameter{Name: literal}, nil
	default:
		return nil, fmt.Errorf("unexpected token %s", p.cur.literal)
	}
}

func (p *parser) parseIdentifierExpression() (query.Expression, error) {
	first := p.cur.literal
	p.nextToken()
	if p.cur.typ == tokenDot {
		p.nextToken()
		if p.cur.typ == tokenStar {
			p.nextToken()
			return query.Wildcard{Table: first}, nil
		}
		if p.cur.typ != tokenIdent {
			return nil, fmt.Errorf("expected identifier after dot")
		}
		second := p.cur.literal
		p.nextToken()
		if p.cur.typ == tokenLParen {
			return p.parseFunctionCall(first + "." + second)
		}
		return query.ColumnRef{Table: first, Name: second}, nil
	}

	if p.cur.typ == tokenLParen {
		return p.parseFunctionCall(first)
	}
	return query.ColumnRef{Name: first}, nil
}

func (p *parser) parseFunctionCall(name string) (query.Expression, error) {
	// current token is '('
	if _, err := p.expect(tokenLParen); err != nil {
		return nil, err
	}
	fn := query.FunctionCall{Name: strings.ToUpper(name)}
	if p.consumeKeyword("DISTINCT") {
		fn.Distinct = true
	}
	if p.cur.typ == tokenStar {
		// COUNT(*) style argument
		p.nextToken()
		fn.Args = append(fn.Args, query.Wildcard{})
	} else if p.cur.typ != tokenRParen {
		for {
			arg, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			fn.Args = append(fn.Args, arg)
			if p.cur.typ != tokenComma {
				break
			}
			p.nextToken()
		}
	}
	if _, err := p.expect(tokenRParen); err != nil {
		return nil, err
	}
	return fn, nil
}

func (p *parser) parseNumberLiteral() (query.Expression, error) {
	lit := p.cur.literal
	p.nextToken()
	if strings.Contains(lit, ".") {
		val, err := strconv.ParseFloat(lit, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float literal %s", lit)
		}
		return query.Literal{Value: columnar.NewFloatValue(val)}, nil
	}
	val, err := strconv.ParseInt(lit, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid int literal %s", lit)
	}
	return query.Literal{Value: columnar.NewIntValue(val)}, nil
}

func (p *parser) currentPrecedence() int {
	if p.cur.typ == tokenKeyword {
		if p.cur.literal == "NOT" && p.peek.typ == tokenKeyword && p.peek.literal == "BETWEEN" {
			return keywordPrecedence["BETWEEN"]
		}
		if prec, ok := keywordPrecedence[p.cur.literal]; ok {
			return prec
		}
	}
	if prec, ok := tokenPrecedence[p.cur.typ]; ok {
		return prec
	}
	return 0
}

func (p *parser) parseBetween(left query.Expression, not bool) (query.Expression, error) {
	if p.cur.literal == "NOT" {
		if err := p.expectKeyword("BETWEEN"); err != nil {
			return nil, err
		}
	} else {
		p.nextToken()
	}
	lower, err := p.parseExpression(keywordPrecedence["BETWEEN"])
	if err != nil {
		return nil, err
	}
	if !p.consumeKeyword("AND") {
		return nil, fmt.Errorf("BETWEEN requires AND")
	}
	upper, err := p.parseExpression(keywordPrecedence["BETWEEN"])
	if err != nil {
		return nil, err
	}
	return query.BetweenExpr{
		Expr:  left,
		Lower: lower,
		Upper: upper,
		Not:   not,
	}, nil
}

func (p *parser) parseInfix(left query.Expression) (query.Expression, error) {
	switch {
	case p.cur.typ == tokenKeyword && (p.cur.literal == "AND" || p.cur.literal == "OR"):
		op := p.cur.literal
		prec := p.currentPrecedence()
		p.nextToken()
		right, err := p.parseExpression(prec)
		if err != nil {
			return nil, err
		}
		return query.BinaryExpr{Left: left, Operator: op, Right: right}, nil
	case p.cur.typ == tokenKeyword && p.cur.literal == "IS":
		return p.parseIsExpression(left)
	case isBinaryOperator(p.cur.typ):
		op := p.cur.literal
		prec := p.currentPrecedence()
		p.nextToken()
		right, err := p.parseExpression(prec)
		if err != nil {
			return nil, err
		}
		return query.BinaryExpr{Left: left, Operator: op, Right: right}, nil
	default:
		return left, nil
	}
}

func (p *parser) parseIsExpression(left query.Expression) (query.Expression, error) {
	p.nextToken()
	not := false
	if p.cur.typ == tokenKeyword && p.cur.literal == "NOT" {
		not = true
		p.nextToken()
	}
	var right query.Expression
	switch {
	case p.cur.typ == tokenKeyword && p.cur.literal == "NULL":
		right = query.NullLiteral{}
		p.nextToken()
	case p.cur.typ == tokenKeyword && (p.cur.literal == "TRUE" || p.cur.literal == "FALSE"):
		val := p.cur.literal == "TRUE"
		right = query.Literal{Value: columnar.NewBoolValue(val)}
		p.nextToken()
	default:
		var err error
		right, err = p.parseExpression(keywordPrecedence["BETWEEN"])
		if err != nil {
			return nil, err
		}
	}
	op := "IS"
	if not {
		op = "IS NOT"
	}
	return query.BinaryExpr{Left: left, Operator: op, Right: right}, nil
}

var tokenPrecedence = map[tokenType]int{
	tokenEqual:        30,
	tokenNotEqual:     30,
	tokenLess:         30,
	tokenLessEqual:    30,
	tokenGreater:      30,
	tokenGreaterEqual: 30,
	tokenPlus:         40,
	tokenMinus:        40,
	tokenStar:         50,
	tokenSlash:        50,
	tokenPercent:      50,
}

var keywordPrecedence = map[string]int{
	"OR":      10,
	"AND":     20,
	"BETWEEN": 25,
	"IS":      25,
}

const prefixPrecedence = 60

func isBinaryOperator(t tokenType) bool {
	_, ok := tokenPrecedence[t]
	return ok
}

func isReservedWord(word string) bool {
	upper := strings.ToUpper(word)
	_, ok := keywords[upper]
	return ok
}
