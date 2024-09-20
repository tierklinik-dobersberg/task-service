package taskql

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"text/scanner"
)

type Condition struct {
	Not       bool
	FieldName string
	Value     string
}

type Token string

const (
	TokenStart     = Token("init")
	TokenField     = Token("fieldName")
	TokenSeparator = Token("sep")
	TokenValue     = Token("value")
)

func Parse(input string) ([]*Condition, Token, error) {
	result := make([]*Condition, 0, 5)

	var s = new(scanner.Scanner)

	s.Error = func(s *scanner.Scanner, msg string) {
		slog.Error("failed to parse query", "error", msg)
	}

	s.Init(strings.NewReader(input))

	s.Mode = scanner.ScanChars | scanner.ScanStrings | scanner.ScanIdents | scanner.GoWhitespace

	var (
		cur   *Condition
		state Token = TokenStart
	)

L:
	for token := s.Scan(); token != scanner.EOF; token = s.Scan() {
		text := s.TokenText()

		if cur == nil {
			state = TokenStart
			cur = &Condition{}
			result = append(result, cur)
		}

		switch token {
		case '-':
			if state == TokenValue {
				cur.Value += text
				continue L
			}

			if state != TokenStart {
				return nil, state, fmt.Errorf("unexpected token %q", token)
			}
			cur.Not = true
			state = TokenField

		case scanner.Ident:
			if state == TokenStart || state == TokenField {
				cur.FieldName = text
				state = TokenSeparator
			} else if state == TokenValue {
				cur.Value = text
				cur = nil
			} else {
				return nil, state, fmt.Errorf("unexpected ident token %q", text)
			}

		case ':':
			if state == TokenValue {
				cur.Value += text
				continue L
			}

			if state != TokenSeparator {
				return nil, state, fmt.Errorf("unexpected seperator ':'")
			}
			state = TokenValue

		case scanner.String:
			if state != TokenValue {
				return nil, state, fmt.Errorf("unexpected string literal %q", text)
			}
			var err error
			cur.Value, err = strconv.Unquote(text)
			if err != nil {
				return nil, state, err
			}

			cur = nil

		default:
			if state == TokenValue {
				cur.Value += text
				continue L
			}

			return nil, state, fmt.Errorf("unexpected token: %q", text)
		}
	}

	if cur == nil {
		state = TokenStart
		result = append(result, &Condition{})
	}

	return result, state, nil
}
