%{
package parser

import (
    "fmt"
    "log"
    "strings"
    "text/scanner"

    "github.com/tylerchr/parallel-database/query"
)
%}

%union{
    tok int
    val string
    ident string
    metrics []query.QueryMetric
    metric query.QueryMetric
}

%token tok_IDENT

%type <ident> tok_IDENT
%type <metrics> metrics
%type <metric> metric

// average(song_hotttnesss)
// average(song_hotttnesss) : is(artist_location, "Detroit, MI")

%%

goal
	: metrics
    	{ yylex.(*lex).query = query.Query{Metrics: $1} }
    ;

metrics
	: metric
        { $$ = []query.QueryMetric{$1} }
    | metrics ',' metric
		{ $$ = append($1, $3) }
	;

metric
    : tok_IDENT '(' tok_IDENT ')'
        { $$.Column, $$.Metric = $3, $1 }
    ;

%%

type token struct {
    tok int
    val string
}

type lex struct {
	tokens []token
	query  query.Query
}

func (l *lex) Lex(lval *yySymType) int {
    if len(l.tokens) == 0 {
        return 0
    }

    v := l.tokens[0]
    l.tokens = l.tokens[1:]
    lval.ident = v.val
    return v.tok
}

func (l *lex) Error(e string) {
    log.Fatal(e)
}

func ParseQuery(qs string) (query.Query, error) {

    tokens, err := TokenizeQuery(qs)
    if err != nil {
        return query.Query{}, err
    }

    l := &lex{tokens, query.Query{}}
    yyParse(l)
    return l.query, nil
}

func TokenizeQuery(q string) ([]token, error) {

    tokens := make([]token, 0, 10)

    s := scanner.Scanner{}
    s.Init(strings.NewReader(q))

    var err error

    s.Error = func(s *scanner.Scanner, msg string) {
        err = fmt.Errorf("%s", msg)
    }

    for tok := s.Scan(); tok != scanner.EOF && err == nil; tok = s.Scan() {
        if strings.ContainsRune("():,", tok) {
            tokens = append(tokens, token{int(tok), ""})
        } else {
            tokens = append(tokens, token{tok_IDENT, s.TokenText()})
        }
    }

    return tokens, err

}