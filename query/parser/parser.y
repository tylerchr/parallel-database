%{
package parser

import (
    "fmt"
    "strings"
    "strconv"
    "text/scanner"

    "github.com/tylerchr/parallel-database/query"
)
%}

%union{
    tok int
    val string
    ident string
    sel query.Query
    metrics []query.QueryMetric
    metric query.QueryMetric
    filters []query.QueryFilter
    filter query.QueryFilter
    numeric int
}

%token SELECT FROM WHERE AND HOSTS
%token GENERIC_IDENTIFIER

%type <ident> GENERIC_IDENTIFIER
%type <sel> sel
%type <metrics> metrics
%type <metric> metric
%type <filters> where_terms where_clause
%type <filter> where_term
%type <numeric> hosts_clause


// average(song_hotttnesss)
// average(song_hotttnesss) : is(artist_location, "Detroit, MI")

// select average(song_hotttnesss) where artist_location == "Detroit, MI"

%%

query
    : sel
        { yylex.(*lex).query = $1 }
    ;

sel
    : SELECT metrics where_clause hosts_clause
        { $$ = query.Query{Metrics: $2, Filter: $3, Hosts: $4} }
    ;

metrics
	: metric
        { $$ = []query.QueryMetric{$1} }
    | metrics ',' metric
		{ $$ = append($1, $3) }
	;

metric
    : GENERIC_IDENTIFIER '(' GENERIC_IDENTIFIER ')'
        { $$.Column, $$.Metric = $3, $1 }
    ;

where_clause
    : WHERE where_terms
        { $$ = $2 }
    |
        { $$ = []query.QueryFilter{} }
    ;

where_terms
    : where_term
        { $$ = []query.QueryFilter{$1} }
    | where_terms AND where_term
        { $$ = append($1, $3) }
    ;

where_term
    : GENERIC_IDENTIFIER GENERIC_IDENTIFIER GENERIC_IDENTIFIER
        { $$.Column, $$.Operator, $$.Operand = $1, $2, $3 }
    ;

hosts_clause
    : HOSTS GENERIC_IDENTIFIER
        { $$, _ = strconv.Atoi($2) }
    |
        { $$ = 0 }
    ;

%%

type token struct {
    tok int
    val string
}

type lex struct {
	tokens []token
	query  query.Query
    error  error
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
    l.error = fmt.Errorf(e)
}

func ParseQuery(qs string) (query.Query, error) {

    tokens, err := TokenizeQuery(qs)
    if err != nil {
        return query.Query{}, err
    }

    l := &lex{tokens, query.Query{}, nil}
    yyParse(l)
    return l.query, l.error
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
        } else if s.TokenText() == "SELECT" {
            tokens = append(tokens, token{SELECT, ""})
        } else if s.TokenText() == "WHERE" {
            tokens = append(tokens, token{WHERE, ""})
        } else if s.TokenText() == "AND" {
            tokens = append(tokens, token{AND, ""})
        } else if s.TokenText() == "HOSTS" {
            tokens = append(tokens, token{HOSTS, ""})
        } else {
            text := s.TokenText()
            if strings.HasPrefix(text, "\"") && strings.HasSuffix(text, "\"") {
                text = text[1:len(text)-1]
            }
            tokens = append(tokens, token{GENERIC_IDENTIFIER, text})
        }
    }

    return tokens, err

}