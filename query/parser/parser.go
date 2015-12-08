//line parser.y:2
package parser

import __yyfmt__ "fmt"

//line parser.y:2
import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/tylerchr/parallel-database/query"
)

//line parser.y:14
type yySymType struct {
	yys     int
	tok     int
	val     string
	ident   string
	sel     query.Query
	metrics []query.QueryMetric
	metric  query.QueryMetric
	filters []query.QueryFilter
	filter  query.QueryFilter
	numeric int
}

const SELECT = 57346
const FROM = 57347
const WHERE = 57348
const AND = 57349
const HOSTS = 57350
const GENERIC_IDENTIFIER = 57351

var yyToknames = [...]string{
	"$end",
	"error",
	"$unk",
	"SELECT",
	"FROM",
	"WHERE",
	"AND",
	"HOSTS",
	"GENERIC_IDENTIFIER",
	"','",
	"'('",
	"')'",
}
var yyStatenames = [...]string{}

const yyEofCode = 1
const yyErrCode = 2
const yyMaxDepth = 200

//line parser.y:93

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
				text = text[1 : len(text)-1]
			}
			tokens = append(tokens, token{GENERIC_IDENTIFIER, text})
		}
	}

	return tokens, err

}

//line yacctab:1
var yyExca = [...]int{
	-1, 1,
	1, -1,
	-2, 0,
}

const yyNprod = 13
const yyPrivate = 57344

var yyTokenNames []string
var yyStates []string

const yyLast = 23

var yyAct = [...]int{

	15, 21, 10, 9, 23, 16, 5, 8, 20, 18,
	17, 6, 12, 19, 3, 13, 1, 11, 7, 14,
	22, 4, 2,
}
var yyPact = [...]int{

	10, -1000, -1000, 2, -3, -1000, -9, 4, 2, -4,
	1, -1000, 0, -1000, 6, -1000, -1, -11, -1000, -4,
	-5, -1000, -1000, -1000,
}
var yyPgo = [...]int{

	0, 22, 21, 6, 19, 18, 0, 17, 16,
}
var yyR1 = [...]int{

	0, 8, 1, 2, 2, 3, 5, 5, 4, 4,
	6, 7, 7,
}
var yyR2 = [...]int{

	0, 1, 4, 1, 3, 4, 2, 0, 1, 3,
	3, 2, 0,
}
var yyChk = [...]int{

	-1000, -8, -1, 4, -2, -3, 9, -5, 10, 6,
	11, -7, 8, -3, -4, -6, 9, 9, 9, 7,
	9, 12, -6, 9,
}
var yyDef = [...]int{

	0, -2, 1, 0, 7, 3, 0, 12, 0, 0,
	0, 2, 0, 4, 6, 8, 0, 0, 11, 0,
	0, 5, 9, 10,
}
var yyTok1 = [...]int{

	1, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	11, 12, 3, 3, 10,
}
var yyTok2 = [...]int{

	2, 3, 4, 5, 6, 7, 8, 9,
}
var yyTok3 = [...]int{
	0,
}

var yyErrorMessages = [...]struct {
	state int
	token int
	msg   string
}{}

//line yaccpar:1

/*	parser for yacc output	*/

var (
	yyDebug        = 0
	yyErrorVerbose = false
)

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyParser interface {
	Parse(yyLexer) int
	Lookahead() int
}

type yyParserImpl struct {
	lookahead func() int
}

func (p *yyParserImpl) Lookahead() int {
	return p.lookahead()
}

func yyNewParser() yyParser {
	p := &yyParserImpl{
		lookahead: func() int { return -1 },
	}
	return p
}

const yyFlag = -1000

func yyTokname(c int) string {
	if c >= 1 && c-1 < len(yyToknames) {
		if yyToknames[c-1] != "" {
			return yyToknames[c-1]
		}
	}
	return __yyfmt__.Sprintf("tok-%v", c)
}

func yyStatname(s int) string {
	if s >= 0 && s < len(yyStatenames) {
		if yyStatenames[s] != "" {
			return yyStatenames[s]
		}
	}
	return __yyfmt__.Sprintf("state-%v", s)
}

func yyErrorMessage(state, lookAhead int) string {
	const TOKSTART = 4

	if !yyErrorVerbose {
		return "syntax error"
	}

	for _, e := range yyErrorMessages {
		if e.state == state && e.token == lookAhead {
			return "syntax error: " + e.msg
		}
	}

	res := "syntax error: unexpected " + yyTokname(lookAhead)

	// To match Bison, suggest at most four expected tokens.
	expected := make([]int, 0, 4)

	// Look for shiftable tokens.
	base := yyPact[state]
	for tok := TOKSTART; tok-1 < len(yyToknames); tok++ {
		if n := base + tok; n >= 0 && n < yyLast && yyChk[yyAct[n]] == tok {
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}
	}

	if yyDef[state] == -2 {
		i := 0
		for yyExca[i] != -1 || yyExca[i+1] != state {
			i += 2
		}

		// Look for tokens that we accept or reduce.
		for i += 2; yyExca[i] >= 0; i += 2 {
			tok := yyExca[i]
			if tok < TOKSTART || yyExca[i+1] == 0 {
				continue
			}
			if len(expected) == cap(expected) {
				return res
			}
			expected = append(expected, tok)
		}

		// If the default action is to accept or reduce, give up.
		if yyExca[i+1] != 0 {
			return res
		}
	}

	for i, tok := range expected {
		if i == 0 {
			res += ", expecting "
		} else {
			res += " or "
		}
		res += yyTokname(tok)
	}
	return res
}

func yylex1(lex yyLexer, lval *yySymType) (char, token int) {
	token = 0
	char = lex.Lex(lval)
	if char <= 0 {
		token = yyTok1[0]
		goto out
	}
	if char < len(yyTok1) {
		token = yyTok1[char]
		goto out
	}
	if char >= yyPrivate {
		if char < yyPrivate+len(yyTok2) {
			token = yyTok2[char-yyPrivate]
			goto out
		}
	}
	for i := 0; i < len(yyTok3); i += 2 {
		token = yyTok3[i+0]
		if token == char {
			token = yyTok3[i+1]
			goto out
		}
	}

out:
	if token == 0 {
		token = yyTok2[1] /* unknown char */
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("lex %s(%d)\n", yyTokname(token), uint(char))
	}
	return char, token
}

func yyParse(yylex yyLexer) int {
	return yyNewParser().Parse(yylex)
}

func (yyrcvr *yyParserImpl) Parse(yylex yyLexer) int {
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	var yyDollar []yySymType
	_ = yyDollar // silence set and not used
	yyS := make([]yySymType, yyMaxDepth)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yystate := 0
	yychar := -1
	yytoken := -1 // yychar translated into internal numbering
	yyrcvr.lookahead = func() int { return yychar }
	defer func() {
		// Make sure we report no lookahead when not parsing.
		yystate = -1
		yychar = -1
		yytoken = -1
	}()
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	if yyDebug >= 4 {
		__yyfmt__.Printf("char %v in %v\n", yyTokname(yytoken), yyStatname(yystate))
	}

	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	yyn = yyPact[yystate]
	if yyn <= yyFlag {
		goto yydefault /* simple state */
	}
	if yychar < 0 {
		yychar, yytoken = yylex1(yylex, &yylval)
	}
	yyn += yytoken
	if yyn < 0 || yyn >= yyLast {
		goto yydefault
	}
	yyn = yyAct[yyn]
	if yyChk[yyn] == yytoken { /* valid shift */
		yychar = -1
		yytoken = -1
		yyVAL = yylval
		yystate = yyn
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	}

yydefault:
	/* default state action */
	yyn = yyDef[yystate]
	if yyn == -2 {
		if yychar < 0 {
			yychar, yytoken = yylex1(yylex, &yylval)
		}

		/* look through exception table */
		xi := 0
		for {
			if yyExca[xi+0] == -1 && yyExca[xi+1] == yystate {
				break
			}
			xi += 2
		}
		for xi += 2; ; xi += 2 {
			yyn = yyExca[xi+0]
			if yyn < 0 || yyn == yytoken {
				break
			}
		}
		yyn = yyExca[xi+1]
		if yyn < 0 {
			goto ret0
		}
	}
	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			yylex.Error(yyErrorMessage(yystate, yytoken))
			Nerrs++
			if yyDebug >= 1 {
				__yyfmt__.Printf("%s", yyStatname(yystate))
				__yyfmt__.Printf(" saw %s\n", yyTokname(yytoken))
			}
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				yyn = yyPact[yyS[yyp].yys] + yyErrCode
				if yyn >= 0 && yyn < yyLast {
					yystate = yyAct[yyn] /* simulate a shift of "error" */
					if yyChk[yystate] == yyErrCode {
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yyTokname(yytoken))
			}
			if yytoken == yyEofCode {
				goto ret1
			}
			yychar = -1
			yytoken = -1
			goto yynewstate /* try again in the same state */
		}
	}

	/* reduction by production yyn */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce %v in:\n\t%v\n", yyn, yyStatname(yystate))
	}

	yynt := yyn
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= yyR2[yyn]
	// yyp is now the index of $0. Perform the default action. Iff the
	// reduced production is ε, $1 is possibly out of range.
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	yyn = yyR1[yyn]
	yyg := yyPgo[yyn]
	yyj := yyg + yyS[yyp].yys + 1

	if yyj >= yyLast {
		yystate = yyAct[yyg]
	} else {
		yystate = yyAct[yyj]
		if yyChk[yystate] != -yyn {
			yystate = yyAct[yyg]
		}
	}
	// dummy call; replaced with literal code
	switch yynt {

	case 1:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line parser.y:47
		{
			yylex.(*lex).query = yyDollar[1].sel
		}
	case 2:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line parser.y:52
		{
			yyVAL.sel = query.Query{Metrics: yyDollar[2].metrics, Filter: yyDollar[3].filters, Hosts: yyDollar[4].numeric}
		}
	case 3:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line parser.y:57
		{
			yyVAL.metrics = []query.QueryMetric{yyDollar[1].metric}
		}
	case 4:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line parser.y:59
		{
			yyVAL.metrics = append(yyDollar[1].metrics, yyDollar[3].metric)
		}
	case 5:
		yyDollar = yyS[yypt-4 : yypt+1]
		//line parser.y:64
		{
			yyVAL.metric.Column, yyVAL.metric.Metric = yyDollar[3].ident, yyDollar[1].ident
		}
	case 6:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line parser.y:69
		{
			yyVAL.filters = yyDollar[2].filters
		}
	case 7:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line parser.y:71
		{
			yyVAL.filters = []query.QueryFilter{}
		}
	case 8:
		yyDollar = yyS[yypt-1 : yypt+1]
		//line parser.y:76
		{
			yyVAL.filters = []query.QueryFilter{yyDollar[1].filter}
		}
	case 9:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line parser.y:78
		{
			yyVAL.filters = append(yyDollar[1].filters, yyDollar[3].filter)
		}
	case 10:
		yyDollar = yyS[yypt-3 : yypt+1]
		//line parser.y:83
		{
			yyVAL.filter.Column, yyVAL.filter.Operator, yyVAL.filter.Operand = yyDollar[1].ident, yyDollar[2].ident, yyDollar[3].ident
		}
	case 11:
		yyDollar = yyS[yypt-2 : yypt+1]
		//line parser.y:88
		{
			yyVAL.numeric, _ = strconv.Atoi(yyDollar[2].ident)
		}
	case 12:
		yyDollar = yyS[yypt-0 : yypt+1]
		//line parser.y:90
		{
			yyVAL.numeric = 0
		}
	}
	goto yystack /* stack new state and value */
}
