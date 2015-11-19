package parser

import (
	"reflect"
	"testing"

    "github.com/tylerchr/parallel-database/query"
)

func TestTokenizeQuery(t *testing.T) {
	qs := `average(song_hotttnesss) : contains(title, "One"), is(artist_location, "Detroit, MI")`
	tokens, _ := TokenizeQuery(qs)
	t.Logf("Got %d tokens: %v\n", len(tokens), tokens)

	expected := []token{
		token{tok_IDENT, "average"},
		token{'(', ""},
		token{tok_IDENT, "song_hotttnesss"},
		token{')', ""},
		token{':', ""},
		token{tok_IDENT, "contains"},
		token{'(', ""},
		token{tok_IDENT, "title"},
		token{',', ""},
		token{tok_IDENT, "\"One\""},
		token{')', ""},
		token{',', ""},
		token{tok_IDENT, "is"},
		token{'(', ""},
		token{tok_IDENT, "artist_location"},
		token{',', ""},
		token{tok_IDENT, "\"Detroit, MI\""},
		token{')', ""},
	}

	if !reflect.DeepEqual(tokens, expected) {
		t.Errorf("Incorrect tokenization: expected %d, got %d", len(expected), len(tokens))
	}
}

func TestParseQuery(t *testing.T) {

	qs := `average(song_hotttnesss), max(tempo)`
	queryObj, _ := ParseQuery(qs)

	expected := query.Query{
		Metrics: []query.QueryMetric{
			query.QueryMetric{Column: "song_hotttnesss", Metric: "average"},
			query.QueryMetric{Column: "tempo", Metric: "max"},
		},
	}

	if !reflect.DeepEqual(queryObj, expected) {
		t.Errorf("Incorrect query parse: expected %v but got %v", expected, queryObj)
	}
}