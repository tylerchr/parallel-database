package query

type RangedQuery struct {
	Query      Query
	Start, End byte
}

type Query struct {
	Metrics []QueryMetric
	Filter  []QueryFilter
	Hosts   int
}

type QueryMetric struct {
	Column string
	Metric string
}

type QueryFilter struct {
	Column   string
	Operator string
	Operand  string
}
