package query

type Query struct {
    Metrics []QueryMetric
    Filter []QueryFilter
}

type QueryMetric struct {
    Column string
    Metric string
}

type QueryFilter struct {
    Column string
    Operator string
    Operand string
}