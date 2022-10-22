package nrpgx5

import (
	"context"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/newrelic/go-agent/v3/internal"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/newrelic/go-agent/v3/newrelic/sqlparse"
)

func init() {
	internal.TrackUsage("integration", "driver", "nrpgx5")
}

type (
	NrPgx5 struct {
		BaseSegment newrelic.DatastoreSegment
		ParseQuery  func(segment *newrelic.DatastoreSegment, query string)
	}

	nrPgxSegmentType string
)

const (
	querySegmentKey   nrPgxSegmentType = "nrPgxSegment"
	prepareSegmentKey nrPgxSegmentType = "prepareNrPgxSegment"
)

func NewNrPgx5() *NrPgx5 {
	return &NrPgx5{
		ParseQuery: sqlparse.ParseQuery,
	}
}

// TraceConnectStart is called at the beginning of Connect and ConnectConfig calls. The returned context is used for
// the rest of the call and will be passed to TraceConnectEnd.
func (n *NrPgx5) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	n.BaseSegment = newrelic.DatastoreSegment{
		Product:      newrelic.DatastorePostgres,
		Host:         data.ConnConfig.Host,
		PortPathOrID: strconv.FormatUint(uint64(data.ConnConfig.Port), 10),
		DatabaseName: data.ConnConfig.Database,
	}

	return ctx
}

// TraceConnectEnd method // implement pgx.ConnectTracer
func (NrPgx5) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {}

// TraceQueryStart is called at the beginning of Query, QueryRow, and Exec calls. The returned context is used for the
// rest of the call and will be passed to TraceQueryEnd.
func (n *NrPgx5) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	segment := n.BaseSegment
	segment.StartTime = newrelic.FromContext(ctx).StartSegmentNow()
	segment.ParameterizedQuery = data.SQL
	segment.QueryParameters = n.getQueryParameters(data.Args)

	// fill Operation and Collection
	n.ParseQuery(&segment, data.SQL)

	return context.WithValue(ctx, querySegmentKey, &segment)
}

// TraceQueryEnd method implement pgx.QueryTracer. It will try to get segment from context and end it.
func (n *NrPgx5) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	segment, ok := ctx.Value(querySegmentKey).(*newrelic.DatastoreSegment)
	if !ok {
		return
	}
	segment.End()
}

func (n *NrPgx5) getQueryParameters(args []interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for i, arg := range args {
		result["$"+strconv.Itoa(i)] = arg
	}
	return result
}
