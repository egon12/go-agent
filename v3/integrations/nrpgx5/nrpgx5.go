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
	Tracer struct {
		BaseSegment newrelic.DatastoreSegment
		ParseQuery  func(segment *newrelic.DatastoreSegment, query string)
	}

	nrPgxSegmentType string
)

const (
	querySegmentKey   nrPgxSegmentType = "nrPgx5Segment"
	prepareSegmentKey nrPgxSegmentType = "prepareNrPgx5Segment"
	batchSegmentKey   nrPgxSegmentType = "batchNrPgx5Segment"
)

func NewTracer() *Tracer {
	return &Tracer{
		ParseQuery: sqlparse.ParseQuery,
	}
}

// TraceConnectStart is called at the beginning of Connect and ConnectConfig calls. The returned context is used for
// the rest of the call and will be passed to TraceConnectEnd. // implement pgx.ConnectTracer
func (t *Tracer) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	t.BaseSegment = newrelic.DatastoreSegment{
		Product:      newrelic.DatastorePostgres,
		Host:         data.ConnConfig.Host,
		PortPathOrID: strconv.FormatUint(uint64(data.ConnConfig.Port), 10),
		DatabaseName: data.ConnConfig.Database,
	}

	return ctx
}

// TraceConnectEnd method // implement pgx.ConnectTracer
func (Tracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {}

// TraceQueryStart is called at the beginning of Query, QueryRow, and Exec calls. The returned context is used for the
// rest of the call and will be passed to TraceQueryEnd. //implement pgx.QueryTracer
func (t *Tracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	segment := t.BaseSegment
	segment.StartTime = newrelic.FromContext(ctx).StartSegmentNow()
	segment.ParameterizedQuery = data.SQL
	segment.QueryParameters = t.getQueryParameters(data.Args)

	// fill Operation and Collection
	t.ParseQuery(&segment, data.SQL)

	return context.WithValue(ctx, querySegmentKey, &segment)
}

// TraceQueryEnd method implement pgx.QueryTracer. It will try to get segment from context and end it.
func (n *Tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	segment, ok := ctx.Value(querySegmentKey).(*newrelic.DatastoreSegment)
	if !ok {
		return
	}
	segment.End()
}

func (n *Tracer) getQueryParameters(args []interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for i, arg := range args {
		result["$"+strconv.Itoa(i)] = arg
	}
	return result
}

// TraceBatchStart is called at the beginning of SendBatch calls. The returned context is used for the
// rest of the call and will be passed to TraceBatchQuery and TraceBatchEnd. // implement pgx.BatchTracer
func (t *Tracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	segment := t.BaseSegment
	segment.StartTime = newrelic.FromContext(ctx).StartSegmentNow()
	segment.Operation = "batch"
	segment.Collection = ""

	return context.WithValue(ctx, batchSegmentKey, &segment)
}

func (t *Tracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	segment, ok := ctx.Value(batchSegmentKey).(*newrelic.DatastoreSegment)
	if !ok {
		return
	}

	segment.ParameterizedQuery += data.SQL + "\n"
}

func (t *Tracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	segment, ok := ctx.Value(batchSegmentKey).(*newrelic.DatastoreSegment)
	if !ok {
		return
	}
	segment.End()
}

// TracePrepareStart is called at the beginning of Prepare calls. The returned context is used for the
// rest of the call and will be passed to TracePrepareEnd.
func (t *Tracer) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	return ctx
}

func (Tracer) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
}
